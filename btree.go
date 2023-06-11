package btree

import (
	"sort"
	"sync"
)

type (
	Item interface {
		// Lessは、現在のアイテムが与えられた引数より小さいかどうかをテストします。
		// a.Less(b) && !b.Less(a) の場合、a == b を意味するものとして扱います（つまり、ツリーの中でaまたはbのどちらか一方しか保持できない）。
		Less(than Item) bool
	}

	items []Item

	children []*node

	copyOnWriteContext struct {
		freelist *FreeList
	}

	FreeList struct {
		mu       sync.Mutex
		freelist []*node
	}

	node struct {
		items    items
		children children
		cow      *copyOnWriteContext
	}

	BTree struct {
		degree int
		length int
		root   *node
		cow    *copyOnWriteContext
	}
	// ItemIteratorは、Ascend*の呼び出し元がツリーの一部を順番に反復処理することを可能にします。
	//この関数が false を返すと、反復処理は停止し、関連する Ascend* 関数が直ちに返されます。
	ItemIterator func(i Item) bool

	toRemove  int
	freeType  int
	direction int
)

const (
	DefaultFreeListSize = 32

	removeItem toRemove = iota // 与えられた項目を削除します。
	removeMin                  // サブツリー内の最小の項目を削除します。
	removeMax                  // サブツリーの最大の項目を削除します。

	ftFreelistFull freeType = iota // ノードが解放された（GCで利用可能、フリーリストに保存されない）。
	ftStored                       // ノードがフリーリストに保存され、後で使用されるようになった
	ftNotOwned                     // ノードは、別のノードに所有されているため、COWによって無視されました。

	descend = direction(-1)
	ascend  = direction(+1)
)

var (
	nilItems    = make(items, 16)
	nilChildren = make(children, 16)
)

func NewFreeList(size int) *FreeList {
	return &FreeList{freelist: make([]*node, 0, size)}
}

// FreeList

// 一番右端のノードを取得して返す、端のノードを取り除いたfreelist設定しなおす。
func (f *FreeList) newNode() (n *node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := len(f.freelist) - 1
	if index < 0 {
		return new(node)
	}
	n = f.freelist[index]
	f.freelist[index] = nil
	f.freelist = f.freelist[:index]
	return
}

// 与えられたノードをリストに追加し、追加された場合はtrueを、破棄された場合はfalseを返す。
func (f *FreeList) freeNode(n *node) (out bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
		out = true
	}
	return
}

func New(degree int) *BTree {
	return NewWithFreeList(degree, NewFreeList(DefaultFreeListSize))
}

// 与えられたノードフリーリストを使用する新しい B-Tree を作成します。
func NewWithFreeList(degree int, f *FreeList) *BTree {
	if degree <= 1 {
		panic("bad degree")
	}
	return &BTree{
		degree: degree,
		cow:    &copyOnWriteContext{freelist: f},
	}
}

// items

// insertAtは、与えられたインデックスに値を挿入し、それ以降の値をすべて後ろに移す。
func (s *items) insertAt(index int, item Item) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:]) // 前に後ろをコピーする
	}
	(*s)[index] = item
}

// removeAtは、指定されたインデックスの値を削除し、それ以降の値をすべて引き戻します。
func (s *items) removeAt(index int) Item {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return item
}

// pop は、リストの最後の要素を削除して返します。
func (s *items) pop() (out Item) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// truncateは、このインスタンスをindexで切り捨て、最初のindex項目のみを含むようにする。indexはlength以下でなければならない。
func (s *items) truncate(index int) {
	var toClear items
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilItems):]
	}
}

// find は、与えられた項目をこのリストに挿入するためのインデックスを返す。 'found' は、その項目が既にリストの中の与えられたインデックスに存在する場合に真となる。
// itemより大きいs[i]を探す、なのでs[i-1]はitemより小さいか同じ、!s[i-1].Less(item)はs[i-1]よりitemが大きくないので同じになる
func (s items) find(item Item) (index int, found bool) {
	i := sort.Search(len(s), func(i int) bool {
		return item.Less(s[i])
	})
	if i > 0 && !s[i-1].Less(item) {
		return i - 1, true
	}
	return i, false
}

// children

// insertAtは、与えられたインデックスに値を挿入し、それ以降の値をすべて前方に押し出します。
func (s *children) insertAt(index int, n *node) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = n
}

func (s *children) removeAt(index int) *node {
	n := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return n
}

func (s *children) pop() (out *node) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

func (s *children) truncate(index int) {
	var toClear children
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilChildren):]
	}
}

// node
//nodeは、木の内部ノードである。
// このノードは、常に、 * len(children) == 0, len(items) unconstrained * len(children) == len(items) + 1 という不変性を保持していなければならない。

// cow の newnode(freelistの端のnode res)を、n のnodenのitems,childrenをコピーして返す。
func (n *node) mutableFor(cow *copyOnWriteContext) *node {
	if n.cow == cow {
		return n
	}
	out := cow.newNode()
	if cap(out.items) >= len(n.items) {
		out.items = out.items[:len(n.items)]
	} else {
		out.items = make(items, len(n.items), cap(n.items))
	}
	copy(out.items, n.items)
	// Copy children
	if cap(out.children) >= len(n.children) {
		out.children = out.children[:len(n.children)]
	} else {
		out.children = make(children, len(n.children), cap(n.children))
	}
	copy(out.children, n.children)
	return out
}

// mutableChild は、与えられたインデックスの子ノードを返す。このノードは、このノードのコピーでなければならない。
func (n *node) mutableChild(i int) *node {
	c := n.children[i].mutableFor(n.cow)
	n.children[i] = c
	return c
}

// split は、与えられたノードを与えられたインデックスで分割する。
// 現在のノードは縮小し、この関数はそのインデックスに存在していたアイテムと、それ以降のすべてのアイテム/子ノードを含む新しいノードを返す。
func (n *node) split(i int) (Item, *node) {
	item := n.items[i]
	next := n.cow.newNode()
	next.items = append(next.items, n.items[i+1:]...)
	n.items.truncate(i)
	if len(n.children) > 0 {
		next.children = append(next.children, n.children[i+1:]...)
		n.children.truncate(i + 1)
	}
	return item, next
}

// maybeSplitChildは、子機が分割されるべきかどうかをチェックし、分割される場合は分割する。分割が行われたかどうかを返します。
func (n *node) maybeSplitChild(i, maxItems int) bool {
	if len(n.children[i].items) < maxItems {
		return false
	}
	// i個目の子ノードをコピーしたnodeを返す。
	first := n.mutableChild(i)
	// 分割
	item, second := first.split(maxItems / 2)
	// itemsにi個目にitemをinsert
	n.items.insertAt(i, item)
	n.children.insertAt(i+1, second)
	return true
}

// insert は、このノードをルートとするサブツリーにアイテムを挿入し、
// サブツリー内のノードが maxItems アイテムを超えていないことを確認する。 insertによって同等のアイテムが見つかったり置き換えられたりした場合は、それが返されます。
// item より大きいアイテムが見つかった場合、そのサブツリーの前に挿入されます。ない場合はさらにその先一番最後に挿入されます。
func (n *node) insert(item Item, maxItems int) Item {
	i, found := n.items.find(item)
	if found {
		out := n.items[i]
		n.items[i] = item
		return out
	}
	if len(n.children) == 0 {
		n.items.insertAt(i, item)
		return nil
	}
	if n.maybeSplitChild(i, maxItems) {
		inTree := n.items[i]
		switch {
		case item.Less(inTree):
			// no change, we want first split node
		case inTree.Less(item):
			i++ // we want second split node
		default:
			out := n.items[i]
			n.items[i] = item
			return out
		}
	}
	return n.mutableChild(i).insert(item, maxItems)
}

// getは、サブツリーから与えられたキーを見つけ、それを返す。
func (n *node) get(key Item) Item {
	i, found := n.items.find(key)
	if found {
		return n.items[i]
	} else if len(n.children) > 0 {
		return n.children[i].get(key)
	}
	return nil
}

// minは、サブツリーの最初の項目を返す。
func min(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[0]
	}
	if len(n.items) == 0 {
		return nil
	}
	return n.items[0]
}

// max は、サブツリーの最後の項目を返す。
func max(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[len(n.children)-1]
	}
	if len(n.items) == 0 {
		return nil
	}
	return n.items[len(n.items)-1]
}

// remove は、このノードをルートとするサブツリーから項目を削除する。
func (n *node) remove(item Item, minItems int, typ toRemove) Item {
	var i int
	var found bool
	switch typ {
	case removeMax:
		if len(n.children) == 0 {
			return n.items.pop()
		}
		i = len(n.items)
	case removeMin:
		if len(n.children) == 0 {
			return n.items.removeAt(0)
		}
		i = 0
	case removeItem:
		i, found = n.items.find(item)
		if len(n.children) == 0 {
			if found {
				return n.items.removeAt(i)
			}
			return nil
		}
	default:
		panic("invalid type")
	}
	// ここまでくれば、子ノードもいる。
	if len(n.children[i].items) <= minItems {
		return n.growChildAndRemove(i, item, minItems, typ)
	}
	child := n.mutableChild(i)
	//もともと十分なアイテムがあったのか、それともマージやスティールをしたのか、今は十分なアイテムがあるので、物を返す準備はできています。
	if found {
		// アイテムはインデックス 'i' に存在し、選択した子は前任者を与えることができる。なぜなら、ここまで来れば、 > minItems アイテムを持っているからである。
		out := n.items[i]
		// 特別なケースである'remove'呼び出し（typ=maxItem）を使って、アイテムiの前任者（すぐ左の子の右端の葉）を引き出し、アイテムを引き出した場所にセットするのです。
		n.items[i] = child.remove(nil, minItems, removeMax)
		return out
	}
	// 最後の再帰的呼び出し。 ここまでくれば、アイテムがこのノードにないこと、子ノードが十分な大きさでそこから削除できることがわかります。
	return child.remove(item, minItems, typ)
}

// growChildAndRemove は、子 'i' を成長させ、minItems を維持しながらそこからアイテムを取り除くことが可能であることを確認し、それから実際に取り除くために remove を呼び出します。
// 多くのドキュメントによると、2つの特別なケーシングを行う必要があるようです：
// 1) アイテムがこのノードの中にある
// 2) 項目が子ノードにある
// どちらの場合も、2つのサブケースを処理する必要があります：
// A) ノードが十分な値を持っていて、1つの値を確保できる。
// B) ノードが十分な値を持っていない
// 後者の場合、以下のことを確認する必要があります：
// a)左の兄弟にノードの予備がある
// b) 右の兄弟に余裕のあるノードがある。
// c) マージする必要がある
// ノードに十分なアイテムがない場合は、（a,b,cを使用して）アイテムがあることを確認します。そして、removeコールをやり直すだけで、2回目には（ケース1でも2でも）十分なアイテムがあり、ケースAに当たることが保証されます。
// 左から取る場合、i,i-1をコピーして,右側の子の最大を取り、iの子のコピーには一番最小にnodeのitems[i-1]を入れる,それをnodeのitems[i-1]に入れる,
func (n *node) growChildAndRemove(i int, item Item, minItems int, typ toRemove) Item {
	if i > 0 && len(n.children[i-1].items) > minItems {
		// 左子から盗む
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i - 1)
		stolenItem := stealFrom.items.pop()
		child.items.insertAt(0, n.items[i-1])
		n.items[i-1] = stolenItem
		if len(stealFrom.children) > 0 {
			child.children.insertAt(0, stealFrom.children.pop())
		}
	} else if i < len(n.items) && len(n.children[i+1].items) > minItems {
		// steal from right child
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i + 1)
		stolenItem := stealFrom.items.removeAt(0)
		child.items = append(child.items, n.items[i])
		n.items[i] = stolenItem
		if len(stealFrom.children) > 0 {
			child.children = append(child.children, stealFrom.children.removeAt(0))
		}
	} else {
		if i >= len(n.items) {
			i--
		}
		child := n.mutableChild(i)
		// merge with right child
		mergeItem := n.items.removeAt(i)
		mergeChild := n.children.removeAt(i + 1)
		child.items = append(child.items, mergeItem)
		child.items = append(child.items, mergeChild.items...)
		child.children = append(child.children, mergeChild.children...)
		n.cow.freeNode(mergeChild)
	}
	return n.remove(item, minItems, typ)
}

//	iterate は、ツリー内の要素を反復処理するための簡単なメソッドを提供する。
//
// 昇順の場合は 'start' が 'stop' よりも小さく、降順の場合は 'start' が 'stop' よりも大きくなければなりません。
// includeStart' を true に設定すると、イテレータが 'start' と等しい場合に最初の項目を含めるようになり、単なる "greaterThan" や "lessThan" ではなく "greaterOrEqual" や "lessThanEqual" というクエリが作成されます。
func (n *node) iterate(dir direction, start, stop Item, includeStart bool, hit bool, iter ItemIterator) (bool, bool) {
	var ok, found bool
	var index int
	switch dir {
	case ascend:
		if start != nil {
			index, _ = n.items.find(start)
		}
		for i := index; i < len(n.items); i++ {
			if len(n.children) > 0 {
				if hit, ok = n.children[i].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if !includeStart && !hit && start != nil && !start.Less(n.items[i]) {
				hit = true
				continue
			}
			hit = true
			if stop != nil && !n.items[i].Less(stop) {
				return hit, false
			}
			if !iter(n.items[i]) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.children[len(n.children)-1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	case descend:
		if start != nil {
			index, found = n.items.find(start)
			if !found {
				index = index - 1
			}
		} else {
			index = len(n.items) - 1
		}
		for i := index; i >= 0; i-- {
			if start != nil && !n.items[i].Less(start) {
				if !includeStart || hit || start.Less(n.items[i]) {
					continue
				}
			}
			if len(n.children) > 0 {
				if hit, ok = n.children[i+1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if stop != nil && !stop.Less(n.items[i]) {
				return hit, false //	continue
			}
			hit = true
			if !iter(n.items[i]) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.children[0].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	}
	return hit, true
}

func (c *copyOnWriteContext) newNode() (n *node) {
	n = c.freelist.newNode()
	n.cow = c
	return
}

func (c *copyOnWriteContext) freeNode(n *node) freeType {
	if n.cow == c {
		// clear to allow GC
		n.items.truncate(0)
		n.children.truncate(0)
		n.cow = nil
		if c.freelist.freeNode(n) {
			return ftStored
		} else {
			return ftFreelistFull
		}
	} else {
		return ftNotOwned
	}
}
