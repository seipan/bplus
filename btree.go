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
)

const (
	DefaultFreeListSize = 32
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
// itemより小さいs[i]を探す、なのでs[i-1]はitemより大きいか同じ、!s[i-1].Less(item)はs[i-1]よりitemが小さくないので同じになる
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
