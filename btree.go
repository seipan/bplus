package btree

import "sync"

type (
	Item interface {
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
