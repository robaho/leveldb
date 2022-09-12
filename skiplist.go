package leveldb

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const kMaxHeight = 12

type SkipList[K any] struct {
	cmp_       func(K, K) int
	head_      *node[K]
	maxHeight_ int32
	random_    rand.Rand
}

func (s *SkipList[K]) Put(key K) K {
	// TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
	// here since Put() is externally synchronized.
	var prev [kMaxHeight]*node[K]

	x := s.findGreaterOrEqual(key, prev[:])

	// we don't support sequence numbers yet, so if key matches update
	if x != nil && s.equal(x.key, key) {
		old := x.key
		x.key = key
		return old
	}

	height := s.randomHeight()
	if height > s.getMaxHeight() {
		for i := s.getMaxHeight(); i < height; i++ {
			prev[i] = s.head_
		}
		// It is ok to mutate max_height_ without any synchronization
		// with concurrent readers.  A concurrent reader that observes
		// the new value of max_height_ will see either the old value of
		// new level pointers from head_ (nullptr), or a new value set in
		// the loop below.  In the former case the reader will
		// immediately drop to the next level since nullptr sorts after all
		// keys.  In the latter case the reader will use the new node.
		atomic.StoreInt32(&s.maxHeight_, int32(height))
	}

	x = newNode(key, height)
	for i := 0; i < height; i++ {
		// NoBarrier_SetNext() suffices since we will add a barrier when
		// we publish a pointer to "x" in prev[i].
		x.setNext(i, prev[i].next(i))
		prev[i].setNext(i, x)
	}
	var noop K
	return noop
}

func newNode[K any](key K, height int) *node[K] {
	return &node[K]{key: key, next_: make([]*node[K], height)}
}

const kBranching = 4

func (s *SkipList[K]) randomHeight() int {
	// Increase height with probability 1 in kBranching
	height := 1
	for height < kMaxHeight && rand.Intn(kBranching) == 0 {
		height++
	}
	return height
}

func (s *SkipList[K]) contains(key K) bool {
	x := s.findGreaterOrEqual(key, nil)
	if x != nil && s.equal(key, x.key) {
		return true
	} else {
		return false
	}
}

func (s *SkipList[K]) get(key K) (K, bool) {
	x := s.findGreaterOrEqual(key, nil)
	if x != nil && s.equal(key, x.key) {
		return x.key, true
	} else {
		var noop K
		return noop, false
	}
}
func (s *SkipList[K]) remove(key K) (K, bool) {
	x := s.findGreaterOrEqual(key, nil)
	if x != nil && s.equal(key, x.key) {
		prev := x.key
		x.key = key
		return prev, true
	} else {
		var noop K
		return noop, false
	}
}

type node[K any] struct {
	key   K
	next_ []*node[K]
}

func (node_ *node[K]) next(n int) *node[K] {
	p := (*unsafe.Pointer)(unsafe.Pointer(&node_.next_[n]))
	return (*node[K])(atomic.LoadPointer(p))
}

func (node_ *node[K]) setNext(n int, x *node[K]) {
	p := (*unsafe.Pointer)(unsafe.Pointer(&node_.next_[n]))
	atomic.StorePointer(p, unsafe.Pointer(x))
}

type iterator[K any] struct {
	list_ *SkipList[K]
	node_ *node[K]
}

func (i *iterator[K]) valid() bool {
	return i.node_ != nil
}

func (i *iterator[K]) next() {
	i.node_ = i.node_.next(0)
}

func (i *iterator[K]) seekToFirst() {
	i.node_ = i.list_.head_.next(0)
}

func (i *iterator[K]) seek(target K) {
	i.node_ = i.list_.findGreaterOrEqual(target, nil)
}

func (i *iterator[K]) key() K {
	return i.node_.key
}

func (s *SkipList[K]) Iterator() iterator[K] {
	return iterator[K]{list_: s}
}

func (s *SkipList[K]) Contains(key K) bool {
	x := s.findGreaterOrEqual(key, nil)
	if x != nil && s.equal(key, x.key) {
		return true
	} else {
		return false
	}
}

func (s *SkipList[K]) findGreaterOrEqual(key K, prev []*node[K]) *node[K] {
	x := s.head_
	level := int(s.getMaxHeight() - 1)
	for {
		next := x.next(level)
		if s.keyIsAfterNode(key, next) {
			// Keep searching in this list
			x = next
		} else {
			if prev != nil {
				prev[level] = x
			}
			if level == 0 {
				return next
			} else {
				// Switch to next list
				level--
			}
		}
	}
}

func (s *SkipList[K]) keyIsAfterNode(key K, n *node[K]) bool {
	// null n is considered infinite
	return (n != nil) && (s.cmp_(n.key, key) < 0)
}

func (s *SkipList[K]) getMaxHeight() int {
	return int(atomic.LoadInt32(&s.maxHeight_))
}

func (s *SkipList[K]) equal(a K, b K) bool {
	return s.cmp_(a, b) == 0
}

func NewSkipList[K any](cmp func(K, K) int) SkipList[K] {
	var noop K

	s := SkipList[K]{cmp_: cmp, head_: newNode[K](noop, kMaxHeight), maxHeight_: 1}

	for i := 0; i < kMaxHeight; i++ {
		s.head_.setNext(i, nil)
	}
	return s
}
