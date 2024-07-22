package utils

import (
	"log"
	"math"
	"sync/atomic"
	_ "unsafe"

	"github.com/pkg/errors"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

// node 跳表的节点
type node struct {
	// 将 value 的多个部分编码为单个 uint64，以便可以原子加载和存储
	// value offset: uint32 (bits 0-31)
	// value size  : uint16 (bits 32-63)
	value uint64

	keyOffset uint32 // 不可变，不需要加锁访问
	keySize   uint16 // 不可变，不需要加锁访问

	// height 节点所处层级
	height uint16

	// tower 所有对元素的访问应该使用 CAS 操作，而无需锁定
	tower [maxHeight]uint32
}

type SkipList struct {
	height     int32  // 当前最大高度
	headOffset uint32 // 头结点在 arena 中的偏移量
	ref        int32  // 引用计数
	arena      *Arena // 所属 arena
	OnClose    func()
}

func (s *SkipList) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *SkipList) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}

	s.arena = nil
}

// newNode 创建一个 node 节点
func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	node := arena.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val
	return node
}

// encodeValue 将 value 的 offset 和 size 编码成 uint64
func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

// decodeValue 从 uint64 解码出 value 在 arena 的 offset 和 size
func decodeValue(value uint64) (uint32, uint32) {
	valOffset := uint32(value)
	valSize := uint32(value >> 32)
	return valOffset, valSize
}

// NewSkipList 用给定的 arena 大小创建一个跳表
func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, ValueStruct{}, maxHeight)
	ho := arena.getNodeOffset(head)
	return &SkipList{
		height:     1,
		headOffset: ho,
		ref:        1,
		arena:      arena,
	}
}

func (n *node) getValueOffset() (valOffset uint32, valSize uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

// key 从 arena 提取 node 的 key
func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

// setValue 原子地更新 node 的 value
func (n *node) setValue(arena *Arena, vo uint64) {
	atomic.StoreUint64(&n.value, vo)
}

// getNextOffset 获取 h 层下一节点的偏移
func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

// casNextOffset 使用 CAS 更新 n.tower[h]
func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

// randomHeight 随机获取一个高度
func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *SkipList) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

// getHead 获取头节点 node
func (s *SkipList) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

// findNear 找到最接近 key 的节点
// 若 less==True，会找最右边的节点，且满足 node.key < key
// 若 less==False，会找最左边的节点，且满足 node.key > key
func (sl *SkipList) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := sl.getHead()
	level := int(sl.getHeight() - 1)
	for {
		// 确保 x.key < key
		next := sl.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// 去下一层找更接近的
				level--
				continue
			}
			if !less {
				return nil, false
			}
			if x == sl.getHead() {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(sl.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp > 0 {
			// x.key < next.key < key
			// 在当前层继续寻找
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key
			if allowEqual {
				// 继续向下找当前 key，应对当前 key 正被修改的情况
				level--
				return next, true
			}
			if !less {
				// 在最底层找一个更大的
				return sl.getNext(next, 0), false
			}
			// 想要小的，在最底层找最接近的
			if level > 0 {
				level--
				continue
			}
			if x == sl.getHead() {
				return nil, false
			}
			return x, false
		}
		// x.key < key < next
		if level > 0 {
			level--
			continue
		}
		if !less {
			return next, false
		}
		if x == sl.getHead() {
			return nil, false
		}
		return x, false
	} // for {
}

// findSpliceForLevel 给定一个 key 值，找到应该被插入的位置
// 满足 before.key < key < next.key
// 返回的是 before.offset 和 next.offset
func (sl *SkipList) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		// 假设 before.key < key
		beforeNode := sl.arena.getNode(before)
		nextOffset := beforeNode.getNextOffset(level)
		nextNode := sl.arena.getNode(nextOffset)
		// 如果 before.next 是空，说明该层级的单链表到头了
		// 该节点就应该插在这层链表的末尾
		if nextNode == nil {
			return before, nextOffset
		}
		nextKey := nextNode.key(sl.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			// 已经有相同的 key 提前插入了，此时原地更新
			return nextOffset, nextOffset
		}
		if cmp < 0 {
			// before.key < key < next.key
			return before, nextOffset
		}
		// 继续在当前层寻找
		before = nextOffset
	}
}

// getHeight 获取跳表最大高度
func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// Add 向跳表中插入 Entry
func (sl *SkipList) Add(e *Entry) {
	// 由于允许在跳表中覆盖，可能不需要创建新节点
	// 甚至不需要增加高度，因此推迟这些操作
	key, vs := e.Key, ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}

	slHeight := sl.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	prev[slHeight] = sl.headOffset

	for i := int(slHeight) - 1; i >= 0; i-- {
		// 更新要插入 Entry 在各层级的前后 node
		prev[i], next[i] = sl.findSpliceForLevel(key, prev[i+1], i)
		// 跳表中已有该 key，直接更新
		if prev[i] == next[i] {
			// 先把 value 放入 arena，再原子地更新 node 中的 value
			vo := sl.arena.putVal(vs)
			encValue := encodeValue(vo, vs.EncodedSize())
			updateEqualNd := sl.arena.getNode(prev[i])
			updateEqualNd.setValue(sl.arena, encValue)
			return
		}
	}

	// 创建新节点
	height := sl.randomHeight()
	x := newNode(sl.arena, key, vs, height)

	// 使用  CAS 增加 s.height
	slHeight = sl.getHeight()
	for height > int(slHeight) {
		if atomic.CompareAndSwapInt32(&sl.height, slHeight, int32(height)) {
			// 成功交换
			break
		}
		slHeight = sl.getHeight()
	}

	for i := 0; i < height; i++ {
		for {
			// 只有 height 是新的最大高度时才会发生
			if sl.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 1)
				// 还没计算这个级别的 prev 和 next，因为高度超过了旧的 slHeight
				prev[i], next[i] = sl.findSpliceForLevel(key, sl.headOffset, i)
				// 在添加之前，已经插入了相同的键
				// 这只能发生在最底层，但此时并不在最底层
				AssertTrue(prev[i] != next[i])
			}
			// cur.next = prev.next
			x.tower[i] = next[i]
			pnode := sl.arena.getNode(prev[i])
			// prev.next = cur
			if pnode.casNextOffset(i, next[i], sl.arena.getNodeOffset(x)) {
				// 当前层插入成功，去下一层插入
				break
			}
			// CAS 失败，重新计算 prev[i] 和 next[i]，然后再次尝试
			prev[i], next[i] = sl.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				vo := sl.arena.putVal(vs)
				encValue := encodeValue(vo, vs.EncodedSize())
				updateEqualNd := sl.arena.getNode(prev[i])
				updateEqualNd.setValue(sl.arena, encValue)
				return
			}
		} // for {
	} // for i := 0; i < height; i++ {
}

func (s *SkipList) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *SkipList) findLast() *node {
	n := s.getHead()
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.getHead() {
				return nil
			}
			return n
		}
		level--
	}
}

// Search 获取 key 对应的 value 值
func (s *SkipList) Search(key []byte) ValueStruct {
	// findGreaterOrEqual
	n, _ := s.findNear(key, false, true)
	if n == nil {
		return ValueStruct{}
	}

	findKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, findKey) {
		return ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	vs.ExpiresAt = ParseTs(findKey)
	return vs
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *SkipList) NewIterator() Iterator {
	s.IncrRef()
	return &SkipListIterator{list: s}
}

// MemSize returns the size of the SkipList in terms of how much memory is used within its internal
// arena.
func (s *SkipList) MemSize() int64 {
	return s.arena.size()
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type SkipListIterator struct {
	list *SkipList
	n    *node
}

func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}

func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

// Close frees the resources held by the iterator
func (s *SkipListIterator) Close() error {
	s.list.DecrRef()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *SkipListIterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *SkipListIterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *SkipListIterator) Value() ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getVal(valOffset, valSize)
}

// ValueUint64 returns the uint64 value of the current node.
func (s *SkipListIterator) ValueUint64() uint64 {
	return s.n.value
}

// Next advances to the next position.
func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev advances to the previous position.
func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first entry with a key >= target.
func (s *SkipListIterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
}

// SeekForPrev finds an entry with key <= target.
func (s *SkipListIterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true) // find <=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

// FastRand is a fast thread local random function.
//
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
