package utils

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	DefaultMaxLevel = 20
)

// SkipList 支持并发读写的跳表
type SkipList struct {
	head     *Element // head 跳表的头节点
	rand     *rand.Rand
	maxLevel int
	length   int64 // length 跳表中节点数量
}

func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())
	return &SkipList{
		head: &Element{
			Key:    nil,
			V:      ValueStruct{},
			levels: make([]*Element, DefaultMaxLevel),
		},
		rand:     rand.New(source),
		maxLevel: DefaultMaxLevel,
		length:   0,
	}
}

// Element 跳表节点
type Element struct {
	Key    []byte
	V      ValueStruct
	levels []*Element // 记录各层的下一节点
}

func newElement(key []byte, v ValueStruct, level int) *Element {
	return &Element{
		Key:    key,
		V:      v,
		levels: make([]*Element, level),
	}
}

// casNextPointer 原子性地将 e.levels[h] 的指向从 old 修改为 val
func (e *Element) casNextPointer(h int, old, val *Element) bool {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&e.levels[h]))
	oldPtr := unsafe.Pointer(old)
	newPtr := unsafe.Pointer(val)
	return atomic.CompareAndSwapPointer(ptr, oldPtr, newPtr)
}

func (sl *SkipList) getHead() *Element {
	return sl.head
}

func (sl *SkipList) getNext(e *Element, height int) *Element {
	return e.levels[height]
}

// findNear 找最接近 key 的节点
// 若 less==True，找满足 node.Key < key 的最大的节点
// 若 less==False，找满足 node.Key > key 的最小的节点
func (sl *SkipList) findNear(key []byte, less, allowEqual bool) (*Element, bool) {
	x := sl.getHead()
	level := int(sl.getHeight() - 1)
	for {
		next := sl.getNext(x, level)
		if next == nil {
			// x.Key < key < end of list
			if level > 0 {
				// 去下一层找
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

		nextKey := next.Key
		comp := CompareKeys(key, nextKey)
		if comp > 0 {
			// x.Key < next.Key < key
			// 在当前层继续找
			x = next
			continue
		}
		if comp == 0 {
			if allowEqual {
				if level > 0 {
					// 继续向下找，以应对当前 key 正被修改的情况
					level--
					continue
				}
				return next, true
			}
			if !less {
				return sl.getNext(next, 0), false
			}
			if level > 0 {
				level--
				continue
			}
			if x == sl.getHead() {
				return nil, false
			}
			return x, false
		}
		// x.Key < key < next.Key
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

// findSpliceForLevel 给定一个 key 和它前面的节点，找到在指定层应该插入的位置
// 满足 beforeElem.Key < key < nextElem.Key
func (sl *SkipList) findSpliceForLevel(key []byte, before *Element, level int) (*Element, *Element) {
	for {
		nextNode := before.levels[level]
		// 该节点应该插入到这层链表的末尾
		if nextNode == nil {
			return before, nil
		}
		comp := bytes.Compare(key, nextNode.Key)
		// 已经有相同的 key 了，此时原地更新
		if comp == 0 {
			return nextNode, nextNode
		}
		if comp < 0 {
			return before, nextNode
		}
		// 继续在当前层找
		before = nextNode
	}
}

// getHeight 获取跳表最大高度
func (sl *SkipList) getHeight() int32 {
	return int32(len(sl.getHead().levels))
}

func (sl *SkipList) Add(e *Entry) {
	key, vs := e.Key, ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}
	var prev [DefaultMaxLevel + 1]*Element
	var next [DefaultMaxLevel + 1]*Element
	prev[DefaultMaxLevel] = sl.getHead()

	for i := DefaultMaxLevel - 1; i >= 0; i-- {
		prev[i], next[i] = sl.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			prev[i].V = vs
			return
		}
	}

	level := sl.randLevel()
	data := newElement(key, vs, level)

	for i := 0; i < level; i++ {
		for {
			data.levels[i] = next[i]
			if prev[i].casNextPointer(i, next[i], data) {
				break
			}
			prev[i], next[i] = sl.findSpliceForLevel(key, prev[i+1], i)
			if prev[i] == next[i] {
				prev[i].V = vs
				return
			}
		}
	}

	atomic.AddInt64(&sl.length, 1)
}

func (sl *SkipList) Search(key []byte) ValueStruct {
	vs := ValueStruct{}
	if sl.length == 0 {
		return vs
	}

	prevElem := sl.getHead()
	i := sl.getHeight() - 1

	for i >= 0 {
		for nextElem := prevElem.levels[i]; nextElem != nil; nextElem = prevElem.levels[i] {
			comp := bytes.Compare(key, nextElem.Key)
			if comp <= 0 {
				if comp == 0 {
					return nextElem.V
				}
				// 当前层节点已经更大了，去下一层找
				break
			}

			// 继续在当前层找
			prevElem = nextElem
		}

		topLevelVal := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevelVal; i-- {
			// 下一层值与当前层值相同，再到下一层
		}
	}
	return vs
}

// randLevel 生成新节点的高度
func (sl *SkipList) randLevel() int {
	for i := 1; i < sl.maxLevel; i++ {
		if Intn(100)%2 == 0 {
			return i
		}
	}
	return sl.maxLevel
}

func (sl *SkipList) MemSize() int64 {
	return sl.length
}

func (sl *SkipList) PrintSkipList() {
	for si := sl.NewIterator(); si.Valid(); si.Next() {
		fmt.Print(string(si.Item().Entry().Key))
		fmt.Print(" ")
		fmt.Println(string(si.Item().Entry().Value))
	}
}

type SkipListIterator struct {
	sl *SkipList
	e  *Element
}

func (sl *SkipList) NewIterator() Iterator {
	return &SkipListIterator{sl: sl, e: sl.getHead()}
}

func (si *SkipListIterator) Next() {
	si.e = si.e.levels[0]
}

func (si *SkipListIterator) Valid() bool {
	return si.e != nil
}

func (si *SkipListIterator) Item() Item {
	vs := si.Value()
	return &Entry{
		Key:       si.Key(),
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}
}

func (si *SkipListIterator) Rewind() {
	si.e = si.sl.getHead().levels[0]
}

func (si *SkipListIterator) Seek(target []byte) {
	si.e, _ = si.sl.findNear(target, false, true)
}

func (si *SkipListIterator) Close() error {
	return nil
}

func (si *SkipListIterator) Key() []byte {
	return si.e.Key
}

func (si *SkipListIterator) Value() ValueStruct {
	return si.e.V
}
