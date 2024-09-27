package utils

import (
	"bytes"
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

func newSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())
	return &SkipList{
		head: &Element{
			Key:    nil,
			Value:  nil,
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
	Value  []byte
	levels []*Element
}

func newElement(key, value []byte, level int) *Element {
	return &Element{
		Key:    key,
		Value:  value,
		levels: make([]*Element, level),
	}
}

func (e *Element) casNextPointer(h int, old, val *Element) bool {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&e.levels[h]))
	oldPtr := unsafe.Pointer(old)
	newPtr := unsafe.Pointer(val)
	return atomic.CompareAndSwapPointer(ptr, oldPtr, newPtr)
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

func (sl *SkipList) Add(e *Entry) {
	key, value := e.Key, e.Value
	var prev [DefaultMaxLevel + 1]*Element
	var next [DefaultMaxLevel + 1]*Element
	prev[DefaultMaxLevel] = sl.head

	for i := DefaultMaxLevel - 1; i >= 0; i-- {
		prev[i], next[i] = sl.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			prev[i].Value = value
			return
		}
	}

	level := sl.randLevel()
	data := newElement(key, value, level)

	for i := 0; i < level; i++ {
		for {
			data.levels[i] = next[i]
			if prev[i].casNextPointer(i, next[i], data) {
				break
			}
			prev[i], next[i] = sl.findSpliceForLevel(key, prev[i+1], i)
			if prev[i] == next[i] {
				prev[i].Value = value
				return
			}
		}
	}

	atomic.AddInt64(&sl.length, 1)
}

func (sl *SkipList) Search(key []byte) *Entry {
	if sl.length == 0 {
		return nil
	}

	prevElem := sl.head
	i := len(sl.head.levels) - 1

	for i >= 0 {
		for nextElem := prevElem.levels[i]; nextElem != nil; nextElem = prevElem.levels[i] {
			comp := bytes.Compare(key, nextElem.Key)
			if comp <= 0 {
				if comp == 0 {
					return newEntry(nextElem.Key, nextElem.Value)
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
	return nil
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
