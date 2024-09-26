package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultMaxLevel = 20
)

// SkipList 支持并发读写的跳表
type SkipList struct {
	head     *Element // head 跳表的头节点
	rand     *rand.Rand
	maxLevel int
	length   int // length 跳表中节点数量
	rw       sync.RWMutex
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

func (sl *SkipList) Add(data *Entry) error {
	sl.rw.Lock()
	defer sl.rw.Unlock()

	prevElem := sl.head
	var prevElems [DefaultMaxLevel]*Element

	for i := len(sl.head.levels) - 1; i >= 0; {
		prevElems[i] = prevElem
		for nextElem := prevElem.levels[i]; nextElem != nil; nextElem = prevElem.levels[i] {
			comp := bytes.Compare(data.Key, nextElem.Key)
			if comp <= 0 {
				if comp == 0 {
					nextElem.Value = data.Value
					return nil
				}
				break
			}

			prevElem = nextElem
			prevElems[i] = prevElem
		}

		topLevelVal := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevelVal; i-- {
			// 下一层值与当前层值相同，再到下一层
			prevElems[i] = prevElem
		}
	}

	level := sl.randLevel()
	elem := newElement(data.Key, data.Value, level)

	for i := 0; i < level; i++ {
		// elem->next = preNode->next
		elem.levels[i] = prevElems[i].levels[i]
		// preNode->next = elem
		prevElems[i].levels[i] = elem
	}

	sl.length++
	return nil
}

func (sl *SkipList) Search(key []byte) *Entry {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

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
