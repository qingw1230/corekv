package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand     *rand.Rand // 用来生成新节点的层高
	maxLevel int
	length   int // 跳表中节点数据
	rw       sync.RWMutex
	size     int64 // 跳表中 kv 的总大小
}

func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())
	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
			entry:  nil,
			score:  0,
		},
		rand:     rand.New(source),
		maxLevel: defaultMaxLevel,
		length:   0,
	}
}

// Element 跳表的节点
type Element struct {
	// levels 节点各层的 next 指针
	levels []*Element
	entry  *Entry
	// score 由 Key 前 8 位计算出的分值，用来加速比较
	score float64
}

func newElement(score float64, entry *Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (e *Element) Entry() *Entry {
	return e.entry
}

func (sl *SkipList) Add(data *Entry) error {
	sl.rw.Lock()
	defer sl.rw.Unlock()

	score := sl.calcScore(data.Key)
	max := len(sl.header.levels)
	prevElem := sl.header
	// prevElems 记录要插入节点在各层的前一节点
	var prevElems [defaultMaxLevel]*Element

	// 从跳表最高层遍历到最低层寻找插入位置
	for i := max - 1; i >= 0; {
		prevElems[i] = prevElem
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := sl.compare(score, data.Key, next); comp <= 0 {
				// 下一节点与要插入的相同，覆盖原节点
				if comp == 0 {
					sl.size += data.Size() - next.Entry().Size()
					next.entry = data
					return nil
				}
				// 要插入的节点比 next 节点更小，继续看下一层
				break
			}
			// 要插入的节点比 next 节点更大，继续遍历当前层
			prevElem = next
			prevElems[i] = prevElem
		}

		// 下面几层 next 节点值与当前层一样，快速跳过
		topLevelValue := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevelValue; i-- {
			prevElems[i] = prevElem
		}
	}

	level := sl.randLevel()
	elem := newElement(score, data, level)

	// 将新节点插入跳表各层
	for i := 0; i < level; i++ {
		// cur->next = preNode->next
		elem.levels[i] = prevElems[i].levels[i]
		// preNode->next = cur
		prevElems[i].levels[i] = elem
	}

	sl.size += data.Size()
	sl.length++
	return nil
}

func (sl *SkipList) Search(key []byte) *Entry {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	if sl.length == 0 {
		return nil
	}

	score := sl.calcScore(key)
	prevElem := sl.header
	i := len(sl.header.levels) - 1

	for i >= 0 {
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := sl.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					return next.Entry()
				}
				// 去下一层找
				break
			}

			// 要找的值更大，继续在当前层找
			prevElem = next
		}

		topLevelValue := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevelValue; i-- {
		}
	}
	return nil
}

/*func (sl *SkipList) Remove(key []byte) error {
	score := sl.calcScore(key)
	max := len(sl.header.levels)
	prevElem := sl.header
	var prevElems [DefaultMaxLevel]*Element
	var elem *Element

	for i := max - 1; i >= 0; {
		prevElems[i] = prevElem
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := sl.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					elem = next
				}
				break
			}

			prevElem = next
			prevElems[i] = prevElem
		}

		topLevelValue := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevelValue; i-- {
			prevElems[i] = prevElem
		}
	}

	if elem == nil {
		return nil
	}

	prevTopLevel := len(elem.levels)
	for i := 0; i < prevTopLevel; i++ {
		// prevNode->next = cur->next
		prevElems[i].levels[i] = elem.levels[i]
	}

	sl.length--
	return nil
}*/

func (s *SkipList) Close() error {
	return nil
}

// calcScore 用 key 的前 8 位映射到一个值，用来加速比较
func (s *SkipList) calcScore(key []byte) float64 {
	var hash uint64
	l := len(key)
	if l > 8 {
		l = 8
	}

	// 将前 8 位字符对应的值映射到一个 uint64 中
	// 第 1 个字符对应前 8 位，第 2 个字符对应次 8 位，以此类推
	for i := 0; i < l; i++ {
		shift := uint64(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}
	score := float64(hash)
	return score
}

// compare 与 next 节点比较大小，小于返回 -1，大于返回 1，相等返回 0
func (s *SkipList) compare(score float64, key []byte, next *Element) int {
	// 只能说明前 8 位相等，还需要再比较后面
	if score == next.score {
		return bytes.Compare(key, next.Entry().Key)
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

// randLevel 随机生成新节点的层高
func (s *SkipList) randLevel() int {
	if s.maxLevel <= 1 {
		return 1
	}
	for i := 1; i < s.maxLevel; i++ {
		if RandN(100)%2 == 0 {
			return i
		}
	}
	return s.maxLevel
}

func (s *SkipList) Size() int64 {
	return s.size
}

type SkipListIterator struct {
	it *Element
	sl *SkipList
}

func (sl *SkipList) NewIterator(opt *Options) Iterator {
	iter := &SkipListIterator{
		it: sl.header,
		sl: sl,
	}
	return iter
}

func (iter *SkipListIterator) Next() {
	iter.it = iter.it.levels[0]
}
func (iter *SkipListIterator) Valid() bool {
	return iter.it != nil
}
func (iter *SkipListIterator) Rewind() {
	iter.it = iter.sl.header.levels[0]
}
func (iter *SkipListIterator) Item() Item {
	return iter.it
}
func (iter *SkipListIterator) Close() error {
	return nil
}
func (iter *SkipListIterator) Seek(key []byte) {
}
