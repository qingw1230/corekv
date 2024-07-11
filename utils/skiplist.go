package utils

import (
	"bytes"
	"sync"
)

const (
	defaultMaxLevel = 20
)

type SkipList struct {
	maxLevel   int
	rw         sync.RWMutex
	curHeight  int32  // 跳表当前的最大高度
	headOffset uint32 // 头结点在 arena 中的偏移量
	arena      *Arena
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	head := newElement(arena, nil, ValueStruct{}, defaultMaxLevel)
	ho := arena.getElementOffset(head)
	return &SkipList{
		curHeight:  1,
		headOffset: ho,
		arena:      arena,
	}
}

// Element 跳表的节点
type Element struct {
	// score 由 Key 前 8 位计算出的分值，用来加速比较
	score float64
	// value 将 value 的 off 和 size 组装成一个 uint64，实现原子化的操作
	value uint64

	keyOffset uint32
	keySize   uint16

	height uint16

	levels [defaultMaxLevel]uint32
}

func newElement(arena *Arena, key []byte, v ValueStruct, height int) *Element {
	nodeOffset := arena.putNode(height)

	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	elem := arena.getElement(nodeOffset)
	elem.score = calcScore(key)
	elem.keyOffset = keyOffset
	elem.keySize = uint16(len(key))
	elem.height = uint16(height)
	elem.value = val

	return elem
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func (e *Element) key(arena *Arena) []byte {
	return arena.getKey(e.keyOffset, e.keySize)
}

func (sl *SkipList) Add(data *Entry) error {
	sl.rw.Lock()
	defer sl.rw.Unlock()

	score := calcScore(data.Key)
	var elem *Element
	value := ValueStruct{
		Value: data.Value,
	}

	//从当前最大高度开始
	max := sl.curHeight
	//拿到头节点，从第一个开始
	prevElem := sl.arena.getElement(sl.headOffset)
	// prevElems 记录要插入节点在各层的前一节点
	var prevElems [defaultMaxLevel]*Element

	// 从跳表最高层遍历到最低层寻找插入位置
	for i := max - 1; i >= 0; {
		prevElems[i] = prevElem
		for next := sl.getNext(prevElem, int(i)); next != nil; next = sl.getNext(prevElem, int(i)) {
			if comp := sl.compare(score, data.Key, next); comp <= 0 {
				// 下一节点与要插入的相同，覆盖原节点
				if comp == 0 {
					vo := sl.arena.putVal(value)
					encV := encodeValue(vo, value.EncodedSize())
					next.value = encV
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
	elem = newElement(sl.arena, data.Key, ValueStruct{Value: data.Value}, level)
	off := sl.arena.getElementOffset(elem)

	// 将新节点插入跳表各层
	for i := 0; i < level; i++ {
		// cur->next = preNode->next
		elem.levels[i] = prevElems[i].levels[i]
		// preNode->next = cur
		prevElems[i].levels[i] = off
	}

	return nil
}

func (sl *SkipList) Search(key []byte) *Entry {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	if sl.arena.Size() == 0 {
		return nil
	}

	score := calcScore(key)
	prevElem := sl.arena.getElement(sl.headOffset)
	i := sl.curHeight

	for i >= 0 {
		for next := sl.getNext(prevElem, int(i)); next != nil; next = sl.getNext(prevElem, int(i)) {
			if comp := sl.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					vo, vSize := decodeValue(next.value)
					return &Entry{Key: key, Value: sl.arena.getVal(vo, vSize).Value}
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

func (s *SkipList) Close() error {
	return nil
}

// calcScore 用 key 的前 8 位映射到一个值，用来加速比较
func calcScore(key []byte) float64 {
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
		return bytes.Compare(key, next.key(s.arena))
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

func (s *SkipList) getNext(e *Element, height int) *Element {
	return s.arena.getElement(e.getNextOffset(height))
}

func (s *SkipList) Size() int64 {
	return s.arena.Size()
}

type SkipListIterator struct {
	rw   sync.RWMutex
	sl   *SkipList
	elem *Element
}

func (sl *SkipList) NewIterator() Iterator {
	return &SkipListIterator{
		sl: sl,
	}
}

func (iter *SkipListIterator) Next() {
	AssertTrue(iter.Valid())
	iter.elem = iter.sl.getNext(iter.elem, 0)
}

func (iter *SkipListIterator) Valid() bool {
	return iter.elem != nil
}

func (iter *SkipListIterator) Rewind() {
	head := iter.sl.arena.getElement(iter.sl.headOffset)
	iter.elem = iter.sl.getNext(head, 0)
}

func (iter *SkipListIterator) Item() Item {
	vo, vs := decodeValue(iter.elem.value)
	return &Entry{
		Key:       iter.sl.arena.getKey(iter.elem.keyOffset, iter.elem.keySize),
		Value:     iter.sl.arena.getVal(vo, vs).Value,
		ExpiresAt: iter.sl.arena.getVal(vo, vs).ExpiresAt,
	}
}

func (iter *SkipListIterator) Close() error {
	return nil
}

func (iter *SkipListIterator) Seek(key []byte) {
}
