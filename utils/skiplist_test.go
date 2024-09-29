package utils

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList_Add(t *testing.T) {
	sl := NewSkipList()
	cnt := 100000
	num := 4
	var m sync.Map
	var wg sync.WaitGroup
	wg.Add(num)

	strsTable := make([][][]byte, num)

	for i := 0; i < num; i++ {
		strsTable[i] = generateData(cnt)
	}

	for i := 0; i < num; i++ {
		go func(i int) {
			addData(sl, strsTable[i])
			for _, str := range strsTable[i] {
				m.Store(string(str), struct{}{})
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	assert.Equal(t, true, verifyCorrectness(sl, &m))
}

func TestSkipList_Add2(t *testing.T) {
	sl := NewSkipList()
	cnt := 100000
	num := 4
	var wg sync.WaitGroup
	wg.Add(num)

	strsTable := make([][][]byte, num)

	for i := 0; i < num; i++ {
		strsTable[i] = generateData(cnt)
	}

	for i := 0; i < num; i++ {
		go func(i int) {
			addData(sl, strsTable[i])
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func BenchmarkSkipList_Add(b *testing.B) {
	sl := NewSkipList()
	cnt := 100000

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			b.StopTimer()
			strs := generateData(cnt)
			b.StartTimer()
			addData(sl, strs)
		}
	})
}

// verifyCorrectness 以跳表节点数量和前后节点大小关系两方面确认插入正确性
func verifyCorrectness(sl *SkipList, m *sync.Map) bool {
	cnt := int64(0)
	m.Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	if cnt != sl.length {
		return false
	}

	cur := sl.head.levels[0]
	for cur != nil && cur.levels[0] != nil {
		next := cur.levels[0]
		comp := bytes.Compare(cur.Key, next.Key)
		if comp > 0 {
			return false
		}
		cur = next
	}
	return true
}

func addData(sl *SkipList, strs [][]byte) {
	for i := 0; i < len(strs); i++ {
		entry := &Entry{
			Key:   strs[i],
			Value: strs[i],
		}
		sl.Add(entry)
	}
}

func generateData(cnt int) [][]byte {
	strs := make([][]byte, cnt)
	for i := 0; i < cnt; i++ {
		str := RandStringRandomLength(100, true)
		strs[i] = []byte(str)
	}
	return strs
}
