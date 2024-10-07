package utils

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSkipList_Add(t *testing.T) {
	sl := NewSkipList()
	cnt := 100000
	num := 4
	var m sync.Map
	var wg sync.WaitGroup
	wg.Add(num)

	strsTable := make([][]string, num)

	for i := 0; i < num; i++ {
		strsTable[i] = GenerateStrs(cnt)
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
	cnt := 100_000
	num := 4
	var wg sync.WaitGroup
	wg.Add(num)

	entriesTable := make([][]*Entry, num)
	for i := 0; i < num; i++ {
		entriesTable[i] = GenerateEntries(cnt)
	}

	time1 := time.Now().UnixMilli()

	for i := 0; i < num; i++ {
		go func(i int) {
			for _, e := range entriesTable[i] {
				sl.Add(e)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	time2 := time.Now().UnixMilli()
	fmt.Println(time2 - time1)
}

func BenchmarkSkipList_Add(b *testing.B) {
	sl := NewSkipList()
	cnt := 100000

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			b.StopTimer()
			strs := GenerateStrs(cnt)
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

func addData(sl *SkipList, strs []string) {
	for i := 0; i < len(strs); i++ {
		entry := &Entry{
			Key:   []byte(strs[i]),
			Value: []byte(strs[i]),
		}
		sl.Add(entry)
	}
}
