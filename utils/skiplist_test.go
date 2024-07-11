package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipListBasicCRUD(t *testing.T) {
	sl := NewSkipList(1000)

	// Add & Search
	entry1 := NewEntry([]byte("Key1"), []byte("Value1"))
	assert.Nil(t, sl.Add(entry1))
	assert.Equal(t, entry1.Value, sl.Search(entry1.Key).Value)

	entry2 := NewEntry([]byte("Key2"), []byte("Value2"))
	sl.Add(entry2)
	assert.Equal(t, entry2.Value, sl.Search(entry2.Key).Value)

	// 获取一个不存在的 entry
	assert.Nil(t, sl.Search([]byte("noexist")))

	// 更新一个 entry
	entry1_new := NewEntry([]byte("Key1"), []byte("Val1+1"))
	assert.Nil(t, sl.Add(entry1_new))
	assert.Equal(t, entry1_new.Value, sl.Search(entry1_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	sl := NewSkipList(1000)
	key, val := "", ""
	maxTime := 10000

	for i := 0; i < maxTime; i++ {
		key, val = RandString(20), RandString(100)
		entry := NewEntry([]byte(key), []byte(val))
		e := sl.Add(entry)
		assert.Equal(b, nil, e)
		searchVal := sl.Search([]byte(key))
		if searchVal != nil {
			assert.Equal(b, []byte(val), searchVal.Value)
		}
	}
}

func TestConcurrentBasicCRUD(t *testing.T) {
	const n = 10000
	sl := NewSkipList(1000)
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(t, sl.Add(NewEntry(key(i), key(i))))
		}(i)
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := sl.Search(key(i))
			if v != nil {
				require.EqualValues(t, key(i), v.Value)
				return
			}
			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasicCRUD(b *testing.B) {
	const n = 10000
	sl := NewSkipList(1000)
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(b, sl.Add(NewEntry(key(i), key(i))))
		}(i)
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := sl.Search(key(i))
			if v != nil {
				require.EqualValues(b, key(i), v.Value)
				return
			}
			require.Nil(b, v)
		}(i)
	}
	wg.Wait()
}
