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
	list := NewSkipList(1000)

	// Put & Get
	entry1 := NewEntry([]byte("Key1111111"), []byte(RandString(10)))
	list.Add(entry1)
	vs := list.Search(entry1.Key)
	assert.Equal(t, entry1.Value, vs.Value)

	entry2 := NewEntry([]byte("Key2222222"), []byte(RandString(10)))
	list.Add(entry2)
	vs = list.Search(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)

	// Get a not exist entry
	assert.Nil(t, list.Search([]byte(RandString(10))).Value)

	// Update a entry
	entry2_new := NewEntry([]byte("Key2222222"), []byte(RandString(10)))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl := NewSkipList(100000000)
		// 1 goroutine 插入并读取 100000 条数据耗时大约为 1.2 s
		maxTime := 100000

		// 预先创建 Entry 列表
		entries := make([]*Entry, maxTime)
		for i := 0; i < maxTime; i++ {
			key, val := RandString(10), fmt.Sprintf("Val%d", i)
			entry := NewEntry([]byte(key), []byte(val))
			entries[i] = entry
		}

		// 重置计时器，以便从循环开始时计时
		b.ResetTimer()
		for i := 0; i < maxTime; i++ {
			entry := entries[i]
			sl.Add(entry)
			searchVal := sl.Search([]byte(entry.Key))
			assert.Equal(b, searchVal.Value, entry.Value)
		}
	}
}

func Benchmark_SkipListBasicCRUD2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl := NewSkipList(100000000)
		// 1 goroutine 插入并读取 100000 条数据耗时大约为 1.2s
		// 2 goroutine 插入并读取 200000 条数据耗时大约为 1.5s
		maxTime := 200000

		// 预先创建 Entry 列表
		entries := make([]*Entry, maxTime)
		for i := 0; i < maxTime; i++ {
			key, val := RandString(10), fmt.Sprintf("Val%d", i)
			entry := NewEntry([]byte(key), []byte(val))
			entries[i] = entry
		}

		// 重置计时器，以便从循环开始时计时
		var wg sync.WaitGroup
		b.ResetTimer()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < maxTime/2; i++ {
				entry := entries[i]
				sl.Add(entry)
				searchVal := sl.Search([]byte(entry.Key))
				assert.Equal(b, searchVal.Value, entry.Value)
			}
		}()

		for i := maxTime / 2; i < maxTime; i++ {
			entry := entries[i]
			sl.Add(entry)
			searchVal := sl.Search([]byte(entry.Key))
			assert.Equal(b, searchVal.Value, entry.Value)
		}
		wg.Wait()
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(t, key(i), v.Value)
			return
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList(1 << 20)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = l.Search(key(i))
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(100000)

	//Put & Get
	entry1 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Add(entry1)
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Add(entry2)
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	//Update a entry
	entry2_new := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)

	iter := list.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s", iter.Item().Entry().Key, iter.Item().Entry().Value)
	}
}
