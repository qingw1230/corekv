package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/qingw1230/corekv/utils/codec"
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

func TestSkipList_compare(t *testing.T) {
	sl := SkipList{
		header:   nil,
		rand:     nil,
		maxLevel: 0,
		length:   0,
	}

	byte1 := []byte("112345678")
	byte2 := []byte("212345678")

	byte1Score := sl.calcScore(byte1)
	byte2Score := sl.calcScore(byte2)

	e := &Element{
		levels: nil,
		Key:    byte2,
		Val:    nil,
		score:  byte2Score,
	}

	assert.Equal(t, -1, sl.compare(byte1Score, byte1, e))
	assert.Equal(t, 0, sl.compare(byte2Score, byte2, e))
}

func TestSkipListBasicCRUD(t *testing.T) {
	sl := NewSkipList()

	// Add & Search
	entry1 := codec.NewEntry([]byte("Key1"), []byte("Value1"))
	assert.Nil(t, sl.Add(entry1))
	assert.Equal(t, entry1.Value, sl.Search(entry1.Key).Value)

	entry2 := codec.NewEntry([]byte("Key2"), []byte("Value2"))
	sl.Add(entry2)
	assert.Equal(t, entry2.Value, sl.Search(entry2.Key).Value)

	// 获取一个不存在的 entry
	assert.Nil(t, sl.Search([]byte("noexist")))

	// 更新一个 entry
	entry1_new := codec.NewEntry([]byte("Key1"), []byte("Val1+1"))
	assert.Nil(t, sl.Add(entry1_new))
	assert.Equal(t, entry1_new.Value, sl.Search(entry1_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	sl := NewSkipList()
	key, val := "", ""
	maxTime := 1_000_000

	for i := 0; i < maxTime; i++ {
		key, val = RandString(20), RandString(100)
		entry := codec.NewEntry([]byte(key), []byte(val))
		e := sl.Add(entry)
		assert.Equal(b, nil, e)
		searchVal := sl.Search([]byte(key))
		assert.Equal(b, []byte(val), searchVal.Value)
	}
}

func TestConcurrentBasicCRUD(t *testing.T) {
	const n = 10_000
	sl := NewSkipList()
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	var wg sync.WaitGroup

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			assert.Nil(t, sl.Add(codec.NewEntry(key(i), key(i))))
		}(i)
	}

	wg.Add(n)
	for i := 0; i < n; i++ {
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

func Benchmark_ConcurrendBasicCRUD(b *testing.B) {
	const n = 10_000
	sl := NewSkipList()
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	var wg sync.WaitGroup

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			assert.Nil(b, sl.Add(codec.NewEntry(key(i), key(i))))
		}(i)
	}

	wg.Add(n)
	for i := 0; i < n; i++ {
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
