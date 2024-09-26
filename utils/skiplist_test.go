package utils

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList_Add(t *testing.T) {
	sl := newSkipList()
	cnt := 100000
	num := 5
	var wg sync.WaitGroup
	wg.Add(num)

	addData := func() {
		strs := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			strs[i] = []byte(RandStringRandomLength(100, true))
		}

		for i := 0; i < cnt; i++ {
			entry := &Entry{
				Key:   strs[i],
				Value: strs[i],
			}
			sl.Add(entry)
			assert.Equal(t, 0, bytes.Compare(strs[i], sl.Search(strs[i]).Value))
		}
		wg.Done()
	}

	for i := 0; i < num; i++ {
		go addData()
	}

	wg.Wait()
	assert.Equal(t, num*cnt, sl.length)
}
