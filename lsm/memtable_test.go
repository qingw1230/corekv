package lsm

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/qingw1230/corekv/utils"
	"github.com/stretchr/testify/assert"
)

func TestMemtable_Set(t *testing.T) {
	clearDir(opt)
	lsm := &LSM{
		option: opt,
	}
	lsm.memTable = lsm.NewMemTable()
	cnt := 100_000
	num := 4
	var m sync.Map
	var wg sync.WaitGroup
	wg.Add(num)

	entriesTable := make([][]*utils.Entry, num)
	for i := 0; i < num; i++ {
		entriesTable[i] = utils.GenerateEntries(cnt)
	}

	for i := 0; i < num; i++ {
		go func(i int) {
			for _, e := range entriesTable[i] {
				lsm.memTable.set(e)
				m.Store(string(e.Key), struct{}{})
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	assert.Equal(t, true, verifyCorrectness(lsm.memTable, &m))
}

func TestMemtable_Set2(t *testing.T) {
	clearDir(opt)
	lsm := &LSM{
		option: opt,
	}
	lsm.memTable = lsm.NewMemTable()
	cnt := 100_000
	num := 4
	var wg sync.WaitGroup
	wg.Add(num)

	entriesTable := make([][]*utils.Entry, num)
	for i := 0; i < num; i++ {
		entriesTable[i] = utils.GenerateEntries(cnt)
	}

	time1 := time.Now().UnixMilli()

	for i := 0; i < num; i++ {
		go func(i int) {
			for _, e := range entriesTable[i] {
				lsm.memTable.set(e)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	time2 := time.Now().UnixMilli()
	fmt.Println(time2 - time1)
	fmt.Println(lsm.memTable.wal.WriteDoneSeq())
}

func verifyCorrectness(memTable *memTable, m *sync.Map) bool {
	cnt := int64(0)
	m.Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	if cnt != memTable.sl.MemSize() {
		fmt.Println(cnt)
		fmt.Println(memTable.sl.MemSize())
		return false
	}
	return true
}
