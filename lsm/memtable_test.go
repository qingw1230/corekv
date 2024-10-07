package lsm

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/qingw1230/corekv/utils"
)

var (
	opt = &Options{
		WorkDir:      "../work_test",
		MemTableSize: 2 * 1024,
	}
)

func TestMemtable_Set(t *testing.T) {
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
}

func clearDir(opt *Options) {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
