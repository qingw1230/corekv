package lsm

import (
	"os"
	"testing"

	"github.com/qingw1230/corekv/utils"
	"github.com/stretchr/testify/assert"
)

var (
	opt = &Options{
		WorkDir:      "../work_test",
		MemTableSize: 2 * 1024,
	}
)

func TestLsm_Recovery(t *testing.T) {
	clearDir(opt)
	lsm1 := buildLSM(opt)

	// 首先生成一个 wal 文件
	lsm1.memTable = lsm1.NewMemTable()
	cnt := 100_000
	entries := utils.GenerateEntries(cnt)
	for _, e := range entries {
		lsm1.memTable.set(e)
	}
	lsm1.memTable.wal.Sync()

	// 利用 wal 文件恢复跳表结构
	lsm2 := buildLSM(opt)
	lsm2.memTable, lsm2.immutables = lsm2.recovery()

	assert.Equal(t, true, lsm1.memTable.sl.CompareSkipList(lsm2.immutables[0].sl))
}

// buildLSM 构建一个 LSM 树
func buildLSM(opt *Options) *LSM {
	lsm := NewLSM(opt)
	return lsm
}

// runTest testFunList 中的每个函数执行 n 次
func runTest(n int, testFunList ...func()) {
	for _, fun := range testFunList {
		for i := 0; i < n; i++ {
			fun()
		}
	}
}

func clearDir(opt *Options) {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
