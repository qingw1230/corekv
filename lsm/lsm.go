package lsm

import "github.com/qingw1230/corekv/utils"

type Options struct {
	WorkDir            string
	MemTableSize       int64
	SSTableMaxSz       int64
	BlockSize          int // sst 文件中每个块的大小
	BloomFalsePositive float64
}

type LSM struct {
	option     *Options
	lm         *levelManager
	memTable   *memTable
	immutables []*memTable
	closer     *utils.Closer
	maxMemFID  uint64
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	lsm.lm = lsm.initLevelManager(opt)
	lsm.memTable, lsm.immutables = lsm.recovery()
	lsm.closer = utils.NewCloser()
	return lsm
}
