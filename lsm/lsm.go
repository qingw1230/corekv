package lsm

type Options struct {
	WorkDir            string
	MemTableSize       int64
	SSTableMaxSz       int64
	BlockSize          int // sst 文件中每个块的大小
	BloomFalsePositive float64
}

type LSM struct {
	option     *Options
	memTable   *memTable
	immutables []*memTable
	lm         *levelManager
	maxMemFID  uint64
}
