package lsm

type Options struct {
	WorkDir      string
	MemTableSize int64
}

type LSM struct {
	option     *Options
	memTable   *memTable
	immutables []*memTable
	lm         *levelManager
	maxMemFID  uint64
}
