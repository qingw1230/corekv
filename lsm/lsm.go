package lsm

import "github.com/qingw1230/corekv/utils"

type Options struct {
	BloomFalsePositive float64
	BlockSize          int // sst 文件中每个块的大小
	MemTableSize       int64
	SSTableMaxSz       int64
	WorkDir            string

	BaseLevelSize       int64
	LevelSizeMultiplier int
	MaxLevelNum         int
	NumLeverZeroTables  int // L0 层 sst 文件数量最大值，默认 15
	TableSizeMultiplier int
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

func (lsm *LSM) Close() error {
	lsm.closer.Close()
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for _, t := range lsm.immutables {
		if err := t.close(); err != nil {
			return err
		}
	}
	if err := lsm.lm.close(); err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) Set(e *utils.Entry) (err error) {
	if e == nil || len(e.Key) == 0 {
		return utils.ErrEmptyKey
	}

	// 优雅关闭
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	if int64(lsm.memTable.wal.Size())+int64(utils.EstimateWalCodecSize(e)) > lsm.option.MemTableSize {
		lsm.Rotate()
	}
	if err = lsm.memTable.set(e); err != nil {
		return err
	}
	// 将不可变内存表刷新到磁盘，形成 sst 文件
	for _, t := range lsm.immutables {
		if err = lsm.lm.flush(t); err != nil {
			return err
		}
		err = t.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

func (lsm *LSM) Get(key []byte) (e *utils.Entry, err error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	if e, err = lsm.memTable.Get(key); e != nil && e.Value != nil {
		return
	}
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if e, err = lsm.immutables[i].Get(key); e != nil && e.Value != nil {
			return e, err
		}
	}
	return lsm.lm.Get(key)
}

// Rotate 将 memTable 变为不可变内存表，创建新的 memTable
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemTable()
}
