package lsm

import (
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
)

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
}

type Options struct {
	WorkDir            string
	MemTableSize       int64
	SSTableMaxSz       int64
	BlockSize          int // sst 文件中每个块的大小
	BloomFalsePositive float64
}

func (lsm *LSM) Close() error {
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	lsm.closer.Close()
	return nil
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	lsm.memTable, lsm.immutables = recovery(opt)
	lsm.levels = newLevelManager(opt)
	lsm.closer = utils.NewCloser(1)
	return lsm
}

func (lsm *LSM) StartMerge() {
	defer lsm.closer.Done()
	for {
		select {
		case <-lsm.closer.Wait():
			return
		}
	}
}

func (lsm *LSM) Set(entry *codec.Entry) (err error) {
	if lsm.memTable.Size() > lsm.option.MemTableSize {
		lsm.immutables = append(lsm.immutables, lsm.memTable)
		if lsm.memTable, err = NewMemtable(); err != nil {
			return err
		}
	}
	if err := lsm.memTable.set(entry); err != nil {
		return err
	}
	for _, immutable := range lsm.immutables {
		if err := lsm.levels.flush(immutable); err != nil {
			return err
		}
	}
	for i := 0; i < len(lsm.immutables); i++ {
		lsm.immutables[i].close()
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	if entry, err = lsm.memTable.Get(key); entry != nil {
		return entry, err
	}
	for _, imm := range lsm.immutables {
		if entry, err = imm.Get(key); entry != nil {
			return entry, err
		}
	}
	return lsm.levels.Get(key)
}
