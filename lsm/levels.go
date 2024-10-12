package lsm

import "github.com/qingw1230/corekv/utils"

type levelManager struct {
	lsm    *LSM // 所属 LSM
	opt    *Options
	maxFID uint64 // 用于生成文件 ID
}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		opt: opt,
		lsm: lsm,
	}
	return lm
}

func (lm *levelManager) close() error {
	return nil
}

func (lm *levelManager) flush(immutable *memTable) error {
	nextID := immutable.wal.FID()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, nextID)

	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		e := iter.Item().Entry()
		builder.add(e, false)
	}

	openTable(lm, sstName, builder)
	return nil
}
