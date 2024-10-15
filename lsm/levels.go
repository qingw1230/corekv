package lsm

import (
	"sync"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
)

type levelManager struct {
	lsm          *LSM // 所属 LSM
	opt          *Options
	levels       []*levelHandler    // 各层的管理句柄
	manifestFile *file.ManifestFile // 保存 sst 文件的层级关系
	maxFID       uint64             // 用于生成文件 ID
}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		lsm:    lsm,
		opt:    opt,
		levels: make([]*levelHandler, 1),
	}
	lm.levels[0] = &levelHandler{
		lm:       lm,
		rw:       sync.RWMutex{},
		levelNum: 0,
	}
	err := lm.loadManifest()
	utils.Panic(err)
	return lm
}

func (lm *levelManager) close() error {
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	return nil
}

func (lm *levelManager) Get(key []byte) (e *utils.Entry, err error) {
	// 在 L0 层查找
	if e, err = lm.levels[0].Get(key); e != nil {
		return
	}
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		lh := lm.levels[level]
		if e, err = lh.Get(key); e != nil {
			return
		}
	}
	return e, utils.ErrKeyNotFount
}

// flush 将内存表刷新到磁盘形成 sst 文件
func (lm *levelManager) flush(immutable *memTable) error {
	nextID := immutable.wal.FID()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, nextID)

	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		e := iter.Item().Entry()
		builder.add(e, false)
	}

	t := openTable(lm, sstName, builder)

	err := lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       nextID,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})
	utils.Panic(err)

	lm.levels[0].add(t)
	return nil
}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{
		Dir: lm.opt.WorkDir,
	})
	return
}

type levelHandler struct {
	lm             *levelManager // 所属 levelManager
	rw             sync.RWMutex
	levelNum       int      // 所管理的层级
	tables         []*table // 当前层管理的 sst 文件
	totalSize      int64
	totalStaleSize int64
}

func (lh *levelHandler) close() error {
	for _, t := range lh.tables {
		if err := t.sst.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 {
		return lh.serachL0SST(key)
	}
	return nil, nil
}

func (lh *levelHandler) serachL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, t := range lh.tables {
		if e, err := t.Search(key, &version); err == nil {
			return e, nil
		}
	}
	return nil, utils.ErrKeyNotFount
}

// add 添加管理的 sst 文件
func (lh *levelHandler) add(t *table) {
	lh.rw.Lock()
	defer lh.rw.Unlock()
	lh.tables = append(lh.tables, t)
}
