package lsm

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/iterator"
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
)

type levelManager struct {
	maxFid       uint64
	opt          *Options
	cache        *cache
	manifestFile *file.ManifestFile
	levels       []*levelHandler
}

type levelHandler struct {
	sync.RWMutex
	levelNum int
	tables   []*table
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) add(t *table) {
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) Get(key []byte) (*codec.Entry, error) {
	if lh.levelNum == 0 {
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].sst.MinKey(), lh.tables[j].sst.MinKey()) < 0
		})
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*codec.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if table == nil {
			return nil, utils.ErrKeyNotFound
		}
		if entry, err := table.Serach(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) searchLNSST(key []byte) (*codec.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if entry, err := table.Serach(key, &version); err == nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if bytes.Compare(key, lh.tables[i].sst.MinKey()) > -1 &&
			bytes.Compare(key, lh.tables[i].sst.MaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm

}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	for level := 1; level < utils.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}

func (lm *levelManager) loadCache() {
	lm.cache = newCache(lm.opt)
	for _, level := range lm.levels {
		for _, table := range level.tables {
			lm.cache.addIndex(table.sst.FID(), table)
		}
	}
}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, utils.MaxLevelNum)
	for i := 0; i < utils.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
		})
	}

	manifest := lm.manifestFile.GetManifest()
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	var maxFid uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFid {
			maxFid = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].tables = append(lm.levels[tableInfo.Level].tables, t)
	}
	for i := 0; i < utils.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	lm.maxFid = maxFid
	lm.loadCache()
	return nil
}

func (lm *levelManager) flush(immutable *memTable) error {
	nextID := atomic.AddUint64(&lm.maxFid, 1)
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, nextID)

	builder := newTableBuiler(lm.opt)
	iter := immutable.sl.NewIterator(&iterator.Options{})
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry)
	}
	table := openTable(lm, sstName, builder)

	lm.levels[0].add(table)
	return lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       nextID,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})
}
