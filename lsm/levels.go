package lsm

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
)

type levelManager struct {
	maxFid   uint32
	opt      *Options
	cache    *cache
	manifest *file.Manifest
	levels   []*levelHandler
}

type levelHandler struct {
	levelNum int
	tables   []*table
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) add(sst *file.SSTable) {
	lh.tables = append(lh.tables, &table{
		ss:   sst,
		idxs: sst.Indexs(),
	})
}

func (lh *levelHandler) Get(key []byte) (*codec.Entry, error) {
	if lh.levelNum == 0 {
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*codec.Entry, error) {
	for _, table := range lh.tables {
		if entry, err := table.Serach(key); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) searchLNSST(key []byte) (*codec.Entry, error) {
	table := lh.getTable(key)
	if entry, err := table.Serach(key); err == nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if bytes.Compare(key, lh.tables[i].ss.MinKey()) > -1 &&
			bytes.Compare(key, lh.tables[i].ss.MaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifest.Close(); err != nil {
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
	return entry, nil
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	lm.loadManifest()
	lm.build()
	return lm
}

func (lm *levelManager) loadCache() {
	lm.cache = newCache(lm.opt)
	for _, level := range lm.levels {
		for _, table := range level.tables {
			lm.cache.addIndex(table.ss.FID(), table)
		}
	}
}

func (lm *levelManager) loadManifest() {
	lm.manifest = file.OpenManifest(&file.Options{Name: "MANIFEST", Dir: lm.opt.WorkDir})
}

func (lm *levelManager) build() {
	lm.levels = make([]*levelHandler, utils.MaxLevelNum)
	tables := lm.manifest.Tables()
	var maxFid uint32
	for num := 0; num < utils.MaxLevelNum; num++ {
		lm.levels[num] = &levelHandler{levelNum: num}
		lm.levels[num].tables = make([]*table, len(tables[num]))
		for i := range tables[num] {
			ot := openTable(lm, tables[num][i].SSTName)
			lm.levels[num].tables[i] = ot
			if ot.fid > maxFid {
				maxFid = ot.fid
			}
		}
	}
	lm.maxFid = maxFid
	lm.loadCache()
}

func (lm *levelManager) flush(immutable *memTable) error {
	newFid := atomic.AddUint32(&lm.maxFid, 1)
	sstName := fmt.Sprintf("%05d.sst", newFid)
	sst := file.OpenSStable(&file.Options{Name: sstName, Dir: lm.opt.WorkDir})
	if err := sst.SaveSkipListToSSTable(immutable.sl); err != nil {
		return err
	}
	lm.levels[0].add(sst)
	return lm.manifest.AppendSST(0, &file.Cell{
		SSTName: sstName,
	})
}
