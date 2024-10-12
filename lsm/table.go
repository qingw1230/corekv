package lsm

import (
	"os"
	"sync/atomic"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
)

type table struct {
	lm  *levelManager
	sst *file.SSTable
	fid uint64
	ref int32
}

func openTable(lm *levelManager, name string, builder *tableBuilder) (t *table) {
	var err error
	fid := utils.FID(name)

	if builder != nil {
		if t, err = builder.flush(lm, name); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{
			lm:  lm,
			fid: fid,
		}
		t.sst = file.OpenSSTable(&file.Options{
			FileName: name,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(lm.opt.SSTableMaxSz),
		})
	}

	t.IncrRef()
	if err := t.sst.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return
}

func (t *table) Delete() error {
	return t.sst.Delete()
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func decrRefs(tables []*table) error {
	for _, t := range tables {
		if err := t.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
