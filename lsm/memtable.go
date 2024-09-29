package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
)

const walFileExt string = ".wal"

// memTable 内存表包含跳表和 wal 文件
type memTable struct {
	lsm        *LSM // 所属 LSM
	sl         *utils.SkipList
	wal        *file.WalFile
	buf        *bytes.Buffer
	maxVersion uint64
}

func (lsm *LSM) NewMemTable() *memTable {
	newFID := atomic.AddUint64(&lsm.lm.maxFID, 1)
	fileOpt := &file.Options{
		FID:      newFID,
		FileName: mtFilePath(lsm.option.WorkDir, newFID),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
	}
	return &memTable{
		lsm: lsm,
		sl:  utils.NewSkipList(),
		wal: file.OpenWalFile(fileOpt),
	}
}

// OpenMemTable 打开内存表，并用 wal 文件恢复跳表结构
func (lsm *LSM) OpenMemTable(fid uint64) (*memTable, error) {
	fileOpt := &file.Options{
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
	}
	sl := utils.NewSkipList()
	mt := &memTable{
		lsm: lsm,
		sl:  sl,
		buf: &bytes.Buffer{},
	}
	mt.wal = file.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}

func (mt *memTable) close() error {
	if err := mt.wal.Close(); err != nil {
		return err
	}
	return nil
}

func (mt *memTable) set(e *utils.Entry) error {
	// 先将数据写到 wal 文件，防止崩溃后数据丢失
	if err := mt.wal.Write(e); err != nil {
		return err
	}
	mt.sl.Add(e)
	return nil
}

func (mt *memTable) Get(key []byte) (*utils.Entry, error) {
	e := mt.sl.Search(key)
	return e, nil
}

func (mt *memTable) Size() int64 {
	return mt.sl.MemSize()
}

func (lsm *LSM) recovery() (*memTable, []*memTable) {
	files, err := os.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}

	// fids 记录所有 wal 文件 ID，并排升序
	var fids []uint64
	maxFID := lsm.lm.maxFID
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-len(walFileExt)], 10, 64)
		if maxFID < fid {
			maxFID = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	imms := []*memTable{}
	for _, fid := range fids {
		mt, err := lsm.OpenMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			continue
		}
		imms = append(imms, mt)
	}
	lsm.lm.maxFID = maxFID
	return lsm.NewMemTable(), imms
}

// UpdateSkipList 使用 wal 文件恢复跳表
func (mt *memTable) UpdateSkipList() error {
	if mt.wal == nil || mt.sl == nil {
		return nil
	}
	endOff, err := mt.wal.Iterator(true, 0, mt.replayFunction(mt.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", mt.wal.Name()))
	}
	return mt.wal.Truncate(int64(endOff))
}

// replayFunction 重放函数，用于将 wal 中的 entry 插入到跳表中
func (mt *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error {
		if ts := utils.ParseTs(e.Key); ts > mt.maxVersion {
			mt.maxVersion = ts
		}
		mt.sl.Add(e)
		return nil
	}
}

func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
