package lsm

import (
	"bytes"
	"fmt"
	"io/ioutil"
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

// memTable 内存表包含 wal 文件和跳表
type memTable struct {
	lsm        *LSM // 所属 LSM
	wal        *file.WalFile
	sl         *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

// NewMemTable 创建一个新的内存表
func (lsm *LSM) NewMemTable() *memTable {
	// 分配一个新的文件 id
	newFID := atomic.AddUint64(&(lsm.lm.maxFID), 1)
	fileOpt := &file.Options{
		FID:      newFID,
		FileName: mtFilePath(lsm.option.WorkDir, newFID),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
	}
	return &memTable{
		lsm: lsm,
		wal: file.OpenWalFile(fileOpt),
		sl:  utils.NewSkipList(int64(1 << 20)),
	}
}

// OpenMemTable 打开指定内存表，并将 wal 文件数据重新插入跳表
func (lsm *LSM) OpenMemTable(fid uint64) (*memTable, error) {
	fileOpt := &file.Options{
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
	}
	sl := utils.NewSkipList(int64(1 << 20))
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

// set 添加数据
func (mt *memTable) set(e *utils.Entry) error {
	// 先写到 wal 文件中，防止崩溃
	if err := mt.wal.Write(e); err != nil {
		return err
	}
	mt.sl.Add(e)
	return nil
}

// Get 从跳表中检索数据
func (mt *memTable) Get(key []byte) (*utils.Entry, error) {
	vs := mt.sl.Search(key)

	e := &utils.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}

	return e, nil
}

func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}

// recovery 根据 wal 文件恢复跳表结构
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}

	// fids 记录所有 wal 文件 ID，并排升序
	var fids []uint64
	maxFID := lsm.lm.maxFID
	// 找出所有的 wal 文件
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

	// 记录不变的内存表
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

// mtFilePath 生成 wal 文件全路径
func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}

// UpdateSkipList 遍历 wal 文件，将数据重新插入跳表
func (mt *memTable) UpdateSkipList() error {
	if mt.wal == nil || mt.sl == nil {
		return nil
	}
	// 遍历 wal 文件，将解析出的 entry 重新插入跳表
	endOff, err := mt.wal.Iterate(true, 0, mt.replayFunction(mt.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", mt.wal.Name()))
	}
	return mt.wal.Truncate(int64(endOff))
}

// replayFunction 将 entry 插入到 mt.sl 中
func (mt *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error {
		if ts := utils.ParseTs(e.Key); ts > mt.maxVersion {
			mt.maxVersion = ts
		}
		mt.sl.Add(e)
		return nil
	}
}
