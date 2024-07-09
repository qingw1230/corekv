package lsm

import (
	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
)

type memTable struct {
	wal *file.WalFile
	sl  *utils.SkipList
}

func NewMemtable() (*memTable, error) {
	return nil, nil
}

func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.sl.Close(); err != nil {
		return err
	}
	return nil
}

func (m *memTable) set(entry *codec.Entry) error {
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	if err := m.sl.Add(entry); err != nil {
		return err
	}
	return nil
}

func (m *memTable) Get(key []byte) (*codec.Entry, error) {
	return m.sl.Search(key), nil
}

func recovery(opt *Options) (*memTable, []*memTable) {
	fileOpt := &file.Options{
		Dir:  opt.WorkDir,
		Name: "00001.mem",
	}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList()}, []*memTable{}
}

func (m *memTable) Size() int64 {
	return m.sl.Size()
}
