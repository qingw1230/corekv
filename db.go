package corekv

import (
	"github.com/qingw1230/corekv/iterator"
	"github.com/qingw1230/corekv/lsm"
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
	"github.com/qingw1230/corekv/vlog"
)

type (
	CoreAPI interface {
		Set(data *codec.Entry) error
		Get(key []byte) (*codec.Entry, error)
		Del(key []byte) error
		NewIterator(opt *iterator.Options) iterator.Iterator
		Info() *Stats
		Close() error
	}

	DB struct {
		opt   *Options
		lsm   *lsm.LSM
		vlog  *vlog.VLog
		stats *Stats
	}
)

func Open(opt *Options) *DB {
	db := &DB{
		opt: opt,
	}
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:            opt.WorkDir,
		MemTableSize:       opt.MemTableSize,
		SSTableMaxSz:       opt.SSTableMaxSz,
		BlockSize:          4 * 1024,
		BloomFalsePositive: 0.01,
	})
	db.vlog = vlog.NewVLog(&vlog.Options{})
	db.stats = newStats(opt)
	go db.lsm.StartMerge()
	go db.vlog.StartGC()
	go db.stats.StartStats()
	return db
}

func (db *DB) Close() error {
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}
	if err := db.stats.close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(key []byte) error {
	return db.Set(&codec.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (db *DB) Set(data *codec.Entry) error {
	var valuePtr *codec.ValuePtr
	if utils.ValueSize(data.Value) > db.opt.ValueThreshold {
		valuePtr = codec.NewValuePtr(data)
		if err := db.vlog.Set(data); err != nil {
			return err
		}
	}
	if valuePtr != nil {
		data.Value = codec.ValuePtrCodec(valuePtr)
	}
	return db.lsm.Set(data)
}

func (db *DB) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	if entry, err = db.lsm.Get(key); err == nil {
		return entry, err
	}
	if entry != nil && codec.IsValuePtr(entry) {
		if entry, err = db.vlog.Get(entry); err == nil {
			return entry, err
		}
	}
	return nil, nil
}

func (db *DB) Info() *Stats {
	return db.stats
}
