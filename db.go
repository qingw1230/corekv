package corekv

import (
	"expvar"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/lsm"
	"github.com/qingw1230/corekv/utils"
)

type (
	CoreAPI interface {
		Set(data *utils.Entry) error
		Get(key []byte) (*utils.Entry, error)
		Del(key []byte) error
		NewIterator(opt *utils.Options) utils.Iterator
		Info() *Stats
		Close() error
	}

	DB struct {
		sync.RWMutex
		opt         *Options
		lsm         *lsm.LSM  // LSM 树
		vlog        *valueLog // 管理 vlog 文件
		stats       *Stats
		flushCh     chan flushTask // 用于 flush 内存表
		writeCh     chan *request  // 向写入 LSM 请求
		blockWrites int32
		vhead       *utils.ValuePtr // 起到 check point 作用
		logRotates  int32
	}
)

var (
	// head 用于存储重放的 value offset
	head = []byte("!kv!head")
)

/**
SSTableMaxSz:        1024,
MemTableSize:        1024,
BlockSize:           1024,
BloomFalsePositive:  0,
BaseLevelSize:       10 << 20,
LevelSizeMultiplier: 10,
BaseTableSize:       2 << 20,
TableSizeMultiplier: 2,
NumLevelZeroTables:  15,
MaxLevelNum:         7,
NumCompactors:       3,
*/

func Open(opt *Options) *DB {
	c := utils.NewCloser()
	db := &DB{opt: opt}
	db.initVLog()
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSz:        opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0, //0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
		DiscardStatsCh:      &(db.vlog.lfDiscardStats.flushCh),
	})
	db.stats = newStats(opt)
	go db.lsm.StartCompacter()
	c.Add(1)
	db.writeCh = make(chan *request)
	db.flushCh = make(chan flushTask, 16)
	go db.doWrites(c)
	go db.stats.StartStats()
	return db
}

func (db *DB) Close() error {
	db.vlog.lfDiscardStats.closer.Close()
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.close(); err != nil {
		return err
	}
	if err := db.stats.close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(key []byte) error {
	// 写入一个值为 nil 的 Entry 作为墓碑消息实现删除
	return db.Set(&utils.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (db *DB) Set(e *utils.Entry) error {
	if e == nil || len(e.Key) == 0 {
		return utils.ErrEmptyKey
	}
	var (
		vp  *utils.ValuePtr
		err error
	)
	e.Key = utils.KeyWithTs(e.Key, math.MaxUint32)
	// 如果 value 比较大，写入 LSM 的 value 实际是指示位置的值指针
	if !db.shouldWriteValueToLSM(e) {
		if vp, err = db.vlog.newValuePtr(e); err != nil {
			return err
		}
		e.Meta |= utils.BitValuePointer
		e.Value = vp.Encode()
	}
	return db.lsm.Set(e)
}

func (db *DB) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	var (
		e   *utils.Entry
		err error
	)
	// 先从 LSM 中查询 Entry
	if e, err = db.lsm.Get(key); err != nil {
		return e, err
	}
	// 如果是值指针再去 vlog 文件获取值
	if e != nil && utils.IsValuePtr(e) {
		var vp utils.ValuePtr
		vp.Decode(e.Value)
		res, cb, err := db.vlog.read(&vp)
		defer utils.RunCallback(cb)
		if err != nil {
			return nil, err
		}
		e.Value = utils.SafeCopy(nil, res)
	}
	if isDeletedOrExpired(e) {
		return nil, utils.ErrKeyNotFound
	}
	return e, nil
}

func isDeletedOrExpired(e *utils.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func (db *DB) Info() *Stats {
	return db.stats
}

// RunValueLogGC 启动 vlog GC
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}

	headKey := utils.KeyWithTs(head, math.MaxUint64)
	val, err := db.lsm.Get(headKey)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			val = &utils.Entry{
				Key:   headKey,
				Value: []byte{},
			}
		} else {
			return errors.Wrap(err, "retrieving head from on-disk LSM")
		}
	}

	var head utils.ValuePtr
	head.FID = db.vlog.maxFID
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	return db.vlog.runGC(discardRatio, &head)
}

// shouldWriteValueToLSM 是否将值直接写入 LSM
func (db *DB) shouldWriteValueToLSM(e *utils.Entry) bool {
	return int64(len(e.Value)) < db.opt.ValueThreshold
}

// sendToWriteCh 将 entries 组装成 req 发送到 db.writeCh
func (db *DB) sendToWriteCh(entries []*utils.Entry) (*request, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return nil, utils.ErrBlockedWrites
	}

	var cnt, size int64
	for _, e := range entries {
		cnt++
		size += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
	}
	if cnt >= db.opt.MaxBatchCount || size >= db.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}

	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()
	db.writeCh <- req
	return req, nil
}

func (db *DB) batchSet(entries []*utils.Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	return req.Wait()
}

func (db *DB) doWrites(closer *utils.Closer) {
	defer closer.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			utils.Err(fmt.Errorf("writeRequests: %v", err))
		}
		<-pendingCh
	}

	reqLen := new(expvar.Int)
	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-db.writeCh:
		case <-closer.CloseSignal:
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*utils.KVWriteChCapacity {
				pendingCh <- struct{}{}
				goto writeCase
			}

			select {
			case r = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-closer.CloseSignal:
				goto closedCase
			}
		}

	closedCase:
		// 所有待处理的请求处理完毕，从函数退出
		for {
			select {
			case r = <-db.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{}
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	} // for {

}

// writeRequests 将 reqs 中的 Entry 写入 vlog 和 LSM
// 不能由多个 goroutine 同时调用
func (db *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}

	err := db.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}
	var count int
	for _, req := range reqs {
		if len(req.Entries) == 0 {
			continue
		}
		count += len(req.Entries)
		if err := db.writeToLSM(req); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		db.Lock()
		db.updateHead(req.Ptrs)
		db.Unlock()
	}
	done(nil)
	return nil
}

// writeToLSM 将 req 中的 Entry 插入 LSM
func (db *DB) writeToLSM(req *request) error {
	if len(req.Ptrs) != len(req.Entries) {
		return errors.Errorf("ptrs and entries don't match: %+v", req)
	}

	for i, entry := range req.Entries {
		if db.shouldWriteValueToLSM(entry) {
			entry.Meta = entry.Meta &^ utils.BitValuePointer
		} else {
			entry.Meta |= utils.BitValuePointer
			entry.Value = req.Ptrs[i].Encode()
		}
		db.lsm.Set(entry)
	}
	return nil
}

func (r *request) IncrRef() {
	atomic.AddInt32(&r.ref, 1)
}

func (r *request) DecrRef() {
	nRef := atomic.AddInt32(&r.ref, -1)
	if nRef > 0 {
		return
	}
	r.Entries = nil
	// 当前请求对象无人用了，再将其放回请求池
	requestPool.Put(r)
}

func (r *request) Wait() error {
	r.Wg.Wait()
	err := r.Err
	r.DecrRef()
	return err
}

type flushTask struct {
	mt           *utils.SkipList
	vptr         *utils.ValuePtr
	dropPrefixes [][]byte
}

func (db *DB) pushHead(ft flushTask) error {
	// Ensure we never push a zero valued head pointer.
	if ft.vptr.IsZero() {
		return errors.New("Head should not be zero")
	}

	fmt.Printf("Storing value log head: %+v\n", ft.vptr)
	val := ft.vptr.Encode()

	// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
	// commits.
	headTs := utils.KeyWithTs(head, uint64(time.Now().Unix()/1e9))
	ft.mt.Add(&utils.Entry{
		Key:   headTs,
		Value: val,
	})
	return nil
}
