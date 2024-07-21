package corekv

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
)

const discardStatsFlushThreshold = 100

// lfDiscardStatsKey 用作脏数据插入 LSM 的 key
var lfDiscardStatsKey = []byte("!kv!discard")

type valueLog struct {
	dirPath          string
	filesLock        sync.RWMutex
	filesMap         map[uint32]*file.LogFile // fid 与 vlog 底层文件的映射
	maxFID           uint32
	filesToBeDeleted []uint32 // 将要删除的 vlog 文件
	numActiveIters   int32    // 迭代器的引用计数，当它为零时可以删除 filesToBeDeleted

	db                *DB // 所属 DB
	opt               Options
	writableLogOffset uint32 // vlog 文件中下一个可写的位置
	numEntriesWritten uint32 // 写入 vlog 的 Entry 数

	garbageCh      chan struct{}   // 用于控制只能同时运行一个 vlog GC 过程
	lfDiscardStats *lfDiscardStats // 用于统计 vlog 中脏 key 数量
}

// newValuePtr 创建一个新的值指针，并将相应 Entry 写入 vlog
func (vl *valueLog) newValuePtr(e *utils.Entry) (*utils.ValuePtr, error) {
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = []*utils.Entry{e}
	req.Wg.Add(1)
	req.IncrRef()
	defer req.DecrRef()
	err := vl.write([]*request{req})
	return req.Ptrs[0], err
}

func (vl *valueLog) open(db *DB, ptr *utils.ValuePtr, replayFn utils.LogEntry) error {
	vl.lfDiscardStats.closer.Add(1)
	// 启动协程，不断将脏数据信息写入 LSM
	go vl.flushDiscardStats()
	// 读取所有 vlog 文件，填充 filesMap
	if err := vl.populateFilesMap(); err != nil {
		return err
	}
	if len(vl.filesMap) == 0 {
		_, err := vl.createVlogFile(0)
		return utils.WarpErr("error while creating log file in valueLog.open", err)
	}

	fids := vl.sortedFids()
	for _, fid := range fids {
		lf, ok := vl.filesMap[fid]
		utils.CondPanic(!ok, fmt.Errorf("vlog.fileMap[fid] fid not found"))
		opt := &file.Options{
			FID:      uint64(fid),
			FileName: vl.fpath(fid),
			Dir:      vl.dirPath,
			Path:     vl.dirPath,
			MaxSz:    2 * vl.db.opt.ValueLogFileSize,
		}
		if err := lf.Open(opt); err != nil {
			return errors.Wrapf(err, "open existing file: %q", lf.FileName())
		}

		var offset uint32
		if fid == ptr.FID {
			offset = ptr.Offset + ptr.Len
		}
		fmt.Printf("replaying file id: %d at offset: %d\n", fid, offset)
		now := time.Now()
		// 重放 vlog 文件
		if err := vl.replayLog(lf, offset, replayFn); err != nil {
			// vlog 文件是损坏的，删除该文件
			if err == utils.ErrDeleteVlogFile {
				delete(vl.filesMap, fid)
				if err := lf.Close(); err != nil {
					return errors.Wrapf(err, "failed to colse vlog file %s", lf.FileName())
				}
				path := vl.fpath(lf.FID)
				if err := os.Remove(path); err != nil {
					return errors.Wrapf(err, "failed to delete empty value log file: %q", path)
				}
				continue
			}
			return err
		}
		fmt.Printf("replay took: %s\n", time.Since(now))

		if fid < vl.maxFID {
			if err := lf.Init(); err != nil {
				return err
			}
		}
	} // for _, fid := range fids

	// 定位到最后一个 vlog 文件，用于后续写入
	last, ok := vl.filesMap[vl.maxFID]
	utils.CondPanic(!ok, errors.New("vlog.filesMap[vlog.maxFid] not found"))
	lastOffset, err := last.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("file.Seek to end path:[%s]", last.FileName()))
	}
	vl.writableLogOffset = uint32(lastOffset)

	vl.db.vhead = &utils.ValuePtr{
		Offset: uint32(lastOffset),
		FID:    vl.maxFID,
	}
	if err := vl.populateDiscardStats(); err != nil {
		fmt.Errorf("failed to populate discard stats: %s\n", err)
	}
	return nil
}

// read 读取 vp 指示的数据，返回读取到的 value
func (vl *valueLog) read(vp *utils.ValuePtr) ([]byte, func(), error) {
	buf, lf, err := vl.readValueBytes(vp)
	cb := vl.getUnlockCallback(lf)
	if err != nil {
		return nil, cb, err
	}

	// 验证校验和
	if vl.opt.VerifyValueChecksum {
		hash := crc32.New(utils.CastagnoliCrcTable)
		if _, err := hash.Write(buf[:len(buf)-crc32.Size]); err != nil {
			utils.RunCallback(cb)
			return nil, nil, errors.Wrapf(err, "failed to write hash for vp %+v", vp)
		}
		checksum := buf[len(buf)-crc32.Size:]
		if hash.Sum32() != utils.BytesToU32(checksum) {
			utils.RunCallback(cb)
			return nil, nil, errors.Wrapf(utils.ErrChecksumMismatch, "value corrupted for vp: %+v", vp)
		}
	}

	var h utils.Header
	headerLen := h.Decode(buf)
	kv := buf[headerLen:]
	if uint32(len(kv)) < h.KLen+h.VLen+crc32.Size {
		fmt.Errorf("invalid read: vp: %+v\n", vp)
		return nil, nil, errors.Errorf("invalid read: len: %d read at:[%d:%d]", len(kv), h.KLen, h.KLen+h.VLen+crc32.Size)
	}
	return kv[h.KLen : h.KLen+h.VLen], cb, nil
}

// write 向 vlog 文件写入 reqs
func (vl *valueLog) write(reqs []*request) error {
	if err := vl.validateWrites(reqs); err != nil {
		return err
	}

	vl.filesLock.RLock()
	maxFID := vl.maxFID
	curLf := vl.filesMap[maxFID]
	vl.filesLock.Unlock()

	var buf bytes.Buffer
	flushWrites := func() error {
		if buf.Len() == 0 {
			return nil
		}
		data := buf.Bytes()
		offset := vl.woffset()
		if err := curLf.Write(offset, data); err != nil {
			return errors.Wrapf(err, "unable to write to value log file: %q", curLf.FileName())
		}
		buf.Reset()
		atomic.AddUint32(&vl.writableLogOffset, offset+uint32(len(data)))
		curLf.AddSize(vl.writableLogOffset)
		return nil
	}

	toDisk := func() error {
		if err := flushWrites(); err != nil {
			return err
		}
		if vl.woffset() > uint32(vl.opt.ValueLogFileSize) || vl.numEntriesWritten > vl.opt.ValueLogMaxEntries {
			if err := curLf.DoneWriting(vl.woffset()); err != nil {
				return err
			}

			newID := atomic.AddUint32(&vl.maxFID, 1)
			newLf, err := vl.createVlogFile(newID)
			if err != nil {
				return err
			}
			curLf = newLf
			atomic.AddInt32(&vl.db.logRotates, 1)
		}
		return nil
	}

	for _, req := range reqs {
		req.Ptrs = req.Ptrs[:0]
		var written int
		// 遍历各个请求内的 Entry
		for _, e := range req.Entries {
			if vl.db.shouldWriteValueToLSM(e) {
				req.Ptrs = append(req.Ptrs, &utils.ValuePtr{})
				continue
			}

			var p utils.ValuePtr
			p.Offset = vl.woffset() + uint32(buf.Len())
			p.FID = curLf.FID
			plen, err := curLf.EncodeEntry(e, &buf, p.Offset)
			if err != nil {
				return err
			}
			p.Len = uint32(plen)
			req.Ptrs = append(req.Ptrs, &p)
			written++

			if buf.Len() > vl.opt.ValueLogFileSize {
				if err := flushWrites(); err != nil {
					return err
				}
			}
		} // for _, e := range req.Entries {

		vl.numEntriesWritten += uint32(written)
		writeNow := vl.woffset()+uint32(buf.Len()) > uint32(vl.opt.ValueLogFileSize) ||
			vl.numEntriesWritten > uint32(vl.opt.ValueLogMaxEntries)
		if writeNow {
			if err := toDisk(); err != nil {
				return err
			}
		}
	} // for _, req := range reqs {
	return toDisk()
}

func (vl *valueLog) close() error {
	if vl == nil || vl.db == nil {
		return nil
	}

	<-vl.lfDiscardStats.closer.CloseSignal
	var err error
	for id, f := range vl.filesMap {
		f.Rw.Lock()
		maxFID := vl.maxFID
		if id == maxFID {
			if truncErr := f.Truncate(int64(vl.woffset())); truncErr != nil && err == nil {
				err = truncErr
			}
			if closeErr := f.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
		f.Rw.Unlock()
	}
	return err
}

// runGC 启动 GC
func (vl *valueLog) runGC(dixcardRatio float64, head *utils.ValuePtr) error {
	select {
	case vl.garbageCh <- struct{}{}:
		defer func() {
			<-vl.garbageCh
		}()

		files := vl.pickLog(head)
		if len(files) == 0 {
			return utils.ErrNoRewrite
		}
		var err error
		tried := make(map[uint32]bool)
		for _, lf := range files {
			// 去重，防止随机策略和统计策略返回同一个 fid
			if _, done := tried[lf.FID]; done {
				continue
			}
			tried[lf.FID] = true
			if err = vl.doRunGC(lf, dixcardRatio); err == nil {
				return nil
			}
		}
		return err
	default:
		return utils.ErrRejected
	} // select {
}

func (vl *valueLog) doRunGC(lf *file.LogFile, discardRatio float64) error {
	var err error
	// 将重写完成的 vlog 的脏 key 信息清空
	defer func() {
		if err == nil {
			vl.lfDiscardStats.Lock()
			delete(vl.lfDiscardStats.table, lf.FID)
			vl.lfDiscardStats.Unlock()
		}
	}()

	s := &sampler{
		lf:            lf,
		sizeRatio:     0.1,
		countRatio:    0.01,
		fromBeginning: false,
	}

	if _, err = vl.sampler(s, discardRatio); err != nil {
		return err
	}

	if err = vl.rewrite(lf); err != nil {
		return err
	}
	return nil
}

func (vl *valueLog) rewrite(lf *file.LogFile) error {
	// wb 保存要批量插入的 Entry
	wb := make([]*utils.Entry, 0, 1000)
	// size 要批量插入的总字节数
	var size int64
	// count 遍历的 Entry 总数
	var count int
	// moved 重新写回的 Entry 数
	var moved int

	fe := func(e *utils.Entry) error {
		count++
		if count%100000 == 0 {
			fmt.Printf("processing entry %d\n", count)
		}

		vs, err := vl.db.lsm.Get(e.Key)
		if err != nil {
			return err
		}
		if utils.DiscardEntry(e, vs) {
			return nil
		}
		if len(vs.Value) == 0 {
			return errors.Errorf("empty value: %+v", vs)
		}

		var vp utils.ValuePtr
		vp.Decode(vs.Value)
		if vp.FID > lf.FID {
			return nil
		}
		if vp.Offset > e.Offset {
			return nil
		}

		// 如果从 vlog 中读取到的 Entry 在 LSM 中存在，并且是同一个，则重新写回
		if vp.FID == lf.FID && vp.Offset == e.Offset {
			moved++
			ne := new(utils.Entry)
			ne.Meta = 0
			ne.ExpiresAt = e.ExpiresAt
			ne.Key = append([]byte{}, e.Key...)
			ne.Value = append([]byte{}, e.Value...)
			es := int64(ne.EstimateSize(vl.db.opt.ValueLogFileSize))

			// 确保 wb 的长度和数量在事务限制内
			if int64(len(wb)+1) >= vl.opt.MaxBatchCount || size+es >= vl.opt.MaxBatchSize {
				if err := vl.db.batchSet(wb); err != nil {
					return err
				}
				size = 0
				wb = wb[:0]
			}
			wb = append(wb, ne)
			size += es
		}
		return nil
	} // fe := func(e *utils.Entry) error {

	_, err := vl.iterate(lf, 0, func(e *utils.Entry, vp *utils.ValuePtr) error {
		return fe(e)
	})
	if err != nil {
		return err
	}

	// 将 wb 内剩余 Entry 批量写入
	batchSize := 1024
	for i := 0; i < len(wb); {
		if batchSize == 0 {
			return utils.ErrNoRewrite
		}
		end := i + batchSize
		if end > len(wb) {
			end = len(wb)
		}
		if err := vl.db.batchSet(wb[i:end]); err != nil {
			// 一次写入太大了， 就减半重试
			if err == utils.ErrTxnTooBig {
				batchSize /= 2
				continue
			}
			return err
		}
		i += batchSize
	}

	// entries 都已写回 LSM 和 vlog，现在可以删除旧的 vlog 文件了
	var deleteFileNow bool
	{
		vl.filesLock.Lock()
		if _, ok := vl.filesMap[lf.FID]; !ok {
			vl.filesLock.Unlock()
			return errors.Errorf("unable to find fid: %d", lf.FID)
		}
		if vl.iteratorCount() == 0 {
			delete(vl.filesMap, lf.FID)
			deleteFileNow = true
		} else {
			vl.filesToBeDeleted = append(vl.filesToBeDeleted, lf.FID)
		}
		vl.filesLock.Unlock()
	}

	if deleteFileNow {
		if err := vl.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

// iteratorCount vlog 上活跃的迭代器数量
func (v *valueLog) iteratorCount() int {
	return int(atomic.LoadInt32(&v.numActiveIters))
}

func (vl *valueLog) decrIteratorCount() error {
	num := atomic.AddInt32(&vl.numActiveIters, -1)
	if num != 0 {
		return nil
	}

	vl.filesLock.Lock()
	lfs := make([]*file.LogFile, 0, len(vl.filesToBeDeleted))
	for _, id := range vl.filesToBeDeleted {
		lfs = append(lfs, vl.filesMap[id])
		delete(vl.filesMap, id)
	}
	vl.filesToBeDeleted = nil
	vl.filesLock.Unlock()

	for _, lf := range lfs {
		if err := vl.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

// deleteLogFile 删除 vlog 底层文件
func (v *valueLog) deleteLogFile(lf *file.LogFile) error {
	if lf == nil {
		return nil
	}
	lf.Rw.Lock()
	defer lf.Rw.Unlock()
	utils.Err(lf.Close())
	return os.Remove(lf.FileName())
}

// validateWrites 检查 reqs 能否写入 vlog 文件
func (vl *valueLog) validateWrites(reqs []*request) error {
	vlOffset := uint64(vl.woffset())
	for _, req := range reqs {
		size := estimateRequestSize(req)
		estimateVlogOffset := vlOffset + size
		if estimateVlogOffset > uint64(utils.MaxVlogFileSize) {
			return errors.Errorf("Request size offset %d is bigger than maximum offset %d", estimateVlogOffset, utils.MaxVlogFileSize)
		}

		if estimateVlogOffset >= uint64(vl.opt.ValueLogFileSize) {
			// 此时创建一个新的 vlog 文件
			vlOffset = 0
			continue
		}
		vlOffset = estimateVlogOffset
	}
	return nil
}

// estimateRequestSize 预估写入给定 requst 的 Entry 列表需要的大小
func estimateRequestSize(req *request) uint64 {
	size := uint64(0)
	for _, e := range req.Entries {
		size += uint64(utils.MaxHeaderSize + len(e.Key) + len(e.Value) + crc32.Size)
	}
	return size
}

// getUnlockCallback 获取 lf.Rw.RUnlock 函数
func (v *valueLog) getUnlockCallback(lf *file.LogFile) func() {
	if lf == nil {
		return nil
	}
	return lf.Rw.RUnlock
}

// readValueBytes 读取 vp 指示的内容，调用者负责调用 LogFile 的 RUnlock()
func (vl *valueLog) readValueBytes(vp *utils.ValuePtr) ([]byte, *file.LogFile, error) {
	lf, err := vl.getFileRLocked(vp)
	if err != nil {
		return nil, nil, err
	}

	buf, err := lf.Read(vp)
	return buf, lf, err
}

// getFileRLocked 获取 vp 所属的 LogFile，并加读锁
func (vl *valueLog) getFileRLocked(vp *utils.ValuePtr) (*file.LogFile, error) {
	vl.filesLock.RLock()
	defer vl.filesLock.Unlock()
	lf, ok := vl.filesMap[vp.FID]
	if !ok {
		return nil, errors.Errorf("file with ID: %d not found", vp.FID)
	}

	maxFID := vl.maxFID
	if vp.FID == maxFID {
		curOffset := vl.woffset()
		if vp.Offset >= curOffset {
			return nil, errors.Errorf("invalid value ptr offset: %d greater than current offset: %d", vp.Offset, curOffset)
		}
	}

	lf.Rw.RLock()
	return lf, nil
}

// woffset 获取可写位置
func (v *valueLog) woffset() uint32 {
	return atomic.LoadUint32(&v.writableLogOffset)
}

// populateFilesMap 填充 fid 与 vlog 底层文件的映射
func (vl *valueLog) populateFilesMap() error {
	vl.filesMap = make(map[uint32]*file.LogFile)

	files, err := ioutil.ReadDir(vl.dirPath)
	if err != nil {
		return utils.WarpErr(fmt.Sprintf("unable to open log dir. path[%s]", vl.dirPath), err)
	}

	found := make(map[uint64]struct{})
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".vlog") {
			continue
		}
		fsz := len(f.Name())
		fid, err := strconv.ParseUint(f.Name()[:fsz-5], 10, 32)
		if err != nil {
			return utils.WarpErr(fmt.Sprintf("unable to parse logid. name[%s]", f.Name()), err)
		}
		if _, ok := found[fid]; ok {
			return utils.WarpErr(fmt.Sprintf("duplicate file found. Please delete one. name:[%s]", f.Name()), err)
		}

		lf := &file.LogFile{
			Rw:  sync.RWMutex{},
			FID: uint32(fid),
		}
		vl.filesMap[uint32(fid)] = lf
		if vl.maxFID < uint32(fid) {
			vl.maxFID = uint32(fid)
		}
	}
	return nil
}

// createVlogFile 创建 vlog 文件，并初始化相应结构体
func (vl *valueLog) createVlogFile(fid uint32) (*file.LogFile, error) {
	lf := &file.LogFile{
		Rw:  sync.RWMutex{},
		FID: fid,
	}
	utils.Panic2(nil, lf.Open(&file.Options{
		FID:      uint64(fid),
		FileName: vl.fpath(fid),
		Dir:      vl.dirPath,
		Path:     vl.dirPath,
		MaxSz:    2 * vl.db.opt.ValueLogFileSize,
	}))

	removeFile := func() {
		utils.Err(os.Remove(lf.FileName()))
	}

	var err error
	if err = lf.Bootstrap(); err != nil {
		removeFile()
		return nil, err
	}

	if err = utils.SyncDir(vl.dirPath); err != nil {
		removeFile()
		return nil, utils.WarpErr(fmt.Sprintf("sync value log dir[%s]", vl.dirPath), err)
	}

	vl.filesLock.Lock()
	defer vl.filesLock.Unlock()
	vl.filesMap[fid] = lf
	vl.maxFID = fid
	atomic.StoreUint32(&vl.writableLogOffset, utils.VlogHeaderSize)
	vl.numEntriesWritten = 0
	return lf, nil
}

// sortedFids 返回不准备删除并且已排升序的 vlog 列表
func (vl *valueLog) sortedFids() []uint32 {
	toBeDeleted := make(map[uint32]struct{})
	for _, fid := range vl.filesToBeDeleted {
		toBeDeleted[fid] = struct{}{}
	}
	out := make([]uint32, 0, len(vl.filesMap))
	for fid := range vl.filesMap {
		if _, ok := toBeDeleted[fid]; !ok {
			out = append(out, fid)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

// replay 重放 vlog 文件
func (vl *valueLog) replayLog(lf *file.LogFile, offset uint32, replayFn utils.LogEntry) error {
	endOffset, err := vl.iterate(lf, offset, replayFn)
	if err != nil {
		return errors.Wrapf(err, "unable to replay logfile:[%s]", lf.FileName())
	}
	if int64(endOffset) == int64(lf.Size()) {
		return nil
	}

	if endOffset <= utils.VlogHeaderSize {
		if lf.FID != vl.maxFID {
			return utils.ErrDeleteVlogFile
		}
		return lf.Bootstrap()
	}

	fmt.Printf("truncating vlog file %s to offset: %d\n", lf.FileName(), endOffset)
	if err := lf.Truncate(int64(endOffset)); err != nil {
		return utils.WarpErr(fmt.Sprintf("truncation needed at offset %d", endOffset), err)
	}
	return nil
}

// iterate 遍历 vlog 文件，对读取到的 Entry 执行 fn 函数
func (vl *valueLog) iterate(lf *file.LogFile, offset uint32, fn utils.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = utils.VlogHeaderSize
	}
	if int64(offset) == int64(lf.Size()) {
		return offset, nil
	}
	if _, err := lf.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, errors.Wrapf(err, "unable to seek, name:%s", lf.FileName())
	}

	reader := bufio.NewReader(lf.FD())
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
		lf:           lf,
	}
	validEnfOffset := offset

loop:
	for {
		e, err := read.Entry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e == nil:
			continue
		}

		var vp utils.ValuePtr
		vp.Len = uint32(int(e.Hlen) + len(e.Key) + len(e.Value) + crc32.Size)
		read.recordOffset += vp.Len

		vp.Offset = e.Offset
		vp.FID = lf.FID
		validEnfOffset = read.recordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, utils.WarpErr(fmt.Sprintf("iterator function %s", lf.FileName()), err)
		}
	} // for {
	return validEnfOffset, nil
}

type safeRead struct {
	k            []byte
	v            []byte
	recordOffset uint32
	lf           *file.LogFile
}

// Entry 从 reader 中读取一个 Entry
func (sr *safeRead) Entry(reader io.Reader) (*utils.Entry, error) {
	tee := utils.NewHashReader(reader)
	var h utils.Header
	// 读取 header
	hlen, err := h.DecodeFrom(tee)
	if err != nil {
		return nil, err
	}
	if h.KLen > uint32(1<<16) {
		return nil, utils.ErrTruncate
	}
	klen := int(h.KLen)
	if cap(sr.k) < klen {
		sr.k = make([]byte, 2*klen)
	}
	vlen := int(h.VLen)
	if cap(sr.v) < vlen {
		sr.v = make([]byte, 2*vlen)
	}

	// 读取 KV 数据
	e := &utils.Entry{}
	e.Offset = sr.recordOffset
	e.Hlen = hlen
	buf := make([]byte, klen+vlen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}

	e.Key = buf[:klen]
	e.Value = buf[klen : klen+vlen]

	// 读取校验和并验证
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.ExpiresAt = h.ExpiresAt
	e.Meta = h.Meta
	return e, nil
}

func (vl *valueLog) populateDiscardStats() error {
	key := utils.KeyWithTs(lfDiscardStatsKey, math.MaxUint64)
	var statsMap map[uint32]int64
	vs, err := vl.db.Get(key)
	if err != nil {
		return err
	}
	if vs.Meta == 0 && len(vs.Value) == 0 {
		return nil
	}
	val := vs.Value

	if utils.IsValuePtr(vs) {
		var vp utils.ValuePtr
		vp.Decode(val)
		res, cb, err := vl.read(&vp)
		val = utils.SafeCopy(nil, res)
		utils.RunCallback(cb)
		if err != nil {
			return err
		}
	}
	if len(val) == 0 {
		return nil
	}

	if err := json.Unmarshal(val, &statsMap); err != nil {
		return errors.Wrapf(err, "failed to unmarshal discard stats")
	}
	vl.lfDiscardStats.flushCh <- statsMap
	return nil
}

// fpath 生成 vlog 文件名
func (v *valueLog) fpath(fid uint32) string {
	return utils.VlogFilePath(v.dirPath, fid)
}

func (db *DB) initVLog() {
	vp, _ := db.getHead()
	vlog := &valueLog{
		dirPath:          db.opt.WorkDir,
		filesToBeDeleted: make([]uint32, 0),
		lfDiscardStats: &lfDiscardStats{
			table:   make(map[uint32]int64),
			closer:  utils.NewCloser(),
			flushCh: make(chan map[uint32]int64, 16),
		},
	}
	vlog.db = db
	vlog.opt = *db.opt
	vlog.garbageCh = make(chan struct{}, 1)
	if err := vlog.open(db, vp, db.replayFunction()); err != nil {
		utils.Panic(err)
	}
	db.vlog = vlog
}

func (db *DB) getHead() (*utils.ValuePtr, uint64) {
	var vptr utils.ValuePtr
	return &vptr, 0
}

// replayFunction 获取一个用于重放 vlog 的函数，即将 Entry 插入到 LSM
func (db *DB) replayFunction() func(*utils.Entry, *utils.ValuePtr) error {
	// toLSM 向 LSM 插入数据
	toLSM := func(k []byte, vs utils.ValueStruct) {
		db.lsm.Set(&utils.Entry{
			Key:       k,
			Value:     vs.Value,
			ExpiresAt: vs.ExpiresAt,
			Meta:      vs.Meta,
		})
	}

	return func(e *utils.Entry, vp *utils.ValuePtr) error {
		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.Meta
		if db.shouldWriteValueToLSM(e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = vp.Encode()
			meta |= utils.BitValuePointer
		}

		db.updateHead([]*utils.ValuePtr{vp})

		v := utils.ValueStruct{
			Meta:      meta,
			Value:     nv,
			ExpiresAt: e.ExpiresAt,
		}
		toLSM(nk, v)
		return nil
	}
}

func (db *DB) updateHead(ptrs []*utils.ValuePtr) {
	var ptr *utils.ValuePtr
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			break
		}
	}
	if ptr.IsZero() {
		return
	}

	utils.CondPanic(ptr.Less(db.vhead), fmt.Errorf("ptr.Less(db.vhead) if true"))
	db.vhead = ptr
}

func (vl *valueLog) sync(fid uint32) error {
	vl.filesLock.RLock()
	maxFid := vl.maxFID
	if fid < maxFid || len(vl.filesMap) == 0 {
		vl.filesLock.RUnlock()
		return nil
	}
	curlf := vl.filesMap[maxFid]
	if curlf == nil {
		vl.filesLock.RUnlock()
		return nil
	}
	curlf.Rw.RLock()
	vl.filesLock.RUnlock()

	err := curlf.Sync()
	curlf.Rw.RUnlock()
	return err
}

func (v *valueLog) set(entry *utils.Entry) error {
	return nil
}

func (v *valueLog) get(entry *utils.Entry) (*utils.Entry, error) {
	return nil, nil
}

// lfDiscardStats 记录 vlog 中可以丢弃的数据量
type lfDiscardStats struct {
	sync.RWMutex
	table             map[uint32]int64      // fid 与脏 key 数量映射
	flushCh           chan map[uint32]int64 // 用于更新脏 key 数量
	closer            *utils.Closer
	updatesSinceFlust int // 自上次 flush 后 vlog 脏 key 更新次数
}

// flushDiscardStats 将脏数据 map 写入 LSM
func (vl *valueLog) flushDiscardStats() {
	defer vl.lfDiscardStats.closer.Done()

	// 将 stats 中数据合并到 vl.lfDiscardStats.table
	mergeStats := func(stats map[uint32]int64) ([]byte, error) {
		vl.lfDiscardStats.Lock()
		defer vl.lfDiscardStats.Unlock()
		for fid, cnt := range stats {
			vl.lfDiscardStats.table[fid] += cnt
			vl.lfDiscardStats.updatesSinceFlust++
		}

		// 达到规定的更新次数，将数据序列化
		if vl.lfDiscardStats.updatesSinceFlust > discardStatsFlushThreshold {
			encodedDS, err := json.Marshal(vl.lfDiscardStats.table)
			if err != nil {
				return nil, err
			}
			vl.lfDiscardStats.updatesSinceFlust = 0
			return encodedDS, nil
		}
		return nil, nil
	}

	process := func(stats map[uint32]int64) error {
		encodedDS, err := mergeStats(stats)
		if err != nil || encodedDS == nil {
			return err
		}

		entries := []*utils.Entry{
			{
				Key:   utils.KeyWithTs(lfDiscardStatsKey, 1),
				Value: encodedDS,
			},
		}
		// 将 entries 插入到 LSM
		req, err := vl.db.sendToWriteCh(entries)
		if err != nil {
			return errors.Wrapf(err, "faild to push discard stats to write channel")
		}
		return req.Wait()
	}

	closer := vl.lfDiscardStats.closer
	for {
		select {
		case <-closer.CloseSignal:
			return
		case stats := <-vl.lfDiscardStats.flushCh:
			if err := process(stats); err != nil {
				utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
		}
	}
}

// requestPool 请求池
var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

// request 请求
type request struct {
	Entries []*utils.Entry    // 请求的 Entry 列表
	Ptrs    []*utils.ValuePtr // 请求的 Entry 对应的值指针，Entry 直接写入 LSM 时为空
	Wg      sync.WaitGroup
	Err     error
	ref     int32 // 引用计数
}

// reset 重置 request 对象
func (r *request) reset() {
	r.Entries = r.Entries[:0]
	r.Ptrs = r.Ptrs[:0]
	r.Wg = sync.WaitGroup{}
	r.Err = nil
	r.ref = 0
}

// pickLog 选择需要 GC 的 vlog 文件
func (vl *valueLog) pickLog(head *utils.ValuePtr) []*file.LogFile {
	vl.filesLock.RLock()
	defer vl.filesLock.RUnlock()

	fids := vl.sortedFids()
	switch {
	// 只有一个 vlog 文件不需要 GC
	case len(fids) <= 1:
		return nil
	}

	// 创建一个候选对象
	candidate := struct {
		fid     uint32
		discard int64
	}{math.MaxUint32, 0}
	// 找 discard 最大的 vlog 文件
	vl.lfDiscardStats.RLock()
	for _, fid := range fids {
		if fid >= head.FID {
			break
		}
		if vl.lfDiscardStats.table[fid] > candidate.discard {
			candidate.fid = fid
			candidate.discard = vl.lfDiscardStats.table[fid]
		}
	}
	vl.lfDiscardStats.RUnlock()

	var files []*file.LogFile
	if candidate.fid != math.MaxUint32 {
		files = append(files, vl.filesMap[candidate.fid])
	}

	// 随机选择一个 fid
	var idxHead int
	for i, fid := range fids {
		if fid == head.FID {
			idxHead = i
			break
		}
	}
	if idxHead == 0 {
		idxHead = 1
	}
	idx := rand.Intn(idxHead)
	if idx > 0 {
		idx = rand.Intn(idx + 1)
	}
	files = append(files, vl.filesMap[fids[idx]])
	return files
}

// sampler 采样器
type sampler struct {
	lf            *file.LogFile
	sizeRatio     float64 // 大小比例
	countRatio    float64 // 数量比例
	fromBeginning bool    // 是否从头开始采样
}

// sampler 采样，看是否需要对 vlog 执行 GC
func (vl *valueLog) sampler(samp *sampler, discardRatio float64) (*reason, error) {
	sizePercent := samp.sizeRatio
	countPercent := samp.countRatio
	fileSize := samp.lf.Size()
	// 设置采样窗口大小
	sizeWindow := float64(fileSize) * sizePercent
	sizeWindowMB := sizeWindow / (1 << 20)
	countWindow := int(float64(vl.opt.ValueLogMaxEntries) * countPercent)

	// 随机选择一个采样起点
	var skipFirstMB float64
	if !samp.fromBeginning {
		for skipFirstMB < sizeWindow {
			skipFirstMB = float64(rand.Int63n(fileSize))
		}
		skipFirstMB -= sizeWindow
		skipFirstMB /= float64(utils.Mi)
	}

	var err error
	var skipped float64
	var r reason
	start := time.Now()
	var numIters int
	_, err = vl.iterate(samp.lf, 0, func(e *utils.Entry, vp *utils.ValuePtr) error {
		numIters++
		esz := float64(vp.Len) / (1 << 20)
		if skipped < skipFirstMB {
			skipped += esz
			return nil
		}

		if r.count > countWindow {
			return utils.ErrStop
		}
		if r.total > sizeWindowMB {
			return utils.ErrStop
		}
		if time.Since(start) > 10*time.Second {
			return utils.ErrStop
		}
		r.total += esz
		r.count++

		// 去 LSM 中查 Entry，看哪些已删除
		entry, err := vl.db.Get(e.Key)
		if err != nil {
			return err
		}
		if utils.DiscardEntry(e, entry) {
			r.discard += esz
			return nil
		}

		vp.Decode(entry.Value)
		// 说明是后续更新的，当前的可以丢弃了
		if vp.FID > samp.lf.FID {
			r.discard += esz
			return nil
		}
		if vp.Offset > e.Offset {
			r.discard += esz
			return nil
		}
		return nil
	}) // _, err = vl.iterate(samp.lf, 0, func(e *utils.Entry, vp *utils.ValuePtr) error {
	if err != nil {
		return nil, err
	}

	if (r.count < countWindow && r.total < sizeWindowMB*0.75) || r.discard < discardRatio*r.total {
		return nil, utils.ErrNoRewrite
	}
	return &r, nil
}

func (v *valueLog) waitOnGC(closer *utils.Closer) {
	defer closer.Done()

	<-closer.CloseSignal

	v.garbageCh <- struct{}{}
}

// reason 用于在采样中判断是否需要 GC
type reason struct {
	total   float64 // 采样总大小
	discard float64 // 丢弃数据
	count   int     // Entry 个数
}
