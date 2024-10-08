package file

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/utils"
)

// WalFile 预写日志文件
type WalFile struct {
	opt     *Options
	f       *MmapFile
	buf     *bytes.Buffer
	size    uint32 // 底层文件大小
	writeAt uint32 // 写入位置

	rw           *sync.RWMutex     // 用于保证写入 chan 与协程 seq 的一致性
	entriesBuf   chan *utils.Entry // 用于批量写入 WAL
	seq          uint32            // 数据写入 chan 后返回的序号
	writeDoneSeq uint32            // 已经将数据写入 WAL 的序号
	mu           *sync.Mutex
	cond         *sync.Cond
	// processNext  uint32
}

func OpenWalFile(opt *Options) *WalFile {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	wf := &WalFile{
		opt:          opt,
		f:            mf,
		buf:          &bytes.Buffer{},
		writeAt:      0,
		rw:           &sync.RWMutex{},
		entriesBuf:   make(chan *utils.Entry, 512),
		seq:          0,
		writeDoneSeq: 0,
		mu:           &sync.Mutex{},
	}
	wf.size = uint32(len(wf.f.Data))
	wf.cond = sync.NewCond(wf.mu)
	utils.Err(err)

	go func() {
		wf.batchWrite()
	}()

	return wf
}

func (wf *WalFile) Wait() {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	wf.cond.Wait()
}

// Close 关闭 wal 文件，并将其删除
func (wf *WalFile) Close() error {
	fileName := wf.f.Fd.Name()
	if err := wf.f.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

func (wf *WalFile) WriteDoneSeq() uint32 {
	return wf.writeDoneSeq
}

func (w *WalFile) FID() uint64 {
	return w.opt.FID
}

func (w *WalFile) Name() string {
	return w.opt.FileName
}

func (w *WalFile) Size() uint32 {
	return w.writeAt
}

func (wf *WalFile) Write(e *utils.Entry) (uint32, error) {
	wf.rw.Lock()
	defer wf.rw.Unlock()

	wf.entriesBuf <- e
	// 必须加锁保护，不能使用原子指令，否则 channel 与 seq 可能出现一致性问题
	wf.seq++
	return wf.seq, nil
}

// batchWrite 只有打开 wal 文件时启动的协程可以访问
func (wf *WalFile) batchWrite() {
	for e := range wf.entriesBuf {
		len := utils.WalCodec(wf.buf, e)
		// 先写入 buf，再写进 wal 文件，保证数据完整性
		data := wf.buf.Bytes()
		utils.Panic(wf.f.AppendBuffer(wf.writeAt, data))
		wf.writeAt += uint32(len)
		atomic.AddUint32(&wf.writeDoneSeq, 1)
		wf.cond.Broadcast()
	}
}

// Iterator 迭代 wal 文件，对文件中每个 Entry 执行 fn 函数
func (wf *WalFile) Iterator(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	reader := bufio.NewReader(wf.f.NewReader(int(offset)))
	read := SafeRead{
		RecordOffset: offset,
		WF:           wf,
	}
	validEndOffset := offset

loop:
	for {
		e, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e.IsZero():
			break loop
		}

		var vp utils.ValuePtr
		sz := uint32(int(e.LogHeaderLen()) + len(e.Key) + len(e.Value) + crc32.Size)
		read.RecordOffset += sz
		validEndOffset = read.RecordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "iterator function")
		}
	} // for {
	return validEndOffset, nil
}

func (wf *WalFile) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}

	if fi, err := wf.f.Fd.Stat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v", wf.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	wf.size = uint32(end)
	return wf.f.Truncate(end)
}

type SafeRead struct {
	RecordOffset uint32
	WF           *WalFile
}

// MakeEntry 从 reader 中读取数据构建 Entry，数据不够、出错时返回 utils.ErrTruncate
func (sr *SafeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	hr := utils.NewHashReader(reader)
	var head utils.WalHeader
	hlen, err := head.Decode(hr)
	if err != nil {
		return nil, err
	}
	klen := int(head.KeyLen)
	vlen := int(head.ValueLen)

	e := &utils.Entry{}
	e.Offset = sr.RecordOffset
	e.Hlen = hlen
	buf := make([]byte, klen+vlen)
	if _, err := io.ReadFull(hr, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	e.Key = buf[:klen]
	e.Value = buf[klen:]

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != hr.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.ExpiresAt = head.ExpiresAt
	return e, nil
}
