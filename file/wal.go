package file

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/utils"
)

// WalFile 预写日志文件
type WalFile struct {
	opt     *Options
	rw      *sync.RWMutex
	f       *MmapFile
	buf     *bytes.Buffer
	size    uint32 // 底层文件大小
	writeAt uint32 // 写入位置
}

func OpenWalFile(opt *Options) *WalFile {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	wf := &WalFile{
		opt: opt,
		rw:  &sync.RWMutex{},
		f:   mf,
		buf: &bytes.Buffer{},
	}
	wf.size = uint32(len(wf.f.Data))
	utils.Err(err)
	return wf
}

// Close 关闭 wal 文件，并将其删除
func (wf *WalFile) Close() error {
	fileName := wf.f.Fd.Name()
	if err := wf.f.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
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

func (wf *WalFile) Write(e *utils.Entry) error {
	wf.rw.Lock()
	defer wf.rw.Unlock()
	len := utils.WalCodec(wf.buf, e)
	// 先写入 buf，再写进 wal 文件，保证数据完整性
	data := wf.buf.Bytes()
	utils.Panic(wf.f.AppendBuffer(wf.writeAt, data))
	wf.writeAt += uint32(len)
	return nil
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
