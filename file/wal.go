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

// WalFile 预写日志文件结构
type WalFile struct {
	opt     *Options
	rw      *sync.RWMutex
	f       *MmapFile
	buf     *bytes.Buffer
	size    uint32 // 底层文件大小
	writeAt uint32 // 写入位置
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

func (wf *WalFile) Name() string {
	return wf.f.Fd.Name()
}

func (wf *WalFile) Size() uint32 {
	return wf.writeAt
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

// Write 向预写日志中添加数据
func (wf *WalFile) Write(e *utils.Entry) error {
	// TODO(qingw1230): 怎么确保 wal 文件同步到磁盘
	wf.rw.Lock()
	defer wf.rw.Unlock()
	len := utils.WalCodec(wf.buf, e)
	data := wf.buf.Bytes()
	utils.Panic(wf.f.AppendBuffer(wf.writeAt, data))
	wf.writeAt += uint32(len)
	return nil
}

// Iterate 遍历 wal 文件，在获取到的 entry 上执行 fn 函数
func (wf *WalFile) Iterate(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	reader := bufio.NewReader(wf.f.NewReader(int(offset)))
	read := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
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
		size := uint32(int(e.LogHeaderLen()) + len(e.Key) + len(e.Value) + crc32.Size)
		read.RecordOffset += size
		validEndOffset = read.RecordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "iterate function")
		}
	} // for {
	return validEndOffset, nil
}

// Truncate 将文件大小截断为 end
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
	return wf.f.Truncature(end)
}

type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	WF           *WalFile
}

// MakeEntry 从 reader 中读取数据构建 Entry
func (sr *SafeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	hr := utils.NewHashReader(reader)
	var head utils.WalHeader
	hlen, err := head.Decode(hr)
	if err != nil {
		return nil, err
	}
	klen := int(head.KeyLen)
	if cap(sr.K) < klen {
		sr.K = make([]byte, 2*klen)
	}
	vlen := int(head.ValueLen)
	if cap(sr.V) < vlen {
		sr.V = make([]byte, 2*vlen)
	}

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
	// 检验校验和
	if crc != hr.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.ExpiresAt = head.ExpiresAt
	return e, nil
}
