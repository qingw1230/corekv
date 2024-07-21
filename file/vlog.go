package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/utils"
)

// LogFile vlog 日志底层文件
type LogFile struct {
	Rw   sync.RWMutex
	FID  uint32
	size uint32 // vlog 大小
	f    *MmapFile
}

// Open 打开 vlog 底层文件
func (lf *LogFile) Open(opt *Options) error {
	var err error
	lf.Rw = sync.RWMutex{}
	lf.FID = uint32(opt.FID)
	lf.f, err = OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Panic2(nil, err)
	fi, err := lf.f.Fd.Stat()
	if err != nil {
		return utils.WarpErr("Unable to run file.Stat", err)
	}
	sz := fi.Size()
	lf.size = uint32(sz)
	return nil
}

// Read 读取 ValuePtr 指示的内容
func (lf *LogFile) Read(p *utils.ValuePtr) ([]byte, error) {
	valSz := p.Len
	valOffset := p.Offset
	size := int64(len(lf.f.Data))
	lfSz := atomic.LoadUint32(&lf.size)
	if int64(valOffset) >= size || int64(valOffset+valSz) > size || int64(valOffset+valSz) > int64(lfSz) {
		return nil, io.EOF
	} else {
		buf, err := lf.f.Bytes(int(valOffset), int(valSz))
		return buf, err
	}
}

// DoneWriting 调用 sync 同步
func (lf *LogFile) DoneWriting(offset uint32) error {
	if err := lf.f.Sync(); err != nil {
		return errors.Wrapf(err, "unable to sync value log: %q", lf.FileName())
	}

	lf.Rw.Lock()
	defer lf.Rw.Unlock()

	if err := lf.f.Truncature(int64(offset)); err != nil {
		return errors.Wrapf(err, "unable to truncate file: %q", lf.FileName())
	}
	if err := lf.Init(); err != nil {
		return errors.Wrapf(err, "failed to initialize file %s", lf.FileName())
	}
	return nil
}

// Write 将 buf 写在 offset 偏移处
func (l *LogFile) Write(offset uint32, buf []byte) (err error) {
	return l.f.AppendBuffer(offset, buf)
}

func (l *LogFile) Truncate(offset int64) error {
	return l.f.Truncature(offset)
}

func (l *LogFile) Close() error {
	return l.f.Close()
}

func (l *LogFile) Size() int64 {
	return int64(atomic.LoadUint32(&l.size))
}

// AddSize 将 vlog 大小设置为 offset
func (l *LogFile) AddSize(offset uint32) {
	atomic.StoreUint32(&l.size, offset)
}

func (l *LogFile) Bootstrap() error {
	return nil
}

// Init 初始化 LogFile size 字段
func (lf *LogFile) Init() error {
	fstat, err := lf.f.Fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "unable to check stat for %q", lf.FileName())
	}
	sz := fstat.Size()
	if sz == 0 {
		return nil
	}
	utils.CondPanic(sz > math.MaxUint32, fmt.Errorf("[LogFile.Init] sz > math.MaxUint32"))
	lf.size = uint32(sz)
	return nil
}

func (l *LogFile) FileName() string {
	return l.f.Fd.Name()
}

func (l *LogFile) Seek(offset int64, whence int) (int64, error) {
	return l.f.Fd.Seek(offset, whence)
}

func (l *LogFile) FD() *os.File {
	return l.f.Fd
}

func (l *LogFile) Sync() error {
	return l.f.Sync()
}

// EncodeEntry 将 Entry 编码到 buf
// vlog 格式 | header | key | value | crc32 |
// header 格式 | Meta | KLen | VLen | ExpiresAt |
func (lf *LogFile) EncodeEntry(e *utils.Entry, buf *bytes.Buffer, offset uint32) (int, error) {
	h := utils.Header{
		KLen:      uint32(len(e.Key)),
		VLen:      uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
		Meta:      e.Meta,
	}

	hash := crc32.New(utils.CastagnoliCrcTable)
	// 同时写入 hash 以便计算校验和
	writer := io.MultiWriter(buf, hash)

	var headerEnc [utils.MaxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	utils.Panic2(writer.Write(headerEnc[:sz]))
	utils.Panic2(writer.Write(e.Key))
	utils.Panic2(writer.Write(e.Value))
	// 写入 crc32 校验和
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	utils.Panic2(buf.Write(crcBuf[:]))
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

// DecodeEntry 从 buf 中解码从 vlog Entry
func (lf *LogFile) DecodeEntry(buf []byte, offset uint32) (*utils.Entry, error) {
	var h utils.Header
	hlen := h.Decode(buf)
	kv := buf[hlen:]
	e := &utils.Entry{
		Key:       kv[:h.KLen],
		Value:     kv[h.KLen : h.KLen+h.VLen],
		ExpiresAt: h.ExpiresAt,

		Meta:   h.Meta,
		Offset: offset,
	}
	return e, nil
}
