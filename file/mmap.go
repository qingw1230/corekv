package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/utils/mmap"
)

// MmapFile 由 mmap 映射出的文件
type MmapFile struct {
	Data []byte   // 映射出的缓冲区
	Fd   *os.File // 对应文件的文件描述符
}

// OpenMmapFile 打开一个现有文件或创建一个新文件，然后调用 mmap 创建映射
func OpenMmapFile(filename string, flag, maxSz int) (*MmapFile, error) {
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	// 若文件已存在，使用文件的大小
	if fileInfo, err := fd.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	return OpenMmapFileUsing(fd, maxSz, writable)
}

// OpenMmapFileUsing 为一个打开的文件调用 mmap 创建映射
func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// 新创建的文件，将其大小截断为 sz
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
	}

	buf, err := mmap.Mmap(fd, writable, int64(sz))
	if err != nil {
		return nil, errors.Wrapf(err, "while mmappint %s with size: %d", filename, sz)
	}

	// 新文件，需要确保同步目录
	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, err
}

type mmapReader struct {
	Data   []byte
	offset int
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

// Read 将 mmap 从 mr.offset 开始的数据读到 buf，返回读到的字节数
func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

// Bytes 返回 m.Data[off:off+sz]，数据不够时返回 nil
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// Slice 返回 offset 偏移处的数据，先读取数据长度，再通过长度获取实际数据
func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	buf := m.Data[start:next]
	return buf
}

const oneGB = 1 << 30

func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4

	if start+sz > len(m.Data) {
		growBy := len(m.Data)
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncate(int64(len(m.Data) + growBy)); err != nil {
			return nil, 0, err
		}
	}

	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
}

// AppendBuffer 向映射文件 offset 偏移处追加数据
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	sz := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > sz {
		growBy := sz
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncate(int64(sz + growBy)); err != nil {
			return err
		}
	}
	len := copy(m.Data[offset:end], buf)
	if len != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

// Close sync + munmap + close
func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func (m *MmapFile) Truncate(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.Fd.Name(), err)
	}
	var err error
	m.Data, err = mmap.Mremap(m.Data, int(maxSz))
	return err
}

func (m *MmapFile) ReName(name string) error {
	return nil
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}
