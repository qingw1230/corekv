package file

import (
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
	"google.golang.org/protobuf/proto"
)

type SSTable struct {
	rw             *sync.RWMutex
	f              *MmapFile
	fid            uint64
	createAt       time.Time
	maxKey         []byte         // 该 sst 文件中最大的 key
	minKey         []byte         // 该 sst 文件中最小的 key
	tableIndex     *pb.TableIndex // 该 sst 文件的索引
	hasBloomFilter bool
	idxLen         int
	idxStart       int
}

// OpenSSTable 打开一个 sst 文件
func OpenSSTable(opt *Options) *SSTable {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{
		rw:  &sync.RWMutex{},
		f:   mf,
		fid: opt.FID,
	}
}

// Init 根据 sst 文件初始化 *SSTable
func (sst *SSTable) Init() error {
	var bo *pb.BlockOffset
	var err error
	if bo, err = sst.initTableIndex(); err != nil {
		return err
	}

	stat, _ := sst.f.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	sst.createAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)

	keyBytes := bo.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	sst.minKey = minKey
	sst.maxKey = minKey
	return nil
}

// initTableIndex 从文件读取 sst 索引部分
func (sst *SSTable) initTableIndex() (*pb.BlockOffset, error) {
	readPos := len(sst.f.Data)

	// 读取校验和长度
	readPos -= 4
	buf := sst.readCheckError(readPos, 4)
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// 读取索引的校验和
	readPos -= checksumLen
	expectedChk := sst.readCheckError(readPos, checksumLen)

	// 读取索引长度
	readPos -= 4
	buf = sst.readCheckError(readPos, 4)
	sst.idxLen = int(utils.BytesToU32(buf))

	// 读取索引
	readPos -= sst.idxLen
	sst.idxStart = readPos
	data := sst.readCheckError(readPos, sst.idxLen)
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "faild to verify checksum for table: %s", sst.f.Fd.Name())
	}
	tableIndex := &pb.TableIndex{}
	if err := proto.Unmarshal(data, tableIndex); err != nil {
		return nil, err
	}
	sst.tableIndex = tableIndex

	sst.hasBloomFilter = false
	if len(tableIndex.GetOffsets()) > 0 {
		return tableIndex.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

// Indexs 获取该 sst 文件的索引
func (s *SSTable) Indexs() *pb.TableIndex {
	return s.tableIndex
}

func (s *SSTable) Close() error {
	return s.f.Close()
}

func (s *SSTable) SetMaxKey(maxKey []byte) {
	s.maxKey = maxKey
}

func (s *SSTable) MaxKey() []byte {
	return s.maxKey
}

func (s *SSTable) MinKey() []byte {
	return s.minKey
}

func (s *SSTable) FID() uint64 {
	return s.fid
}

func (s *SSTable) HasBloomFilter() bool {
	return s.hasBloomFilter
}

// readCheckError 读取数据并检验错误
func (s *SSTable) readCheckError(off, sz int) []byte {
	buf, err := s.read(off, sz)
	utils.Panic(err)
	return buf
}

// read 读取文件 [off, off+sz) 处的数据
func (s *SSTable) read(off, sz int) ([]byte, error) {
	if len(s.f.Data) > 0 {
		if len(s.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return s.f.Data[off : off+sz], nil
	}

	buf := make([]byte, sz)
	_, err := s.f.Fd.ReadAt(buf, int64(off))
	return buf, err
}

// Bytes 获取文件映射出的缓冲区中 [off, off+sz) 处的数据
func (s *SSTable) Bytes(off, sz int) ([]byte, error) {
	return s.f.Bytes(off, sz)
}

func (s *SSTable) Size() int64 {
	fileStats, err := s.f.Fd.Stat()
	utils.Panic(err)
	return fileStats.Size()
}

func (s *SSTable) GetCreatedAt() *time.Time {
	return &s.createAt
}

func (s *SSTable) SetCreatedAt(t *time.Time) {
	s.createAt = *t
}

func (s *SSTable) Delete() error {
	return s.f.Delete()
}

func (s *SSTable) Truncate(sz int64) error {
	return s.f.Truncate(sz)
}
