package file

import (
	"io"
	"os"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
	"github.com/qingw1230/corekv/utils/codec/pb"
)

// SSTable SST 文件结构
type SSTable struct {
	rw             *sync.RWMutex
	f              *MmapFile // 磁盘文件的 mmap 映射
	maxKey         []byte    // 该 sst 文件存的最大 key
	minKey         []byte    // 该 sst 文件存的最小 key
	idxTables      *pb.TableIndex
	hasBloomFilter bool
	idxLen         int // 索引长度
	idxStart       int // 索引开始位置
	fid            uint32
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

// Init 初始化 *SSTable，根据 sst 文件初始化结构体
func (sst *SSTable) Init() error {
	var ko *pb.BlockOffset
	var err error
	if ko, err = sst.initTable(); err != nil {
		return err
	}

	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	sst.minKey = minKey

	blockLen := len(sst.idxTables.Offsets)
	ko = sst.idxTables.Offsets[blockLen-1]
	keyBytes = ko.GetKey()
	maxKey := make([]byte, 0)
	copy(maxKey, keyBytes)
	sst.maxKey = maxKey
	return nil
}

// initTable 初始化 *SSTable 索引部分
func (sst *SSTable) initTable() (*pb.BlockOffset, error) {
	readPos := len(sst.f.Data)

	// 读取校验和长度
	readPos -= 4
	buf := sst.readCheckError(readPos, 4)
	checksumLen := int(codec.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// 读取索引的校验和
	readPos -= checksumLen
	expectedChk := sst.readCheckError(readPos, checksumLen)

	// 读取索引长度
	readPos -= 4
	buf = sst.readCheckError(readPos, 4)
	sst.idxLen = int(codec.BytesToU32(buf))

	// 读取索引
	readPos -= sst.idxLen
	sst.idxStart = readPos
	data := sst.readCheckError(readPos, sst.idxLen)
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", sst.f.Fd.Name())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	sst.idxTables = indexTable

	sst.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

func (s *SSTable) Indexs() *pb.TableIndex {
	return s.idxTables
}

func (s *SSTable) MaxKey() []byte {
	return s.maxKey
}

func (s *SSTable) MinKey() []byte {
	return s.minKey
}

func (s *SSTable) FID() uint32 {
	return s.fid
}

func (s *SSTable) HasBloomFilter() bool {
	return s.hasBloomFilter
}

// readCheckError 读取数据并检查错误
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

func (s *SSTable) Bytes(off, sz int) ([]byte, error) {
	return s.f.Bytes(off, sz)
}
