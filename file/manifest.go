package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
	"google.golang.org/protobuf/proto"
)

// ManifestFile 维护 sst 文件元信息的文件
type ManifestFile struct {
	opt      *Options
	mu       sync.Mutex
	f        *os.File
	manifest *Manifest
}

// Manifest 保存 sst 文件层级关系
type Manifest struct {
	Levels    []levelManifest          // 记录每层有哪些 sst 文件
	Tables    map[uint64]TableManifest // 快速查找 sst 文件在哪层
	Creations int                      // sst 文件创建次数
	Deletions int                      // sst 文件删除次数
}

type TableManifest struct {
	Level    uint8
	Checksum []byte
}

// levelManifest 存储一层有哪些 sst 文件
type levelManifest struct {
	Tables map[uint64]struct{}
}

// Table sst 文件元信息
type TableMeta struct {
	ID       uint64
	Checksum []byte
}

func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{
		opt: opt,
		mu:  sync.Mutex{},
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		// 文件不存在，创建一个新的 MANIFEST 文件
		m := createManifest()
		fp, _, err := helpRewrite(opt.Dir, m)
		if err != nil {
			return mf, err
		}
		mf.f = fp
		mf.manifest = m
		return mf, nil
	}

	// 如果打开成功，则对 MANIFEST 文件重放
	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		f.Close()
		return mf, err
	}

	if err := f.Truncate(truncOffset); err != nil {
		f.Close()
		return mf, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return mf, err
	}
	mf.f = f
	mf.manifest = manifest
	return mf, nil
}

func createManifest() *Manifest {
	return &Manifest{
		Levels: make([]levelManifest, 0),
		Tables: make(map[uint64]TableManifest),
	}
}

func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) error {
	err := mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, levelNum, t.Checksum),
	})
	return err
}

func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{
		Changes: changesParam,
	}
	buf, err := proto.Marshal(&changes)
	if err != nil {
		return nil
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		return err
	}

	if (mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold) && (mf.manifest.Creations/mf.manifest.Deletions > utils.ManifestDeletionsRatio) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCRCBuf [8]byte
		binary.BigEndian.PutUint32(lenCRCBuf[:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCRCBuf[4:], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCRCBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}

	err = mf.f.Sync()
	return err
}

// rewrite 用当前结构 mf.manifest 重写 MANIFEST 文件
func (mf *ManifestFile) rewrite() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	fp, newCreation, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.f = fp
	mf.manifest.Creations = newCreation
	mf.manifest.Deletions = 0
	return nil
}

// ReplayManifestFile 根据 MANIFEST 文件重放 *Manifest 结构
func ReplayManifestFile(fp *os.File) (m *Manifest, offset int64, err error) {
	r := &bufReader{
		reader: bufio.NewReader(fp),
	}
	// 检查 MANIFEST 文件魔数和版本号
	var magicBuf [8]byte
	if _, err = io.ReadFull(r, magicBuf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(magicBuf[:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	version := binary.BigEndian.Uint32(magicBuf[4:])
	if version != uint32(utils.MagicVersion) {
		return &Manifest{}, 0, fmt.Errorf("manifest has unsupported version: %d", version)
	}

	m = createManifest()
	for {
		offset = r.count
		var lenCRCBuf [8]byte
		if _, err = io.ReadFull(r, lenCRCBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCRCBuf[:4])
		buf := make([]byte, length)
		if _, err = io.ReadFull(r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCRCBuf[4:]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := proto.Unmarshal(buf, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
		// 应用更改，修改 *Manifest 结构
		if err = applyChangeSet(m, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	} // for {
	return m, offset, nil
}

// applyChangeSet 根据 changeSet 应用一组更改（修改 Manifest 结构体）
func applyChangeSet(m *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyChange(m, change); err != nil {
			return err
		}
	}
	return nil
}

// applyChange 对 Manifest 结构应用单个更改
func applyChange(m *Manifest, change *pb.ManifestChange) error {
	switch change.Op {
	case pb.ManifestChange_CREATE:
		if _, ok := m.Tables[change.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table: %d exists", change.Id)
		}
		m.Tables[change.Id] = TableManifest{
			Level:    uint8(change.Level),
			Checksum: append([]byte{}, change.Checksum...),
		}
		for len(m.Levels) <= int(change.Level) {
			m.Levels = append(m.Levels, levelManifest{map[uint64]struct{}{}})
		}
		m.Levels[change.Level].Tables[change.Id] = struct{}{}
		m.Creations++
	case pb.ManifestChange_DELETE:
		tm, ok := m.Tables[change.Id]
		if !ok {
			return fmt.Errorf("MANIFEST remove non-existing table: %d", change.Id)
		}
		delete(m.Tables, change.Id)
		delete(m.Levels[tm.Level].Tables, change.Id)
		m.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid ManifestChange Op")
	}
	return nil
}

// helpRewrite 用 Manifest 保存的当前结构重写 MANIFEST 文件
// MANIFEST 格式 | magic | version | change1 | change2 |
// change 格式   | change_len | checksum | ManifestChangeSet |
func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	// 打开用于 REWRITEMANIFEST 文件
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	// 添加魔数和版本号
	buf := make([]byte, 8)
	copy(buf[:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], utils.MagicVersion)
	// 生成变更集合
	newCreations := len(m.Tables)
	changes := m.asChanges()
	set := &pb.ManifestChangeSet{
		Changes: changes,
	}
	changeBuf, err := proto.Marshal(set)
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	var lenCRCBuf [8]byte
	binary.BigEndian.PutUint32(lenCRCBuf[:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCRCBuf[4:], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	// 追加数据长度、校验和、数据
	buf = append(buf, lenCRCBuf[:]...)
	buf = append(buf, changeBuf...)

	// 将 buf 写入 REWRITEMANIFEST 文件
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Close(); err != nil {
		return nil, 0, err
	}

	// 将文件重命名为 MANIFEST
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := utils.SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}
	return fp, newCreations, nil
}

// asChanges 将 m 保存的信息创建为一系列变更
func (m *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, int(tm.Level), tm.Checksum))
	}
	return changes
}

// newCreateChange 创建向 MANIFEST 添加数据的变更
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

type bufReader struct {
	reader *bufio.Reader
	count  int64 // 从 reader 中读取的总字节数
}

func (b *bufReader) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.count += int64(n)
	return n, err
}

func (mf *ManifestFile) AnalyzaManifest() {
	f := mf.f
	f.Seek(0, io.SeekStart)
	buf, err := io.ReadAll(f)
	utils.Panic(err)
	n := len(buf)

	fmt.Println("magic:    ", string(buf[:4]))
	fmt.Println("version:  ", binary.BigEndian.Uint32(buf[4:8]))

	for pos := 8; pos < n; {
		len := int(binary.BigEndian.Uint32(buf[pos : pos+4]))
		binary.BigEndian.Uint32(buf[pos+4 : pos+8])
		// fmt.Println("len:      ", len)
		// fmt.Println("checksum: ", checksum)
		pos += 8

		changes := pb.ManifestChangeSet{}
		proto.Unmarshal(buf[pos:pos+len], &changes)
		for _, change := range changes.Changes {
			fmt.Println("id: ", change.Id, " op: ", change.Op, "level: ", change.Level)
		}
		pos += len
	}
}
