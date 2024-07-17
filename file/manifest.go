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

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
)

// ManifestFile 维护 sst 文件元信息的文件
type ManifestFile struct {
	opt                 *Options
	f                   *os.File
	mu                  sync.Mutex
	manifest            *Manifest // 实际保存层级关系的的结构体
	delRewriteThreshold int
}

type Manifest struct {
	// Levels level -> table sst 每层有哪些 sst 文件
	Levels []levelManifest
	// Tables 用于快速查找一个 sst 文件在哪一层
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

// TableManifast 包含 sst 文件的基本信息
type TableManifest struct {
	Level    uint8  // sst 文件所在层级
	Checksum []byte // mock
}

// levelManifest 存储一层里有哪些 sst 文件
type levelManifest struct {
	Tables map[uint64]struct{} // sst id 的集合
}

// TableMeta sst 文件的元信息
type TableMeta struct {
	ID       uint64
	Checksum []byte
}

// OpenManifestFile 打开 manifest 文件，并重放 MANIFEST 存储的层级关系
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{
		opt: opt,
		mu:  sync.Mutex{},
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	// 如果打开失败，则尝试创建一个新的 manifest 文件
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		m := createManifest()
		fp, _, err := helpRewrite(opt.Dir, m)
		if err != nil {
			return mf, err
		}
		mf.f = fp
		mf.manifest = m
		return mf, nil
	}

	// 如果打开成功，则对 manifest 文件重放
	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		f.Close()
		return mf, err
	}
	// 将文件大小更改为正确的值
	if err := f.Truncate(truncOffset); err != nil {
		f.Close()
		return mf, err
	}
	// 设置下次写入偏移位置
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return mf, err
	}

	mf.f = f
	mf.manifest = manifest
	return mf, nil
}

// ReplayManifestFile 根据 MANIFEST 文件重放内存中的 *Manifest
// 返回重放的 *Manifest，MANIFEST 文件的偏移以及 error
func ReplayManifestFile(fp *os.File) (*Manifest, int64, error) {
	r := &bufReader{
		reader: bufio.NewReader(fp),
	}
	// 检查 MANIFEST 文件魔数和版本号
	var magicBuf [8]byte
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != uint32(utils.MagicVersion) {
		return &Manifest{}, 0, fmt.Errorf("manifest has unsupported version: %d", version)
	}

	build := createManifest()
	var offset int64
	// 循环读取 MANIFEST 文件，修改内存中相应结构体
	for {
		offset = r.count
		// 读取数据长度、校验和，并验证校验和是否正确
		var lenCRCBuf [8]byte
		if _, err := io.ReadFull(r, lenCRCBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCRCBuf[0:4])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCRCBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return &Manifest{}, 0, err
		}
		// 应用更改，修改 *Manifest 结构
		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	} // for {
	return build, offset, nil
}

// applyChangeSet 根据 manifest 文件内容应用一组更改（修改内存中的 Manifest 结构体）
func applyChangeSet(build *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

// applyManifestChange 对 Manifest 结构体应用单个更改
func applyManifestChange(build *Manifest, change *pb.ManifestChange) error {
	switch change.Op {
	case pb.ManifestChange_CREATE:
		// 检查该 sst 文件信息是否在 Manifest 中
		if _, ok := build.Tables[change.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", change.Id)
		}
		// 不存在时添加相关信息
		build.Tables[change.Id] = TableManifest{
			Level:    uint8(change.Level),
			Checksum: append([]byte{}, change.Checksum...),
		}
		for len(build.Levels) <= int(change.Level) {
			build.Levels = append(build.Levels, levelManifest{map[uint64]struct{}{}})
		}
		build.Creations++
		build.Levels[change.Level].Tables[change.Id] = struct{}{}
	case pb.ManifestChange_DELETE:
		tm, ok := build.Tables[change.Id]
		if !ok {
			return fmt.Errorf("MANIFEST remove non-existing table %d", change.Id)
		}
		delete(build.Tables, change.Id)
		delete(build.Levels[tm.Level].Tables, change.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid ManifestChange Op")
	}
	return nil
}

// 创建一个空的 Manifest 结构体
func createManifest() *Manifest {
	return &Manifest{
		Levels: make([]levelManifest, 0),
		Tables: make(map[uint64]TableManifest),
	}
}

type bufReader struct {
	reader *bufio.Reader
	// count 从 reader 中读取的总字节数
	count int64
}

func (b *bufReader) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.count += int64(n)
	return n, err
}

// asChanges 返回一系列更改，这些更改可以用来重放 manifest 的当前状态
func (m *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, int(tm.Level), tm.Checksum))
	}
	return changes
}

// newCreateChange 创建一个向 manifest 添加数据的更改
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// rewrite 必须在持有 appendLock 时调用，重写 manifest 文件
func (mf *ManifestFile) rewrite() error {
	if err := mf.Close(); err != nil {
		return err
	}
	fp, newCreations, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.manifest.Creations = newCreations
	mf.manifest.Deletions = 0
	mf.f = fp
	return nil
}

// helpRewrite 用 Manifest 保存的当前状态重写 manifest 文件，修改的是磁盘上的文件
// manifest 格式 | magic | version | changes | changes |
// changes 格式 | len | crc32 | ManifestChangeSet |
func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	// 打开一个用于重写的 manifest 文件
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	// 添加魔数和版本号
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))
	// 生成变更集合
	newCreations := len(m.Tables)
	changes := m.asChanges()
	set := pb.ManifestChangeSet{
		Changes: changes,
	}
	// 存储序列化后的变更集合
	changeBuf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	// 获取数据长度，并为数据生成校验和
	var lenCRCBuf [8]byte
	binary.BigEndian.PutUint32(lenCRCBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCRCBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	// 追加数据长度、检验和、数据
	buf = append(buf, lenCRCBuf[:]...)
	buf = append(buf, changeBuf...)

	// 将 buf 写入用于 REWRITEMANIFEST 并同步
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	// 将文件重命名为 MANIFEST 并重新打开
	if err := fp.Close(); err != nil {
		return nil, 0, err
	}
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

func (mf *ManifestFile) AddChanges(changesParam []*pb.ManifestChange) error {
	return mf.addChanges(changesParam)
}

// addChanges 添加一批变更，同时更新内存和磁盘的相关结构，必要时重写 manifest 文件
func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{
		Changes: changesParam,
	}
	buf, err := changes.Marshal()
	if err != nil {
		return nil
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		return err
	}
	// TODO(qingw1230): 添加需要重写的条件，很大并且缩减了很多时重写
	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCRCBuf [8]byte
		// 后续每添加的一批数据前面都有长度和校验和信息
		binary.BigEndian.PutUint32(lenCRCBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCRCBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCRCBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
}

// AddTableMeta 添加 sst 信息，更新 manifest 相关结构
func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) error {
	err := mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, levelNum, t.Checksum),
	})
	return err
}

// RevertToManifest 检查所有在 MANIFEST 中的文件是否存在，删除不在 MANIFEST 但实际存在的文件
// idMap 是从目录中读取的 sst 文件 id 集合
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	// 检查 manifest 中的所有 sst 文件是否存在
	for id := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 删除不应该存在的文件
	for id := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("table file %d not referenced in MANIFEST", id))
			filename := utils.FileNameSSTable(mf.opt.Dir, id)
			// 删除不在 MANIFEST 文件中，但实际存在的文件
			if err := os.Remove(filename); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

// Close 关闭 manifest 的底层文件
func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}
