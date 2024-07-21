package lsm

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
)

type levelManager struct {
	opt          *Options
	maxFID       uint64             // 用于生成文件 ID
	cache        *cache             // W-TinyLFU 缓存
	manifestFile *file.ManifestFile // 保存 sst 文件的层级关系
	levels       []*levelHandler    // 各层的管理句柄
	lsm          *LSM               // 所属 LSM
	compactState *compactStatus     // 压缩状态
}

// initLevelManager 初始化函数，读取 manifest 构建层级
func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		opt:          opt,
		lsm:          lsm,
		compactState: lsm.newCompactStatus(),
	}
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

// Get 在 LSM 树中查找 key
func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)
	// L0 层查找
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-6 层查找
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		lh := lm.levels[level]
		if entry, err = lh.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}

// loadManifest 加载 manifest 文件保存的层级关系
func (lm *levelManager) loadManifest() error {
	var err error
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}

// build 为各层的管理句柄添加 sst 文件，并加载 sst 文件的索引
func (lm *levelManager) build() error {
	// 构建各层的管理句柄
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		})
	}

	// 保证 MANIFEST 与实际 sst 文件一致
	manifest := lm.manifestFile.GetManifest()
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}

	lm.cache = newCache(lm.opt)

	var maxFID uint64
	// 为各层添加管理的 sst 文件
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFID {
			maxFID = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].add(t)
		lm.levels[tableInfo.Level].addSize(t)
	}
	// 将 sst 文件排升序
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

func (lm *levelManager) flush(immutable *memTable) error {
	nextID := immutable.wal.FID()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, nextID)

	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	table := openTable(lm, sstName, builder)

	err := lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       nextID,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})

	utils.Panic(err)
	lm.levels[0].add(table)
	return err
}

type levelHandler struct {
	sync.RWMutex
	levelNum       int           // 所管理的层级
	tables         []*table      // 当前层管理的 sst 文件集合
	totalSize      int64         // 管理的数据大小总和
	totalStaleSize int64         // 管理的过期数据大小总和
	lm             *levelManager // 所属 levelManager
}

func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].sst.Close(); err != nil {
			return err
		}
	}
	return nil
}

// add 添加管理的 sst 文件
func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 {
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

// Sort 为 sst 文件排序，L0 层以 fid 排升序，其他层以最小 key 排升序
func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].sst.MinKey(), lh.tables[j].sst.MinKey()) < 0
		})
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Search(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if entry, err := table.Search(key, &version); err == nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if bytes.Compare(key, lh.tables[i].sst.MinKey()) > -1 &&
			bytes.Compare(key, lh.tables[i].sst.MaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}

func (lh *levelHandler) addBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, ts...)
}

// getTotalSize 获取管理的数据大小
func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize
}

// addSize 增加管理的数据
func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
}

// subtractSize 减少管理的数据
func (lh *levelHandler) subtractSize(t *table) {
	lh.totalSize -= t.Size()
	lh.totalStaleSize -= int64(t.StaleDataSize())
}

// numTables 管理的 sst 文件数量
func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}

// isLastLevel 是否为最大层管理句柄
func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum-1
}

// levelHandlerRLocked 表示调用时需加 lh 的读锁
type levelHandlerRLocked struct{}

// overlappingTables 返回与 kr 范围相的 sst 文件，[left, right)
func (lh *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.left, lh.tables[i].sst.MaxKey()) <= 0
	})
	right := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.right, lh.tables[i].sst.MaxKey()) < 0
	})
	return left, right
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
func (lh *levelHandler) replaceTables(toDel, toAdd []*table) error {
	// Need to re-search the range of tables in this level to be replaced as other goroutines might
	// be changing it as well.  (They can't touch our tables, but if they add/remove other tables,
	// the indices get shifted around.)
	lh.Lock() // We s.Unlock() below.

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}

	// Increase totalSize first.
	for _, t := range toAdd {
		lh.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	// Assign tables.
	lh.tables = newTables
	sort.Slice(lh.tables, func(i, j int) bool {
		return utils.CompareKeys(lh.tables[i].sst.MinKey(), lh.tables[i].sst.MinKey()) < 0
	})
	lh.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(toDel)
}

// deleteTables remove tables idx0, ..., idx1-1.
func (lh *levelHandler) deleteTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}
	lh.tables = newTables

	lh.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(toDel)
}
