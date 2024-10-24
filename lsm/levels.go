package lsm

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
)

type levelManager struct {
	lsm          *LSM // 所属 LSM
	opt          *Options
	levels       []*levelHandler    // 各层的管理句柄
	manifestFile *file.ManifestFile // 保存 sst 文件的层级关系
	compactState *compactStatus     // 压缩状态
	maxFID       uint64             // 用于生成文件 ID
}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		lsm:          lsm,
		opt:          opt,
		compactState: lsm.newCompactStatus(),
	}
	err := lm.loadManifest()
	utils.Panic(err)
	lm.build()
	return lm
}

func (lm *levelManager) close() error {
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	return nil
}

func (lm *levelManager) Get(key []byte) (e *utils.Entry, err error) {
	// 在 L0 层查找
	if e, err = lm.levels[0].Get(key); e != nil {
		return
	}
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		lh := lm.levels[level]
		if e, err = lh.Get(key); e != nil {
			return
		}
	}
	return e, utils.ErrKeyNotFount
}

// flush 将内存表刷新到磁盘形成 sst 文件
func (lm *levelManager) flush(immutable *memTable) error {
	nextID := immutable.wal.FID()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, nextID)

	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		e := iter.Item().Entry()
		builder.add(e, false)
	}

	t := openTable(lm, sstName, builder)

	err := lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       nextID,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})
	utils.Panic(err)

	lm.levels[0].add(t)
	return nil
}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{
		Dir: lm.opt.WorkDir,
	})
	return
}

// build 用 MANIFEST 文件初始化各层的管理句柄
func (lm *levelManager) build() error {
	// 构建各层的管理句柄
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			lm:       lm,
			levelNum: i,
			tables:   make([]*table, 0),
		})
	}

	manifest := lm.manifestFile.GetManifest()
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}

	var maxFID uint64
	for id, tableInfo := range manifest.Tables {
		filename := utils.FileNameSSTable(lm.opt.WorkDir, id)
		if id > maxFID {
			maxFID = id
		}
		t := openTable(lm, filename, nil)
		lm.levels[tableInfo.Level].add(t)
		lm.levels[tableInfo.Level].addSize(t)
	}
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

// iterators 为 levelManager 管理的 sst 文件创建迭代器
func (lm *levelManager) iterators() []utils.Iterator {
	iters := make([]utils.Iterator, 0, len(lm.levels))
	for _, l := range lm.levels {
		iters = append(iters, l.iterators()...)
	}
	return iters
}

type levelHandler struct {
	lm *levelManager // 所属 levelManager
	sync.RWMutex
	levelNum       int      // 所管理的层级
	tables         []*table // 当前层管理的 sst 文件
	totalSize      int64
	totalStaleSize int64
}

func (lh *levelHandler) close() error {
	for _, t := range lh.tables {
		if err := t.sst.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 {
		return lh.serachL0SST(key)
	}
	return nil, nil
}

// Sort 为 SST 文件排序，L0 层以 fid 排升序，其他层以最小 key 排升序
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

func (lh *levelHandler) serachL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, t := range lh.tables {
		if e, err := t.Search(key, &version); err == nil {
			return e, nil
		}
	}
	return nil, utils.ErrKeyNotFount
}

// add 添加管理的 sst 文件
func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum-1
}

// levelHandlerRLocked 表示调用时需加 lh 的读锁
type levelHandlerRLocked struct{}

// overlappingTables 返回与 kr 范围重叠的 sst 文件，[left, right)
func (lh *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.left, lh.tables[i].sst.MaxKey()) <= 0
	})
	right := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.right, lh.tables[i].sst.MinKey()) < 0
	})
	return left, right
}

func (lh *levelHandler) replaceTables(toDel, toAdd []*table) error {
	lh.Lock()
	defer lh.Unlock()

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
		lh.subSize(t)
	}

	for _, t := range toAdd {
		lh.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	lh.tables = newTables
	sort.Slice(lh.tables, func(i, j int) bool {
		return utils.CompareKeys(lh.tables[i].sst.MinKey(), lh.tables[i].sst.MinKey()) < 0
	})
	return decrRefs(toDel)
}

func (lh *levelHandler) deleteTables(toDel []*table) error {
	lh.Lock()
	defer lh.Unlock()

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
		lh.subSize(t)
	}
	lh.tables = newTables
	return decrRefs(toDel)
}

// iterators 为 levelHandler 管理的 sst 文件创建迭代器
func (lh *levelHandler) iterators() []utils.Iterator {
	lh.RLock()
	defer lh.RUnlock()
	if len(lh.tables) == 0 {
		return nil
	}
	opt := &utils.Options{
		IsAsc: true,
	}
	if lh.levelNum == 0 {
		return iteratorsReversed(lh.tables, opt)
	}
	return []utils.Iterator{
		NewConcatIterator(lh.tables, opt),
	}
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
}

// subSize 减少管理的数据
func (lh *levelHandler) subSize(t *table) {
	lh.totalSize -= t.Size()
}

// numTables 管理的 sst 文件数量
func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}
