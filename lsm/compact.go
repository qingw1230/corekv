package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
)

type compactionPriority struct {
	level       int
	score       float64
	adjusted    float64
	dropPrefixs [][]byte
	t           targets
}

// targets 管理各层及内部 sst 期望大小
type targets struct {
	targetLevel int     // 压缩的目标层（L0 层向其他层压缩时使用）
	targetSize  []int64 // 每层的期望大小
	fileSize    []int64 // 每层内 sst 的期望大小
}

// compactDef 压缩计划
type compactDef struct {
	compactID   int // 执行该压缩计划的协程 id
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler // 当前层管理句柄
	targetLevel *levelHandler // 目标层管理句柄

	top      []*table // 当前层要压缩的 sst 文件列表
	bot      []*table // 目标层要压缩的 sst 文件列表
	thisSize int64    // 当前压缩计划 sst 文件总大小

	thisRange   keyRange   // 当前层 key 范围
	targetRange keyRange   // 目标层 key 范围
	splits      []keyRange // 压缩计划分成的子压缩

	dropPrefixs [][]byte
}

// lockLevels 为压缩计划用到层加读锁
func (c *compactDef) lockLevels() {
	c.thisLevel.RLock()
	c.targetLevel.RLock()
}

// unlockLevels 为压缩计划用的层解锁
func (c *compactDef) unlockLevels() {
	c.thisLevel.Unlock()
	c.targetLevel.Unlock()
}

// thisAndTargetLevelRLocked 表示调用时需加当前层和目标层 lh 的读锁
type thisAndTargetLevelRLocked struct{}

// runCompacter 启动一个 compacter
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()

	// 随机的启动时间使压缩器执行的并发性被打散，降低冲突的可能性
	randomDelay := time.NewTicker(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
		randomDelay.Stop()
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}

	// 每 50s 执行一次压缩
	ticker := time.NewTicker(50000 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}

// runOnce 执行一次压缩
func (lm *levelManager) runOnce(id int) bool {
	prios := lm.pickCompactLevels()

	// 0 号协程总是倾向于压缩 L0 层
	if id == 0 {
		prios = moveL0toFront(prios)
	}

	for _, p := range prios {
		if lm.run(id, p) {
			return true
		}
	}
	return false
}

func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
	default:
		log.Printf("[taskID:%d] while running doCompact: %v\n", id, err)
	}
	return false
}

func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	if p.t.targetLevel == 0 {
		p.t = lm.levelTargets()
	}

	level := p.level
	cd := compactDef{
		compactID:   id,
		t:           p.t,
		p:           p,
		thisLevel:   lm.levels[level],
		dropPrefixs: p.dropPrefixs,
	}

	if level == 0 {
		cd.targetLevel = lm.levels[p.t.targetLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		// 其他层就是压缩到下一层（最后一层除外）
		cd.targetLevel = cd.thisLevel
		if !cd.thisLevel.isLastLevel() {
			cd.targetLevel = lm.levels[level+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}

	defer lm.compactState.delete(cd)
	if err := lm.runCompactDef(id, level, cd); err != nil {
		return err
	}
	return nil
}

func (lm *levelManager) runCompactDef(id, level int, cd compactDef) error {
	if len(cd.t.fileSize) == 0 {
		return errors.New("filesize cannot be zero. targets are not set.")
	}

	thisLevel := cd.thisLevel
	targetLevel := cd.targetLevel

	if thisLevel != targetLevel {
		lm.addSplits(&cd)
	}

	// newTables 合成生成的 table 列表
	// decr 用于减少这组 sst 文件的引用计数
	newTables, decr, err := lm.compactBuildTables(level, cd)
	if err != nil {
		return err
	}

	defer func() {
		if decrErr := decr(); err == nil {
			err = decrErr
		}
	}()

	// 更新 MANIFEST 文件
	changeSet := buildChangeSet(&cd, newTables)
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}
	if err := targetLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	defer decrRefs(cd.top)
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	return nil
}

// compactBuildTables 合成两层的 sst 文件，返回合并生成的 table 列表
func (lm *levelManager) compactBuildTables(level int, cd compactDef) ([]*table, func() error, error) {
	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc: true,
	}

	newIter := func() []utils.Iterator {
		var iters []utils.Iterator
		switch {
		case level == 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			// 此时 topTables 中只有一个 sst 文件
			iters = []utils.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	tableBuf := make(chan *table, 4)
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	// 开始并行执行压缩过程
	for _, kr := range cd.splits {
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := NewMergeIterator(newIter(), false)
			defer it.Close()
			lm.subcompact(it, kr, cd, inflightBuilders, tableBuf)
		}(kr)
	}

	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for t := range tableBuf {
			newTables = append(newTables, t)
		}
	}()

	// 等待所有压缩过程完成，并且已生成 sst 文件
	err := inflightBuilders.Finish()
	close(tableBuf)
	wg.Wait()

	if err == nil {
		err = utils.SyncDir(lm.opt.WorkDir)
	}
	if err != nil {
		decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].sst.MaxKey(), newTables[j].sst.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// subcompact 用 it 在 kr 范围内的数据生成 sst 文件，将生成的文件写入 tableBuf
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef, inflightBuilders *utils.Throttle, tableBuf chan<- *table) {
	var lastKey []byte

	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			if !utils.SameKey(key, lastKey) {
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					break
				}
				lastKey = utils.SafeCopy(lastKey, key)
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				tableKr.right = lastKey
			}

			builder.AddKey(it.Item().Entry())
		} // for ; it.Valid(); it.Next() {
	} // addKeys := func(builder *tableBuilder) {

	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		it.Rewind()
	}

	for it.Valid() {
		key := it.Item().Entry().Key
		// 属于该子压缩的任务已经完成了，更大的 key 不归它负责
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}

		builder := newTAbleBuilderWithSSTSize(lm.opt, cd.t.fileSize[cd.thisLevel.levelNum])
		addKeys(builder)
		if builder.empty() {
			builder.finish()
			builder.Close()
			continue
		}

		// 创建一个协程，将 builder 内容刷到磁盘
		if err := inflightBuilders.Do(); err != nil {
			break
		}
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			newFID := atomic.AddUint64(&lm.maxFID, 1)
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			t := openTable(lm, sstName, builder)
			if t == nil {
				return
			}
			tableBuf <- t
		}(builder)
	} // for it.Valid() {
}

// addSplits 将压缩计划分成多个子压缩
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// 每 witch 个 sst 文件分成一子压缩
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}

	skr := cd.thisRange
	skr.extend(cd.targetRange)

	addRange := func(right []byte) {
		skr.right = utils.Copy(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i, t := range cd.bot {
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if (i+1)%width == 0 {
			right := utils.KeyWithTs(utils.ParseKey(t.sst.MaxKey()), 0)
			addRange(right)
		}
	}
}

func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

// buildChangeSet 构造因生成 cd 压缩计划生成 newTables 引起的 MANIFEST 变更
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, t := range newTables {
		changes = append(changes, newCreateChange(t.fid, cd.targetLevel.levelNum))
	}
	for _, t := range cd.top {
		changes = append(changes, newDeleteChange(t.fid))
	}
	for _, t := range cd.bot {
		changes = append(changes, newDeleteChange(t.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

// fillTables 添加压缩计划
func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}

	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}

	lm.sortByHeuristic(tables, cd)

	// Lx 到 Ly 的压缩，优先选择最早创建的 sst 文件
	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		cd.top = []*table{t}
		left, right := cd.targetLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.targetLevel.tables[left:right])

		// 下一层没有与 t 重叠的 sst 文件，直接将其压缩到下一层
		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.targetRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndTargetLevelRLocked{}, *cd) {
				continue
			}
			return true
		}

		cd.targetRange = getKeyRange(cd.bot...)
		if lm.compactState.overlapsWith(cd.targetLevel.levelNum, cd.targetRange) {
			continue
		}
		if !lm.compactState.compareAndAdd(thisAndTargetLevelRLocked{}, *cd) {
			continue
		}
		return true
	} // for _, t := range tables {
	return false
}

func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		return false
	}

	cd.bot = []*table{}
	// collectBotTables 收集更多的 sst 文件一起压缩
	collectBotTables := func(t *table, needSize int64) {
		totalSize := t.Size()
		j := sort.Search(len(sortedTables), func(i int) bool {
			return utils.CompareKeys(sortedTables[i].sst.MinKey(), t.sst.MinKey()) >= 0
		})
		j++
		for j < len(sortedTables) {
			newTable := tables[j]
			totalSize += newTable.Size()

			if totalSize >= needSize {
				break
			}
			cd.bot = append(cd.bot, newTable)
			cd.targetRange.extend(getKeyRange(newTable))
			j++
		}
	}

	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Minute {
			continue
		}
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		cd.targetRange = cd.thisRange
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// 找到了一个需要压缩的 sst 文件
		cd.top = []*table{t}
		needFileSize := cd.t.fileSize[cd.thisLevel.levelNum]
		if t.Size() >= needFileSize {
			break
		}
		// 当前 sst 文件不到预期大小，多找几个一起压缩
		collectBotTables(t, needFileSize)
		if !lm.compactState.compareAndAdd(thisAndTargetLevelRLocked{}, *cd) {
			// 压缩计划添加失败，重新找
			cd.bot = cd.bot[:0]
			cd.targetRange = keyRange{}
			continue
		}
		return true
	} // for _, t := range sortedTables {

	if len(cd.top) == 0 {
		return false
	}
	return lm.compactState.compareAndAdd(thisAndTargetLevelRLocked{}, *cd)
}

// fillTablesL0 添加 L0 层到其他层的压缩计划
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

// fillTablesL0ToL0 添加 L0 层到 L0 层的压缩计划
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactID != 0 {
		return false
	}

	cd.targetLevel = lm.levels[0]
	cd.targetRange = keyRange{}
	cd.bot = nil

	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()
	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	var out []*table
	now := time.Now()
	for _, t := range cd.thisLevel.tables {
		// 不对过大的 sst 文件进行压缩，这会造成性能抖动
		if t.Size() >= 2*cd.t.fileSize[0] {
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			continue
		}
		// 已经处于压缩状态，也应该忽略
		if _, aleady := lm.compactState.tables[t.fid]; aleady {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	thisLevelState := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevelState.ranges = append(thisLevelState.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	cd.t.fileSize[0] = math.MaxUint32
	return true
}

// fillTablesL0ToLbase 添加 L0 到 Lbase 的压缩计划
func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.targetLevel.levelNum == 0 {
		utils.Panic(errors.New("base level cannot be zero"))
	}

	if cd.p.score > 0.0 && cd.p.score < 1.0 {
		return false
	}

	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	// thisLevel.top[0] 是 L0 层最老的文件，若该层其他文件和它 key 范围有重叠，就一起压缩
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			break
		}
	}

	cd.thisRange = kr
	cd.top = out

	left, right := cd.targetLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.targetLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.targetRange = cd.thisRange
	} else {
		cd.targetRange = getKeyRange(cd.bot...)
	}
	return lm.compactState.compareAndAdd(thisAndTargetLevelRLocked{}, *cd)
}

// pickCompactLevels 选择合适的层执行压缩，返回优先级列表
func (lm *levelManager) pickCompactLevels() []compactionPriority {
	t := lm.levelTargets()

	var prios []compactionPriority
	addPriority := func(level int, score float64) {
		prio := compactionPriority{
			level: level,
			score: score,
			t:     t,
		}
		prios = append(prios, prio)
	}

	// L0 层根据 sst 文件数量计算优先级
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLeverZeroTables))
	// 非 L0 层根据层大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		compactSize := lm.compactState.compactSize(i)
		sz := lm.levels[i].getTotalSize() - compactSize
		addPriority(i, float64(sz)/float64(t.targetSize[i]))
	}

	out := prios[:0]
	for _, p := range prios {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	sort.Slice(prios, func(i, j int) bool {
		return prios[i].score > prios[j].score
	})
	return prios
}

// levelTargets 计算各层及其 sst 的期望大小，确定压缩的目标层
func (lm *levelManager) levelTargets() targets {
	// adjust 向上调整，保证大于最小值
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}
	t := targets{
		targetSize: make([]int64, len(lm.levels)),
		fileSize:   make([]int64, len(lm.levels)),
	}

	// 从最后一层开始计算每层期望的大小
	dbSize := lm.lastLevelHandler().getTotalSize()
	for i := len(lm.levels) - 1; i > 0; i-- {
		levelTargetSize := adjust(dbSize)
		t.targetSize[i] = levelTargetSize
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}

	// 从第一层开始计算每层期望的 sst 大小
	sstSz := lm.opt.BaseLevelSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			t.fileSize[i] = lm.opt.MemTableSize
		} else {
			sstSz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSize[i] = sstSz
		}
	}

	// 将目标层调整到最后一个为空的 level
	for i := t.targetLevel + 1; i < len(lm.levels); i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.targetLevel = i
	}

	// 当前层为空，下一层也还没满，继续调整目标层
	// 当前层不为空时不能调整，否则将影响数据布局，进而影响查找
	b := t.targetLevel
	if b < len(lm.levels)-1 && lm.levels[b].getTotalSize() == 0 && lm.levels[b+1].getTotalSize() < t.targetSize[b+1] {
		t.targetLevel++
	}
	return t
}

// sortByHeuristic 以 sst MaxVersion 排升序
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.targetLevel == nil {
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].sst.Indexs().MaxVersion < tables[j].sst.Indexs().MaxVersion
	})
}

// sortByStaleDataSize 以过期数据大小排降序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.targetLevel == nil {
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// moveL0toFront 将 L0 层的移到列表最前面
func moveL0toFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// lastLevelHandler 获取最后一层的管理句柄
func (lm *levelManager) lastLevelHandler() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus // 各层的压缩状态
	tables map[uint64]struct{}   // 参与压缩的 sst 文件集合
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, lsm.option.MaxLevelNum),
		tables: make(map[uint64]struct{}),
	}
	return cs
}

// overlapsWith 检查指定层在 r 范围是否有压缩计划
func (cs *compactStatus) overlapsWith(level int, r keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(r)
}

// compareAndAdd 尝试添加压缩计划，key 范围没有冲突时才能添加
func (cs *compactStatus) compareAndAdd(_ thisAndTargetLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	thisLevelState := cs.levels[cd.thisLevel.levelNum]
	targetLevelState := cs.levels[cd.targetLevel.levelNum]

	if thisLevelState.overlapsWith(cd.thisRange) {
		return false
	}
	if targetLevelState.overlapsWith(cd.targetRange) {
		return false
	}

	// 没有冲突的情况下才能压缩
	thisLevelState.ranges = append(thisLevelState.ranges, cd.thisRange)
	targetLevelState.ranges = append(targetLevelState.ranges, cd.targetRange)
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

// delete 将指定压缩计划删除
func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	thisLevelState := cs.levels[cd.thisLevel.levelNum]
	targetLevelState := cs.levels[cd.targetLevel.levelNum]

	thisLevelState.compactSize -= cd.thisSize
	thisLevelState.remove(cd.thisRange)
	if cd.thisLevel != cd.targetLevel && !cd.targetRange.isEmpty() {
		targetLevelState.remove(cd.targetRange)
	}

	// 将该压缩计划包含的 sst 文件删除
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables id nil"))
		delete(cs.tables, t.fid)
	}
}

// compactSize 获取指定处于压缩的 sst 文件总大小
func (cs *compactStatus) compactSize(level int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[level].compactSize
}

// levelCompactStatus 一层的压缩状态
type levelCompactStatus struct {
	ranges      []keyRange // 当前层处于压缩状态的 key 范围列表
	compactSize int64      // 当前层处于压缩状态的 sst 文件总大小
}

// overlapsWith 检查是否与已处于压缩的 key 范围有重叠
func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

// remove 在当前层删除指定的 key 范围
func (lcs *levelCompactStatus) remove(dst keyRange) (found bool) {
	newRanges := lcs.ranges[:0]
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			newRanges = append(newRanges, r)
		} else {
			found = true
		}
	}
	lcs.ranges = newRanges
	return
}

// getKeyRange 返回一组 sst 文件的 key 范围
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}

	minKey := tables[0].sst.MinKey()
	maxKey := tables[0].sst.MaxKey()
	for _, t := range tables[1:] {
		if utils.CompareKeys(t.sst.MinKey(), minKey) < 0 {
			minKey = t.sst.MinKey()
		}
		if utils.CompareKeys(t.sst.MaxKey(), maxKey) > 0 {
			maxKey = t.sst.MaxKey()
		}
	}

	return keyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), 0),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), math.MaxUint64),
	}
}

// keyRange key 的范围
type keyRange struct {
	left  []byte
	right []byte
	inf   bool // 无穷大的 key 范围
	size  int64
}

var infRange = keyRange{inf: true}

// extend 用 r 扩展 kr 保存的范围
func (kr *keyRange) extend(r keyRange) {
	if r.isEmpty() {
		return
	}
	if kr.isEmpty() {
		*kr = r
		return
	}
	if len(kr.left) == 0 || utils.CompareKeys(kr.left, r.left) > 0 {
		kr.left = r.left
	}
	if len(kr.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		kr.right = r.right
	}
	if r.inf {
		kr.inf = r.inf
	}
}

// overlapsWith 判断 key 范围是否有重叠，kr 为空总是重叠，r 为空总是不重叠
func (kr *keyRange) overlapsWith(r keyRange) bool {
	if kr.isEmpty() {
		return true
	}
	if r.isEmpty() {
		return false
	}
	if kr.inf || r.inf {
		return true
	}

	if utils.CompareKeys(kr.left, r.right) > 0 {
		return false
	}
	if utils.CompareKeys(kr.right, r.left) < 0 {
		return false
	}
	return true
}

// isEmpty 范围是否为空
func (kr *keyRange) isEmpty() bool {
	return len(kr.left) == 0 && len(kr.right) == 0 && !kr.inf
}

func (kr *keyRange) equals(dst keyRange) bool {
	return bytes.Equal(kr.left, dst.left) && bytes.Equal(kr.right, dst.right) && kr.inf == dst.inf
}

func (kr *keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", kr.left, kr.right, kr.inf)
}

// iteratorsReversed 以倒序为 L0 层 sst 文件创建迭代器
func iteratorsReversed(tables []*table, opt *utils.Options) []utils.Iterator {
	out := make([]utils.Iterator, 0, len(tables))
	for i := len(tables) - 1; i >= 0; i-- {
		out = append(out, tables[i].NewIterator(opt))
	}
	return out
}
