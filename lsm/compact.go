package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
)

type compactionPriority struct {
	level       int     // 所属层级
	score       float64 // 得分
	adjusted    float64 // 调整后的得分
	dropPrefixs [][]byte
	t           targets
}

// targets 管理各层及内部 sst 文件期望大小
type targets struct {
	baseLevel int     // 压缩目标层
	targetSz  []int64 // 每层的期望大小
	fileSz    []int64 // 每层内 sst 文件期望大小
}

// compactDef 压缩计划
type compactDef struct {
	compactID int // 执行该压缩计划的协程 id
	t         targets
	p         compactionPriority // 压缩优先级
	thisLevel *levelHandler      // 当前层管理句柄
	nextLevel *levelHandler      // 目标层管理句柄

	top []*table // 当前层要压缩的 sst 文件列表
	bot []*table // 目标层要参与压缩的 sst 文件列表

	thisRange keyRange   // 当前层 key 范围
	nextRange keyRange   // 目标层 key 范围
	splits    []keyRange // 压缩计划分成的子压缩

	thisSize int64 // 当前 sst 文件大小

	dropPrefixs [][]byte
}

// lockLevels 为压缩计划用的层加读锁
func (c *compactDef) lockLevels() {
	c.thisLevel.RLock()
	c.nextLevel.RLock()
}

// unlockLevels 将压缩计划用的层的读锁解锁
func (c *compactDef) unlockLevels() {
	c.thisLevel.Unlock()
	c.nextLevel.Unlock()
}

// runCompacter 启动一个 compacter
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	// 随机的启动时间使每个压缩器执行的并发性被打散，降低冲突的可能性
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)

	select {
	case <-randomDelay.C:
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}

	// 每 50s 执行一次压缩
	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
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
	if id == 0 {
		// 0 号协程总是倾向于压缩 L0 层
		prios = moveL0ToFront(prios)
	}

	for _, p := range prios {
		if id == 0 && p.level == 0 {
			// 对于 L0 不论得分多少都要运行
		} else if p.adjusted < 1.0 {
			break
		}

		if lm.run(id, p) {
			return true
		}
	}
	return false
}

// moveL0ToFront 将 L0 层的移到列表最前面
func moveL0ToFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	// 找 L0 层所在的下标
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}
	// 将 L0 层的移到列表最前面
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// run 执行一个由 p 指定的压缩计划
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
		// 什么也不做，此时合并过程被忽略
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// doCompact 选择当前层的某些 sst 文件压缩到目标层
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	if p.t.baseLevel == 0 {
		p.t = lm.levelTargets()
	}

	level := p.level
	// 创建真正的压缩计划
	cd := compactDef{
		compactID:   id,
		p:           p,
		t:           p.t,
		thisLevel:   lm.levels[level],
		dropPrefixs: p.dropPrefixs,
	}

	// 添加压缩计划
	if level == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// 如果不是最后一层，则压缩到下一层即可
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[level+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}

	// 完成压缩后，从压缩状态中删除
	defer lm.compactState.delete(cd)
	// 执行压缩计划
	if err := lm.runCompactDef(id, level, cd); err != nil {
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// pickCompactLevels 选择合适的 level 执行压缩，返回优先级列表
func (lm *levelManager) pickCompactLevels() []compactionPriority {
	t := lm.levelTargets()

	var prios []compactionPriority
	addPriority := func(level int, score float64) {
		prio := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, prio)
	}

	// L0 层根据 sst 文件数量计算优先级
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))
	// 非 L0 层根据大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		// 处于压缩状态的 sst 不计算在内
		delSize := lm.compactState.delSize(i)
		sz := lm.levels[i].getTotalSize() - delSize
		// 当前层未在压缩状态的 sst 文件总大小与预期大小的比值为得分
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}

	// var prevLevel int
	// for level := t.baseLevel; level < len(lm.levels); level++ {
	// 	if prios[prevLevel].adjusted >= 1 {
	// 		const minScore = 0.01
	// 		if prios[level].score >= minScore {
	// 			prios[prevLevel].adjusted /= prios[level].adjusted
	// 		} else {
	// 			prios[prevLevel].adjusted /= minScore
	// 		}
	// 	}
	// 	prevLevel = level
	// }

	// 仅选择得分大于 1 的压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按调整调整后的得分排降序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// lastLevel 获取最后一层管理句柄
func (l *levelManager) lastLevel() *levelHandler {
	return l.levels[len(l.levels)-1]
}

// levelTargets 返回各层及其 sst 文件期望大小，确定压缩的目标 level
func (lm *levelManager) levelTargets() targets {
	// adjust 将层期望大小调整为最小值
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}
	t := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}

	// 从最后一层开始计算每层期望大小
	dbSize := lm.lastLevel().getTotalSize()
	for i := len(lm.levels) - 1; i > 0; i-- {
		levelTargetSize := adjust(dbSize)
		t.targetSz[i] = levelTargetSize
		// 从后向前，找到一个不满足期望大小的 level
		if t.baseLevel == 0 && levelTargetSize <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}

	// 从第一层开始计算每层期望 sst 文件大小
	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// 将 baseLevel 调整到最后一个为空的 level
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}
	// 当前层为空，下一层也还没满，继续调整 baseLevel
	b := t.baseLevel
	if b < len(lm.levels)-1 && lm.levels[b].getTotalSize() == 0 && lm.levels[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

// thisAndNextLevelRLocked 表示调用时需加当前层和目标层 lh 的读锁
type thisAndNextLevelRLocked struct{}

// fillTables 添加压缩计划
func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}

	// MaxLevel 到 MaxLevel 的压缩以过期数据大小为选择依据
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}

	lm.sortByHeuristic(tables, cd)

	// Lx 到 Ly 的压缩，优先选择最早创建的 sst 文件
	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// 当前 sst 文件的范围有冲突，继续寻找
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		cd.top = []*table{t}
		// 获取下层中与 t 有重叠的 sst 文件
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		// 下一层没有与 t 重叠的 sst 文件，直接将其压缩到下一层
		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}

		cd.nextRange = getKeyRange(cd.bot...)
		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	} // for _, t := range tables {
	return false
}

// sortByHeuristic 以 sst MaxVersion 排升序
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].sst.Indexs().MaxVersion < tables[j].sst.Indexs().MaxVersion
	})
}

// runCompactDef 执行压缩计划
func (lm *levelManager) runCompactDef(id, level int, cd compactDef) error {
	if len(cd.t.fileSz) == 0 {
		return errors.New("filesize cannot be zero. Targets are not set")
	}

	timeStart := time.Now()
	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	if thisLevel != nextLevel {
		lm.addSplits(&cd)
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// newTables 合并生成的 table 列表
	// dedr 用于减少这组 sst 文件的引用计数
	newTables, decr, err := lm.compactBuildTables(level, cd)
	if err != nil {
		return err
	}

	defer func() {
		if decrErr := decr(); err == nil {
			err = decrErr
		}
	}()

	changeSet := buildChangeSet(&cd, newTables)

	// 更新 manifest 文件
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	defer decrRefs(cd.top)
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// 打印耗时较长的压缩
	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

func tablesToString(tables []*table) []string {
	var buf []string
	for _, t := range tables {
		buf = append(buf, fmt.Sprintf("%05d", t.fid))
	}
	buf = append(buf, ".")
	return buf
}

// buildChangeSet 构造由 newTables 引起的 manifest 更改
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, t := range newTables {
		changes = append(changes, newCreateChange(t.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables 合并两层的 sst 文件，返回合并生成的 table 列表
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

	// tableBuf 用来存储压缩好的 sst 文件
	tableBuf := make(chan *table, 3)
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

	// newTables 存储新创建的 table 句柄
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)

	// 收集 table 的句柄
	go func() {
		defer wg.Done()
		for t := range tableBuf {
			newTables = append(newTables, t)
		}
	}()

	// 等待所有压缩过程完成，完成时 table 已写入 tableBuf 了
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

// addSplits 将压缩计划划分为多个子压缩
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// 每 width 个 sst 文件分成一个子压缩
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}

	skr := cd.thisRange
	skr.extend(cd.nextRange)

	// addRange 增加 [skr.left, right] 一段子范围
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
		if i&width == width-1 {
			right := utils.KeyWithTs(utils.ParseKey(t.sst.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// sortByStaleDataSize 用 sst 中过期数据大小排降序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// fillMaxLevelTables 添加 MaxLevel 的压缩计划
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		return false
	}

	cd.bot = []*table{}
	// 收集更多 sst 文件一起压缩
	collectBotTables := func(t *table, needSz int64) {
		totalSize := t.Size()
		// 找到 t 在 数组中的下标
		j := sort.Search(len(sortedTables), func(i int) bool {
			return utils.CompareKeys(sortedTables[i].sst.MinKey(), t.sst.MinKey()) >= 0
		})
		j++
		// 寻找更多一起压缩的 sst 文件，直到达到所需大小
		for j < len(sortedTables) {
			newTable := tables[j]
			totalSize += newTable.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newTable)
			cd.nextRange.extend(getKeyRange(newTable))
			j++
		}
	}

	now := time.Now()
	for _, t := range sortedTables {
		// 一个小时内创建的不执行压缩
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			continue
		}
		// 过期数据很少的 sst 文件不执行压缩
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// 如果不这样做，无法同时运行多个 MaxLevel 压缩
		cd.nextRange = cd.thisRange
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// 找到了一个需要压缩的 sst 文件
		cd.top = []*table{t}
		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]
		// 当前 sst 文件已经够大了，不再找更多的文件了
		if t.Size() >= needFileSz {
			break
		}

		// 当前 sst 文件比较小，多找几个 sst 文件一起压缩
		collectBotTables(t, needFileSz)
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			// 压缩计划添加失败，重新寻找
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	} // for _, t := range sortedTables {
	if len(cd.top) == 0 {
		return false
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 添加 L0 层的压缩计划
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

// fillTablesL0ToLbase L0 到 Lbase 的压缩计划
func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level cannot be zero"))
	}
	// 说明 L0 层文件数量少
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
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
	// thisLevel.top[0] 是 L0 层最老的文件
	// 若 thisLevel[1]... 和 thisLevel[0] key 范围有重叠，那就一起压缩
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

	// 获取与当前层 key 范围有重叠的 sst 文件
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0ToL0 L0 到 L0 的压缩计划，该函数只能由 0 号协程执行，并且会锁定整个 L0 层
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactID != 0 {
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	// 遍历 L0 层文件，确定要压缩的 sst 文件
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在 L0 to L0 的压缩过程中，不对过大的 sst 文件进行压缩，这会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// sst 的创建时间不足 10s 不要回收
			continue
		}
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			// 已经处于压缩状态，也应该忽略
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// 在 L0 层自压缩过程中，避免任何 L0 到其他层的压缩
	thisLevelState := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevelState.ranges = append(thisLevelState.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	// L0 to L0 的压缩最终都会压缩为一个文件，这大大减少了 L0 层文件数量，减少了读放大
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// getKeyRange 返回一组 sst 文件的 key 范围
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}

	// 找出 tables key 的最大值和最小值
	minKey := tables[0].sst.MinKey()
	maxKey := tables[0].sst.MaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].sst.MinKey(), minKey) < 0 {
			minKey = tables[i].sst.MinKey()
		}
		if utils.CompareKeys(tables[i].sst.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].sst.MaxKey()
		}
	}

	return keyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), 0),
	}
}

// iteratorsReversed 以倒序为 L0 层 sst 文件创建迭代器
func iteratorsReversed(tables []*table, opt *utils.Options) []utils.Iterator {
	out := make([]utils.Iterator, 0, len(tables))
	for i := len(tables) - 1; i >= 0; i-- {
		out = append(out, tables[i].NewIterator(opt))
	}
	return out
}

// updateDiscardStats 更新 vlog 的脏数据
func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
	}
}

// subcompact 将 it 在 kr 范围内的数据压缩到 sst 文件，并写入 tableBuf
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef, inflightBuilders *utils.Throttle, tableBuf chan<- *table) {
	var lastKey []byte

	// 更新 discardStats
	discardStats := make(map[uint32]int64)
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()

	updateStats := func(e *utils.Entry) {
		if e.Meta&utils.BitValuePointer > 0 {
			var vp utils.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.FID] += int64(vp.Len)
		}
	}

	// addKeys 不断向 builder 添加数据
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			isExpired := isDeletedOrExpired(0, it.Item().Entry().ExpiresAt)

			if !utils.SameKey(key, lastKey) {
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// 当前 builder 生成的 sst 文件已经够大了
					break
				}
				lastKey = utils.SafeCopy(lastKey, key)
				// 初始化左边界
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				// 更新 builder 对应 sst 文件右边界
				tableKr.right = lastKey
			}

			switch {
			case isExpired:
				// TODO(qingw1230): 过期数据怎么优化
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
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

		builder := newTableBuilderWithSSTSize(lm.opt, cd.t.fileSz[cd.nextLevel.levelNum])
		addKeys(builder)
		if builder.empty() {
			builder.finish()
			builder.Close()
			continue
		}

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

func (lm *levelManager) checkOverlap(tables []*table, level int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range lm.levels {
		if i < level {
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.Unlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// isDeletedOrExpired 判断有 expiresAt 过期时间的 key 是否可用
func isDeletedOrExpired(_ byte, expiresAt uint64) bool {
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

// compactStatus 压缩状态
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

// overlapsWith 检查指定层在 this 范围是否有压缩计划
func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.Unlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

// delSize 获取指定层处于压缩的 sst 文件总大小
func (cs *compactStatus) delSize(level int) int64 {
	cs.RLock()
	defer cs.Unlock()
	return cs.levels[level].delSize
}

// delete 将指定压缩计划从压缩状态中删除
func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	// 获取当前层与目标层的压缩状态
	thisLevelState := cs.levels[cd.thisLevel.levelNum]
	nextLevelState := cs.levels[cd.nextLevel.levelNum]

	thisLevelState.delSize -= cd.thisSize
	found := thisLevelState.remove(cd.thisRange)
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevelState.remove(cd.nextRange) && found
	}
	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this.String(), tl)
		fmt.Printf("This Level:\n%s\n", thisLevelState.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next.String(), cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevelState.debug())
		log.Fatal("keyRange not found")
	}

	// 将该压缩计划包含的 sst 删除
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables id nil"))
		delete(cs.tables, t.fid)
	}
}

// compareAndAdd 尝试将该压缩计划添加到 cs 中，key 范围没有冲突时才能压缩
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	thisLevelState := cs.levels[cd.thisLevel.levelNum]
	nextLevelState := cs.levels[cd.nextLevel.levelNum]

	if thisLevelState.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevelState.overlapsWith(cd.nextRange) {
		return false
	}

	// 没有冲突的情况下才能压缩
	thisLevelState.ranges = append(thisLevelState.ranges, cd.thisRange)
	nextLevelState.ranges = append(nextLevelState.ranges, cd.nextRange)
	thisLevelState.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

// levelCompactStatus 一层的压缩状态
type levelCompactStatus struct {
	ranges  []keyRange // 当前层处于压缩的 key 范围列表
	delSize int64      // 当前层处于压缩的 sst 文件总大小
}

// overlapsWith 检查是否与当前层处于压缩的 key 范围有重叠
func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

// remove 删除当前层状态指定的 key 范围
func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	newRanges := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			newRanges = append(newRanges, r)
		} else {
			found = true
		}
	}
	lcs.ranges = newRanges
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

// keyRange sst 文件 key 的范围
type keyRange struct {
	left  []byte
	right []byte
	inf   bool // 是否为无穷大的 key 范围
	size  int64
}

// isEmpty 范围是否为空
func (k *keyRange) isEmpty() bool {
	return len(k.left) == 0 && len(k.right) == 0 && !k.inf
}

// infRange 无穷大的 key 范围
var infRange = keyRange{inf: true}

func (k *keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", k.left, k.right, k.inf)
}

func (k *keyRange) equals(dst keyRange) bool {
	return bytes.Equal(k.left, dst.left) &&
		bytes.Equal(k.right, dst.right) &&
		k.inf == dst.inf
}

// extend 扩展保存的 key 范围
func (kr *keyRange) extend(r keyRange) {
	if r.isEmpty() {
		return
	}
	if kr.isEmpty() {
		*kr = r
	}
	if len(kr.left) == 0 || utils.CompareKeys(kr.left, r.left) > 0 {
		kr.left = r.left
	}
	if len(kr.right) == 0 || utils.CompareKeys(kr.right, r.right) < 0 {
		kr.right = r.right
	}
	if r.inf {
		kr.inf = true
	}
}

// overlapsWith 判断 key 范围是否有重叠，kr 为空总是重叠，dst 为空总是不重叠
func (kr *keyRange) overlapsWith(dst keyRange) bool {
	if kr.isEmpty() {
		return true
	}
	if dst.isEmpty() {
		return false
	}
	if kr.inf || dst.inf {
		return true
	}

	if utils.CompareKeys(kr.left, dst.right) > 0 {
		return false
	}
	if utils.CompareKeys(kr.right, dst.left) < 0 {
		return false
	}
	return true
}
