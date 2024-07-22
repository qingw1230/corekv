package lsm

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/qingw1230/corekv/utils"
)

var (
	opt = &Options{
		WorkDir:             "../work_test",
		SSTableMaxSz:        1024,
		MemTableSize:        1024,
		BlockSize:           1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       3,
	}
)

func TestBase(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	test := func() {
		baseTest(t, lsm, 128)
	}
	runTest(1, test)
}

func TestRecovery(t *testing.T) {
	clearDir()
	recovery := func() {
		lsm := buildLSM()
		baseTest(t, lsm, 10)
	}
	// 允许两次就能实现恢复
	runTest(3, recovery)
}

func TestRecovery2(t *testing.T) {
	recovery := func() {
		// 每次运行都是相当于意外重启
		lsm := buildLSM()
		// 测试正确性
		baseTest(t, lsm, 10)
	}
	runTest(3, recovery)
}

func TestClose(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	lsm.StartCompacter()
	test := func() {
		baseTest(t, lsm, 128)
		utils.Err(lsm.Close())
		lsm = buildLSM()
		baseTest(t, lsm, 128)
	}
	runTest(1, test)
}

func TestHitStorage(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	e := utils.BuildEntry()
	lsm.Set(e)
	hitMemtable := func() {
		v, err := lsm.memTable.Get(e.Key)
		utils.Err(err)
		utils.CondPanic(!bytes.Equal(v.Value, e.Value), fmt.Errorf("[hitMemtable] !equal(v.Value, e.Value)"))
	}
	hitL0 := func() {
		baseTest(t, lsm, 128)
	}
	hitNotL0 := func() {
		lsm.lm.runOnce(0)
		baseTest(t, lsm, 128)
	}
	hitBloom := func() {
		ee := utils.BuildEntry()
		v, err := lsm.lm.levels[0].tables[0].Search(ee.Key, &ee.Version)
		utils.CondPanic(v != nil, fmt.Errorf("[hitBloom] v != nil"))
		utils.CondPanic(err != utils.ErrKeyNotFound, fmt.Errorf("[hitBloom] err != utils.ErrKeyNotFound"))
	}

	runTest(1, hitMemtable, hitL0, hitNotL0, hitBloom)
}

func TestPsarameter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	testNil := func() {
		utils.CondPanic(lsm.Set(nil) != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
		_, err := lsm.Get(nil)
		utils.CondPanic(err != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
	}
	runTest(1, testNil)
}

func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	ok := false
	l0TOLMax := func() {
		baseTest(t, lsm, 128)
		fid := lsm.lm.maxFID + 1
		lsm.lm.runOnce(1)
		for _, t := range lsm.lm.levels[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[l0TOLMax] fid not found"))
	}
	l0ToL0 := func() {
		baseTest(t, lsm, 128)
		fid := lsm.lm.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 0)
		tricky(cd.thisLevel.tables)
		ok := lsm.lm.fillTablesL0ToL0(cd)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.lm.runCompactDef(0, 0, *cd)
		lsm.lm.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.lm.levels[0].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}
	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.lm.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		tricky(cd.thisLevel.tables)
		ok := lsm.lm.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.lm.runCompactDef(0, 0, *cd)
		lsm.lm.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.lm.levels[1].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		fid := lsm.lm.maxFID + 1
		cd := buildCompactDef(lsm, 6, 6, 6)
		tricky(cd.thisLevel.tables)
		ok := lsm.lm.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.lm.runCompactDef(0, 6, *cd)
		lsm.lm.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.lm.levels[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] fid not found"))
	}
	parallerCompact := func() {
		baseTest(t, lsm, 128)
		cd := buildCompactDef(lsm, 0, 0, 1)
		tricky(cd.thisLevel.tables)
		ok := lsm.lm.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[parallerCompact] lsm.levels.fillTables(cd) ret == false"))
		go lsm.lm.runCompactDef(0, 0, *cd)
		lsm.lm.runCompactDef(0, 0, *cd)
		isParaller := false
		for _, state := range lsm.lm.compactState.levels {
			if len(state.ranges) != 0 {
				isParaller = true
			}
		}
		utils.CondPanic(!isParaller, fmt.Errorf("[parallerCompact] not is paralle"))
	}
	runTest(1, l0TOLMax, l0ToL0, nextCompact, maxToMax, parallerCompact)
}

func baseTest(t *testing.T, lsm *LSM, n int) {
	e := &utils.Entry{
		Key:       []byte("CRTS😁暗算MrGSBtL12345678"),
		Value:     []byte("我草了"),
		ExpiresAt: 0,
	}

	lsm.Set(e)
	for i := 1; i < n; i++ {
		ee := utils.BuildEntry()
		lsm.Set(ee)
	}
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
}

func buildLSM() *LSM {
	c := make(chan map[uint32]int64, 16)
	opt.DiscardStatsCh = &c
	lsm := NewLSM(opt)
	return lsm
}

func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for i := 0; i < n; i++ {
			f()
		}
	}
}

func buildCompactDef(lsm *LSM, id, thisLevel, nextLevel int) *compactDef {
	t := targets{
		targetSz:  []int64{0, 10485760, 10485760, 10485760, 10485760, 10485760, 10485760},
		fileSz:    []int64{1024, 2097152, 2097152, 2097152, 2097152, 2097152, 2097152},
		baseLevel: nextLevel,
	}
	def := &compactDef{
		compactID: id,
		thisLevel: lsm.lm.levels[thisLevel],
		nextLevel: lsm.lm.levels[nextLevel],
		t:         t,
		p:         buildCompactionPriority(lsm, thisLevel, t),
	}
	return def
}

func buildCompactionPriority(lsm *LSM, thisLevel int, t targets) compactionPriority {
	return compactionPriority{
		level:    thisLevel,
		score:    8.6,
		adjusted: 860,
		t:        t,
	}
}

func tricky(tables []*table) {
	for _, table := range tables {
		table.sst.Indexs().StaleDataSize = 10 << 20
		t, _ := time.Parse("2006-01-02 15:04:05", "1995-08-10 00:00:00")
		table.sst.SetCreatedAt(&t)
	}
}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
