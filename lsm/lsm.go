package lsm

import (
	"github.com/qingw1230/corekv/utils"
)

type Options struct {
	WorkDir            string
	MemTableSize       int64 // 跳表大小限制，同时作为 L0 层 sst 文件大小限制
	SSTableMaxSz       int64
	BlockSize          int // sst 文件中每个块的大小
	BloomFalsePositive float64

	NumCompactors       int   // 执行 compact 的协程数
	BaseLevelSize       int64 // 层大小基础大小，默认 10G
	LevelSizeMultiplier int   // 各层大小比例，默认为 10
	BaseTableSize       int64 // sst 的基础大小
	TableSizeMultiplier int   // 各层 sst 文件大小比例
	NumLevelZeroTables  int   // L0 层 sst 文件数最大值，默认 15
	MaxLevelNum         int   // 最大层编号

	DiscardStatsCh *chan map[uint32]int64 // vlog 统计脏数据的 channel
}

type LSM struct {
	option     *Options
	memTable   *memTable
	immutables []*memTable
	lm         *levelManager
	closer     *utils.Closer
	maxMemFID  uint32
}

// NewLSM 创建一个 LSM 树，并根据 wal 文件恢复内存表
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	// 初始化 levelManager
	lsm.lm = lsm.initLevelManager(opt)
	lsm.memTable, lsm.immutables = lsm.recovery()
	lsm.closer = utils.NewCloser()
	return lsm
}

func (lsm *LSM) Close() error {
	lsm.closer.Close()
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.lm.close(); err != nil {
		return err
	}
	return nil
}

// StartCompacter 在后台启动用于压缩的协程
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.lm.runCompacter(i)
	}
}

func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 优雅关闭
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	if int64(lsm.memTable.wal.Size())+int64(utils.EstimateWalCodecSize(entry)) > lsm.option.MemTableSize {
		lsm.Rotate()
	}
	if err = lsm.memTable.set(entry); err != nil {
		return err
	}
	for _, immutable := range lsm.immutables {
		if err = lsm.lm.flush(immutable); err != nil {
			return err
		}
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	var (
		entry *utils.Entry
		err   error
	)
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	return lsm.lm.Get(key)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.SkipList {
	return lsm.memTable.sl
}

// Rotate 将 memTable 添加到不可变表，创建新的 memTable
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemTable()
}
