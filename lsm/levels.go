package lsm

type levelManager struct {
	lsm    *LSM // 所属 LSM
	opt    *Options
	maxFID uint64 // 用于生成文件 ID

}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		opt: opt,
		lsm: lsm,
	}
	return lm
}
