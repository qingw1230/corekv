package lsm

type levelManager struct {
	maxFID uint64 // 用于生成文件 ID
	lsm    *LSM   // 所属 LSM
}
