package utils

import "hash/crc32"

const (
	BitDelete       byte = 1 << 0 // key 被删除时设置
	BitValuePointer byte = 1 << 1 // value 存储到 vlog 文件时设置
)

// codec
var (
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
