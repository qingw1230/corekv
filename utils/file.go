package utils

import "strings"

// FID 根据文件名（00001.sst）获取其 fid
func FID(name string) string {
	return strings.Split(name, ".")[0]
}
