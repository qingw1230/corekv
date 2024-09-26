package utils

import (
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

// RandStringRandomLength 生成随机长度的字符串，len 为原始字符串最大长度
func RandStringRandomLength(len int, appendTimestamp bool) string {
	len = Intn(len) + 1
	return RandStringWithLength(len, appendTimestamp)
}

// RandStringWithLength 生成指定长度的字符串
func RandStringWithLength(len int, appendTimestamp bool) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := Intn(26) + 65
		bytes[i] = byte(b)
	}

	str := string(bytes)

	if appendTimestamp {
		now := time.Now().UnixNano()
		timestampStr := strconv.FormatInt(now, 10)
		str += timestampStr
	}
	return str
}

func Int63n(n int64) int64 {
	mu.Lock()
	defer mu.Unlock()
	return r.Int63n(n)
}

// Intn 并发安全的获取随机数
func Intn(n int) int {
	mu.Lock()
	defer mu.Unlock()
	return r.Intn(n)
}

func Float64() float64 {
	mu.Lock()
	defer mu.Unlock()
	return r.Float64()
}
