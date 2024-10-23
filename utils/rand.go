package utils

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

const (
	timestampLen = 8
)

// BuildEntry 构建一个随机的 Entry 对象
func BuildEntry() *Entry {
	key := []byte(RandStringWithLength(16, true))
	value := []byte(RandStringWithLength(128, false))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func GenerateEntries(cnt int) []*Entry {
	strs := GenerateStrs(cnt)
	table := make([]*Entry, cnt)
	for i := 0; i < cnt; i++ {
		e := &Entry{
			Key:   []byte(strs[i]),
			Value: []byte(strs[i]),
		}
		table[i] = e
	}
	return table
}

func GenerateStrs(cnt int) []string {
	strs := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		str := RandStringRandomLength(100, true)
		strs[i] = str
	}
	return strs
}

// RandStringRandomLength 生成随机长度的字符串，len 为原始字符串最大长度
func RandStringRandomLength(len int, appendTimestamp bool) string {
	len = Intn(len) + 1
	return RandStringWithLength(len, appendTimestamp)
}

// RandStringWithLength 生成指定长度的字符串，timestamp 为纳秒级时间戳
func RandStringWithLength(len int, appendTimestamp bool) string {
	bytes := make([]byte, len+timestampLen)
	for i := 0; i < len; i++ {
		b := Intn(26) + 65
		bytes[i] = byte(b)
	}

	if appendTimestamp {
		now := time.Now().UnixNano()
		binary.BigEndian.PutUint64(bytes[len:], uint64(now))
	}
	return string(bytes)
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
