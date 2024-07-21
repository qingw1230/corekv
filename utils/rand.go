package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

func Int63n(n int64) int64 {
	mu.Lock()
	defer mu.Unlock()
	return r.Int63n(n)
}

func RandN(n int) int {
	mu.Lock()
	defer mu.Unlock()
	return r.Intn(n)
}

func Float64() float64 {
	mu.Lock()
	defer mu.Unlock()
	return r.Float64()
}

// randStr 生成指定长度的随机字符串
func randStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®😁😭🉑️🐂㎡"
	bytes := []byte(str)
	res := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		res = append(res, bytes[rand.Intn(len(bytes))])
	}
	return string(res)
}

// BuildEntry 构建随机 entry 对象
func BuildEntry() *Entry {
	rand.Seed(time.Now().Unix())
	key := []byte(fmt.Sprintf("%s%s", randStr(16), "12345678"))
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}
