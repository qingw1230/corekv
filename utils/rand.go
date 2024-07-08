package utils

import (
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
