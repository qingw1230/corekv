package cache

import (
	"math/rand"
	"time"
)

const (
	// cmWidth hash 函数个数
	cmWidth = 4
)

// cmSketch 用于近似频率估计
type cmSketch struct {
	rows [cmWidth]cmRow
	seed [cmWidth]uint64
	mask uint64 // & 该值获取位置
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}

	// 将数量提升到 2 的幂次方是为了将 % 运算转换为 & 运算
	numCounters = next2Power(numCounters)
	sketch := cmSketch{
		mask: uint64(numCounters - 1),
	}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < cmWidth; i++ {
		sketch.rows[i] = newCmRow(numCounters)
		sketch.seed[i] = source.Uint64()
	}

	return &sketch
}

// Increment 增加 hashed 的计数
func (c *cmSketch) Increment(hashed uint64) {
	for i := range c.rows {
		c.rows[i].increment((hashed ^ c.seed[i]) & c.mask)
	}
}

// Estimate 获取计数的近似值（各个 hash 计算出的最小值）
func (c *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range c.rows {
		val := c.rows[i].get((hashed & c.seed[i]) & c.mask)
		if val < min {
			min = val
		}
	}
	return int64(min)
}

// Reset 将计数减半
func (c *cmSketch) Reset() {
	for _, r := range c.rows {
		r.reset()
	}
}

// Clear 清空计数
func (c *cmSketch) Clear() {
	for _, r := range c.rows {
		r.clear()
	}
}

// next2Power 获取 >=x 的最小的 2 的幂次方
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

type cmRow []byte

// newCmRow 为每个计数器分配 4 bits
func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

// get 获取指定位置计数
func (c cmRow) get(n uint64) byte {
	// n/2 定位到相应字节
	// (n & 1) * 4 奇数为 4 偶数为 0
	return c[n/2] >> ((n & 1) * 4) & 0x0f
}

// increment 增加指定位置计数
func (c cmRow) increment(n uint64) {
	i := n / 2
	s := (n & 1) * 4
	v := (c[i] >> s) & 0x0f
	if v < 15 {
		c[i] += 1 << s
	}
}

// reset 将值减半
func (c cmRow) reset() {
	for i := range c {
		// >> 1 将低 4 位减半
		// & 0x77 将高 4 位减半，同时去掉高位右移的影响
		c[i] = (c[i] >> 1) & 0x77
	}
}

// clear 将值置 0
func (c cmRow) clear() {
	for i := range c {
		c[i] = 0
	}
}
