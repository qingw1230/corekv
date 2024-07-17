package utils

import "sync"

type Closer struct {
	waiting     sync.WaitGroup
	closeSignal chan struct{}
}

func NewCloser(i int) *Closer {
	c := &Closer{
		waiting: sync.WaitGroup{},
	}
	c.waiting.Add(i)
	c.closeSignal = make(chan struct{})
	return c
}

// Close 关闭，真正调用 wg.Wait()
func (c *Closer) Close() {
	close(c.closeSignal)
	c.waiting.Wait()
}

func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}

func (c *Closer) Done() {
	c.waiting.Done()
}

// Wait 返回用于关闭的 channel
func (c *Closer) Wait() chan struct{} {
	return c.closeSignal
}
