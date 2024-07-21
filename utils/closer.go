package utils

import "sync"

type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{} // 关闭信号
}

func NewCloser() *Closer {
	c := &Closer{
		waiting: sync.WaitGroup{},
	}
	c.CloseSignal = make(chan struct{})
	return c
}

// Close 关闭，真正调用 wg.Wait()
func (c *Closer) Close() {
	close(c.CloseSignal)
	c.waiting.Wait()
}

// Done 标识协程已完成资源回收，通知上游
func (c *Closer) Done() {
	c.waiting.Done()
}

func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}
