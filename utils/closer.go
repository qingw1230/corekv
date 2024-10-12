package utils

import "sync"

type Closer struct {
	wg          sync.WaitGroup
	CloseSignal chan struct{}
}

func NewCloser() *Closer {
	c := &Closer{
		wg: sync.WaitGroup{},
	}
	c.CloseSignal = make(chan struct{})
	return c
}

func (c *Closer) Add(n int) {
	c.wg.Add(n)
}

func (c *Closer) Done() {
	c.wg.Done()
}

func (c *Closer) Close() {
	close(c.CloseSignal)
	c.wg.Wait()
}
