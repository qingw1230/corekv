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

func (c *Closer) Close() {
	close(c.closeSignal)
	c.waiting.Wait()
}

func (c *Closer) Done() {
	c.waiting.Done()
}

func (c *Closer) Wait() chan struct{} {
	return c.closeSignal
}
