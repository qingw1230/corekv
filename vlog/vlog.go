package vlog

import (
	"github.com/qingw1230/corekv/utils"
)

type Options struct {
}

type VLog struct {
	closer *utils.Closer
}

func (v *VLog) Close() error {
	return nil
}

func NewVLog(opt *Options) *VLog {
	v := &VLog{}
	v.closer = utils.NewCloser(1)
	return v
}

func (v *VLog) StartGC() {
	defer v.closer.Done()
	for {
		select {
		case <-v.closer.Wait():
			return
		}
	}
}

func (v *VLog) Set(entry *utils.Entry) error {
	return nil
}

func (v *VLog) Get(entry *utils.Entry) (*utils.Entry, error) {
	return nil, nil
}
