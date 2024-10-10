package lsm

import "github.com/qingw1230/corekv/utils"

type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}
