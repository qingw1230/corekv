package cache

import (
	"container/list"
)

// segmentedLRU 分段 LRU
type segmentedLRU struct {
	data                      map[uint64]*list.Element
	stageColdCap, stageHotCap int
	stageCold, stageHot       *list.List
}

const (
	STAGE_COLD = iota
	STAGE_HOT
	STAGE_WINDOW
)

func newSLRU(data map[uint64]*list.Element, coldCap, hotCap int) *segmentedLRU {
	return &segmentedLRU{
		data:         data,
		stageColdCap: coldCap,
		stageHotCap:  hotCap,
		stageCold:    list.New(),
		stageHot:     list.New(),
	}
}

func (sl *segmentedLRU) add(newitem storeItem) {
	newitem.stage = STAGE_COLD

	// 主缓存还没满，直接插入
	if sl.stageCold.Len() < sl.stageColdCap || sl.Len() < sl.stageColdCap+sl.stageHotCap {
		sl.data[newitem.key] = sl.stageCold.PushFront(&newitem)
		return
	}

	// 满了的话淘汰非热门链表的最后一个
	e := sl.stageCold.Back()
	item := e.Value.(*storeItem)
	delete(sl.data, item.key)
	*item = newitem

	sl.data[item.key] = e
	sl.stageCold.MoveToFront(e)
}

func (sl *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	// 已经在热门区域了，提到链表头部
	if item.stage == STAGE_HOT {
		sl.stageHot.MoveToFront(v)
		return
	}

	// 本来在非热门区域，热门区域还有空，将其提升到热门区域
	if sl.stageHot.Len() < sl.stageHotCap {
		sl.stageCold.Remove(v)
		item.stage = STAGE_HOT
		sl.data[item.key] = sl.stageHot.PushFront(item)
		return
	}

	// 此时既不在热门区域，热门区域也没空了，需要将热门区域最后一个下放到非热门区域
	back := sl.stageHot.Back()
	bitem := back.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGE_HOT
	item.stage = STAGE_COLD

	sl.data[bitem.key] = back
	sl.data[item.key] = v

	sl.stageHot.MoveToFront(back)
	sl.stageCold.MoveToFront(v)
}

func (s *segmentedLRU) Len() int {
	return s.stageHot.Len() + s.stageCold.Len()
}

// victim 获取将要淘汰的数据
func (s *segmentedLRU) victim() *storeItem {
	if s.Len() < s.stageColdCap+s.stageHotCap {
		return nil
	}

	v := s.stageCold.Back()
	return v.Value.(*storeItem)
}
