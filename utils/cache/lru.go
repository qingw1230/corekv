package cache

import "container/list"

type windowLRU struct {
	data map[uint64]*list.Element // key 到相应元素的映射
	cap  int                      // w-lru 容量
	list *list.List
}

// storeItem list.Element.Value 存储的元素类型
type storeItem struct {
	stage    int // 所处位置
	key      uint64
	conflict uint64 // 用于冲突时检测
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

// add 向 windowLRU 添加数据
func (wl *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	// 没满直接插入
	if wl.list.Len() < wl.cap {
		wl.data[newitem.key] = wl.list.PushFront(&newitem)
		return storeItem{}, false
	}

	// 满了的话淘汰链表最后一个
	evictItem := wl.list.Back()
	item := evictItem.Value.(*storeItem)

	delete(wl.data, item.key)
	// 交换后 eitem 里面是要淘汰的数据，item 里面是新插入的数据
	eitem, *item = *item, newitem

	wl.data[item.key] = evictItem
	wl.list.MoveToFront(evictItem)
	return eitem, true
}

func (w *windowLRU) get(v *list.Element) {
	w.list.MoveToFront(v)
}
