package lsm

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/qingw1230/corekv/utils"
)

type Iterator struct {
	it    utils.Item
	iters []utils.Iterator
}

type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}

// NewIterator 创建迭代器
func (lsm *LSM) NewIterator(opt *utils.Options) utils.Iterator {
	iter := &Iterator{}
	iter.iters = make([]utils.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))
	for _, imm := range lsm.immutables {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.lm.NewIterator(opt))
	return iter
}

func (iter *Iterator) Next() {
	iter.iters[0].Next()
}

func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}

func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}

func (iter *Iterator) Item() utils.Item {
	return iter.iters[0].Item()
}

func (iter *Iterator) Close() error {
	return nil
}

func (iter *Iterator) Seek(key []byte) {
}

// memIterator 内存表迭代器，内部保存跳表迭代器
type memIterator struct {
	skipIter utils.Iterator
}

func (m *memTable) NewIterator(opt *utils.Options) utils.Iterator {
	return &memIterator{skipIter: m.sl.NewIterator()}
}

func (iter *memIterator) Next() {
	iter.skipIter.Next()
}

func (iter *memIterator) Valid() bool {
	return iter.skipIter.Valid()
}

func (iter *memIterator) Rewind() {
	iter.skipIter.Rewind()
}

func (iter *memIterator) Item() utils.Item {
	return iter.skipIter.Item()
}

func (iter *memIterator) Close() error {
	return iter.skipIter.Close()
}

func (iter *memIterator) Seek(key []byte) {
}

// levelIterator levelManager 上的迭代器
type levelIterator struct {
	it    *utils.Item
	iters []*Iterator
}

func (lm *levelManager) NewIterator(options *utils.Options) utils.Iterator {
	return &levelIterator{}
}

func (iter *levelIterator) Next() {
}

func (iter *levelIterator) Valid() bool {
	return false
}

func (iter *levelIterator) Rewind() {

}

func (iter *levelIterator) Item() utils.Item {
	return &Item{}
}

func (iter *levelIterator) Close() error {
	return nil
}

func (iter *levelIterator) Seek(key []byte) {
}

// ConcatIterator 将一组 sst 文件放到一个迭代器遍历
type ConcatIterator struct {
	idx     int              // 目前活跃迭代器下标
	cur     utils.Iterator   // 目前在操纵的迭代器
	iters   []utils.Iterator // 对应 sst 文件的迭代器
	tables  []*table         // 待遍历的 sst 文件
	options *utils.Options   // 有效选项是 REVERSED 和 NOCACHE
}

// NewConcatIterator 用于遍历一组 sst 文件
func NewConcatIterator(tables []*table, opt *utils.Options) *ConcatIterator {
	return &ConcatIterator{
		idx:     -1,
		iters:   make([]utils.Iterator, len(tables)),
		tables:  tables,
		options: opt,
	}
}

// setIdx 将 cur 指向 tables[idx] 的迭代器
func (ci *ConcatIterator) setIdx(idx int) {
	ci.idx = idx
	if idx < 0 || idx >= len(ci.iters) {
		ci.cur = nil
		return
	}
	if ci.iters[idx] == nil {
		ci.iters[idx] = ci.tables[idx].NewIterator(ci.options)
	}
	ci.cur = ci.iters[idx]
}

func (ci *ConcatIterator) Rewind() {
	if len(ci.iters) == 0 {
		return
	}
	// 先指向正确的 sst 文件
	if ci.options.IsAsc {
		ci.setIdx(0)
	} else {
		ci.setIdx(len(ci.iters) - 1)
	}
	// 再指向 sst 文件内正确的 key 位置
	ci.cur.Rewind()
}

func (c *ConcatIterator) Valid() bool {
	return c.cur != nil && c.cur.Valid()
}

func (c *ConcatIterator) Item() utils.Item {
	return c.cur.Item()
}

// Seek 利用二分寻找指定 key
func (ci *ConcatIterator) Seek(key []byte) {
	var idx int
	// 先看 key 在哪个 sst 文件
	if ci.options.IsAsc {
		idx = sort.Search(len(ci.tables), func(i int) bool {
			return utils.CompareKeys(ci.tables[i].sst.MaxKey(), key) >= 0
		})
	} else {
		n := len(ci.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return utils.CompareKeys(ci.tables[n-1-i].sst.MinKey(), key) <= 0
		})
	}
	if idx >= len(ci.tables) || idx < 0 {
		ci.setIdx(-1)
		return
	}

	ci.setIdx(idx)
	ci.cur.Seek(key)
}

func (ci *ConcatIterator) Next() {
	ci.cur.Next()
	if ci.cur.Valid() {
		return
	}
	// 当前 sst 文件已遍历完了，遍历下一个
	for {
		if ci.options.IsAsc {
			ci.setIdx(ci.idx + 1)
		} else {
			ci.setIdx(ci.idx - 1)
		}
		// 所有 sst 文件都遍历完了
		if ci.cur == nil {
			return
		}
		ci.cur.Rewind()
		if ci.cur.Valid() {
			break
		}
	}
}

func (c *ConcatIterator) Close() error {
	for _, it := range c.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil {
			return fmt.Errorf("ConcatIterator:%+v", err)
		}
	}
	return nil
}

// MergeIterator 多路合并迭代器
type MergeIterator struct {
	left  node  // 左迭代器
	right node  // 右迭代器
	small *node // 正向遍历时，指向 left、right 较小的呢个值

	curKey  []byte // 当前指向的 key
	reverse bool   // 是否反向
}

// node 持有迭代器和当前遍历到的 Entry
type node struct {
	valid  bool // 当前指向是否有效
	entry  *utils.Entry
	iter   utils.Iterator
	merge  *MergeIterator
	concat *ConcatIterator
}

// NewMergeIterator 以 iters 为左右节点构建一棵二叉树
func NewMergeIterator(iters []utils.Iterator, reverse bool) utils.Iterator {
	switch len(iters) {
	case 0:
		return &Iterator{}
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// 此时 small 是随机指向的，当使用 rewind/seek 时，该问题将得到修复
		mi.small = &mi.left
		return mi
	}

	// 超过 2 个节点时，递归构建左右子树
	mid := len(iters) / 2
	return NewMergeIterator(
		[]utils.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}

// setIterator 将 node 相关变量设为 iter
func (n *node) setIterator(iter utils.Iterator) {
	n.iter = iter
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}

// setKey 根据迭代器设置 entry
func (n *node) setKey() {
	switch {
	case n.merge != nil:
		n.valid = n.merge.small.valid
		if n.valid {
			n.entry = n.merge.small.entry
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		if n.valid {
			n.entry = n.concat.Item().Entry()
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.entry = n.iter.Item().Entry()
		}
	}
}

// next 遍历下一个 key
func (n *node) next() {
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setKey()
}

func (n *node) rewind() {
	n.iter.Rewind()
	n.setKey()
}

func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}

// fix 修正 mi.small 指向
func (mi *MergeIterator) fix() {
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}

	cmp := utils.CompareKeys(mi.small.entry.Key, mi.bigger().entry.Key)
	switch {
	case cmp == 0:
		// 如果 key 相同，则将右侧迭代器向前移动
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		return
	case cmp < 0:
		// small 当前指向的是更小的
		if mi.reverse {
			mi.swapSmall()
		}
		return
	default:
		// small 当前指向的是更大的
		if !mi.reverse {
			mi.swapSmall()
		}
		return
	}
}

// bigger 获取左右节点更大的呢个
func (mi *MergeIterator) bigger() *node {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

// swapSmall 将 small 指向另一边
func (mi *MergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
		return
	}
	if mi.small == &mi.right {
		mi.small = &mi.left
		return
	}
}

func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.entry.Key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

// setCurrent 将 mi.curKey 更新为 small 所指向的 key
func (mi *MergeIterator) setCurrent() {
	utils.CondPanic(mi.small.entry == nil && mi.small.valid, fmt.Errorf("mi.small.entry is nil"))
	if mi.small.valid {
		mi.curKey = append(mi.curKey[:0], mi.small.entry.Key...)
	}
}

// Rewind 定位到遍历所需的第一个 key
func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

// Seek 递归地定位到 >= key 的位置
func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

func (mi *MergeIterator) Valid() bool {
	return mi.small.valid
}

func (mi *MergeIterator) Item() utils.Item {
	return mi.small.iter.Item()
}

func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return utils.WarpErr("MergeIterator", err1)
	}
	return utils.WarpErr("MergeIterator", err2)
}
