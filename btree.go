package nimbusdb

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/manosriram/nimbusdb/utils"
)

type BlockOffsetPair struct {
	startOffset int64
	endOffset   int64
	filePath    string
}

type BTree struct {
	tree         *btree.BTree
	blockOffsets map[int64]BlockOffsetPair
	mu           sync.RWMutex
}

type item struct {
	key []byte
	v   KeyDirValue
}

func (it item) Less(i btree.Item) bool {
	return bytes.Compare(it.key, i.(*item).key) < 0
}

func (b *BTree) GetBlockNumber(key []byte) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	i := b.tree.Get(&item{key: key})
	if i == nil {
		return -1
	}
	return i.(*item).v.blockNumber
}

func (b *BTree) Get(key []byte) (*KeyDirValue, []*KeyDirValue) {
	i := b.tree.Get(&item{key: key})
	if i == nil {
		return nil, nil
	}
	// fmt.Println("i = ", i)
	keyItem := i.(*item)

	v := make([]*KeyDirValue, 0)
	b.tree.Ascend(func(it btree.Item) bool {
		itm := it.(*item)
		if itm.v.blockNumber == keyItem.v.blockNumber {
			v = append(v, &itm.v)
		}
		return true
	})
	return &keyItem.v, v
}

func (b *BTree) Set(key []byte, value KeyDirValue) *KeyDirValue {
	i := b.tree.ReplaceOrInsert(&item{key: key, v: value})
	if i != nil {
		return &i.(*item).v
	}
	y := b.blockOffsets[value.blockNumber]
	fmt.Println("s = ", y.startOffset)
	if y.startOffset == 0 {
		y.startOffset = value.offset
		y.filePath = value.path
		b.blockOffsets[value.blockNumber] = y
		// x.Store(value.offset)
		// b.blockOffsets[value.blockNumber].startOffset = x

		fmt.Printf("block = %d, path = %s, start offset = %d\n", value.blockNumber, value.path, value.offset)
	} else {
		y.endOffset = value.offset
		b.blockOffsets[value.blockNumber] = y
		// x := b.blockOffsets[value.blockNumber].endOffset
		// x.Store(value.offset)
		// b.blockOffsets[value.blockNumber].endOffset = x
		fmt.Printf("block = %d, path = %s, end offset = %d\n", value.blockNumber, value.path, value.offset)
	}

	return nil
}

func (b *BTree) Delete(key []byte) *KeyValuePair {
	i := b.tree.Delete(&item{key: key})
	if i != nil {
		x := i.(*item)
		return &KeyValuePair{
			Key:   x.key,
			Value: x.v,
		}
	}
	return nil
}

func (b *BTree) List() []*KeyValuePair {
	var pairs []*KeyValuePair
	b.tree.Ascend(func(it btree.Item) bool {
		pairs = append(pairs, &KeyValuePair{
			Key:   it.(*item).key,
			Value: it.(*item).v,
			Ttl:   utils.TimeUntilUnixNano(it.(*item).v.tstamp),
		})
		return true
	})
	return pairs
}
