package nimbusdb

import (
	"bytes"
	"strings"
	"sync"

	"github.com/google/btree"
	"github.com/manosriram/nimbusdb/utils"
)

type BTree struct {
	tree *btree.BTree
	mu   sync.RWMutex
}

type item struct {
	key      []byte
	v        KeyDirValue
	revision int64
}

func (i item) Less(other btree.Item) bool {
	otherItem := other.(*item)

	if bytes.Equal(i.key, otherItem.key) {
		switch i.revision {
		case 0:
			return i.revision > otherItem.revision
		default:
			return i.revision < otherItem.revision
		}
	}
	return bytes.Compare(i.key, otherItem.key) < 0

}

func (b *BTree) Get(key []byte) *KeyDirValue {
	i := b.tree.Get(&item{key: key})
	if i == nil {
		return nil
	}
	v := &i.(*item).v
	return v
}

func (b *BTree) GetUsingRevision(key []byte, revision int64) *KeyDirValue {
	i := b.tree.Get(&item{key: key, revision: revision})
	if i == nil {
		return nil
	}
	v := &i.(*item).v
	return v
}

func (b *BTree) Set(key []byte, value KeyDirValue) *KeyDirValue {
	i := b.tree.ReplaceOrInsert(&item{key: key, v: value, revision: value.revision})
	if i != nil {
		return &i.(*item).v
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

func (b *BTree) AscendWithPrefix(prefix string, handler func(k []byte)) {
	b.tree.Ascend(func(it btree.Item) bool {
		key := it.(*item).key
		if strings.HasPrefix(string(key), prefix) {
			handler(key)
		}
		return true
	})
}
