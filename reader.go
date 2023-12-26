package nimbusdb

import (
	"strings"

	"github.com/google/btree"
)

// KeyReader iterates through each key matching given prefix.
// If prefix is an empty string, all keys are matched.
// The second argument is a callback function which contains the key.
func (db *Db) KeyReader(prefix string, handler func(k []byte)) {
	db.keyDir.tree.Ascend(func(it btree.Item) bool {
		key := it.(*item).key
		if strings.HasPrefix(string(key), prefix) {
			handler(key)
		}
		return true
	})
}

// KeyValueReader iterates through each key-value pair matching given key's prefix.
// If prefix is an empty string, all key-value pairs are matched.
// The second argument is a callback function which contains key and the value.
func (db *Db) KeyValueReader(keyPrefix string, handler func(k []byte, v []byte)) (bool, error) {
	db.keyDir.tree.Ascend(func(it btree.Item) bool {
		key := it.(*item).key
		if strings.HasPrefix(string(key), keyPrefix) {
			v, err := db.getKeyDir(key)
			if err != nil {
				return false
			}
			handler(it.(*item).key, v.v)
		}
		return true
	})
	return false, nil
}
