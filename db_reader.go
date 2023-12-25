package nimbusdb

import (
    "sync"
    "github.com/google/btree"
)

// DbReader is used to iterate over key-value pairs in the database.
type DbReader struct {
    db          *Db
    currentItem btree.Item
    mu          sync.Mutex
}

// NewDbReader creates a new DbReader for the given database.
func NewDbReader(db *Db) *DbReader {
    return &DbReader{
        db: db,
    }
}

// Read fetches up to n key-value pairs from the database.
func (r *DbReader) Read(n int) ([]*KeyValuePair, error) {
    r.mu.Lock()
    defer r.mu.Unlock()

    var result []*KeyValuePair
    var lastItem btree.Item = nil

    processItem := func(i btree.Item) bool {
        if len(result) >= n {
            return false
        }
        it := i.(*item)
        kv := &KeyValuePair{
            Key:   it.key,
            Value: it.v,
        }
        result = append(result, kv)
        lastItem = i
        return true
    }

    // If currentItem is nil, start from the beginning
    if r.currentItem == nil {
        r.db.keyDir.tree.Ascend(processItem)
    } else {
        // Continue from the current item
        r.db.keyDir.tree.AscendGreaterOrEqual(r.currentItem, processItem)
    }

    if lastItem != nil {
        r.currentItem = lastItem
    }

    return result, nil
}
