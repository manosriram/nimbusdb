package nimbusdb

import (
	"bytes"
	"time"

	"github.com/manosriram/nimbusdb/utils"
)

type Batch struct {
	size       int64
	db         *Db
	writeQueue []*KeyValuePair
}

func (db *Db) NewBatch() *Batch {
	return &Batch{
		db:   db,
		size: 5,
	}
}

func (b *Batch) Get(k []byte) ([]byte, error) {
	index := -1
	var record *KeyValuePair
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			record = b.writeQueue[i]
			index = i
			break
		}
	}

	if index != -1 { // key found in write queue
		return utils.Encode(record.Value), nil
	}

	v, err := b.db.getKeyDir(k) // else, search datafiles
	if err != nil {
		return nil, err
	}
	return v.v, nil
}

func (b *Batch) Set(k []byte, v []byte) error {
	// var record *KeyValuePair
	index := -1
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			// record = b.writeQueue[i]
			index = i
			break
		}
	}
	if index != -1 {
		b.writeQueue[index] = &KeyValuePair{
			Key:   k,
			Value: v,
			Ttl:   24 * time.Hour,
		}
	} else {
		b.writeQueue = append(b.writeQueue, &KeyValuePair{
			Key:   k,
			Value: v,
			Ttl:   24 * time.Hour,
		})
	}
	return nil
}

func (b *Batch) SetWithTTL(k []byte, v []byte, ttl time.Duration) error {
	index := -1
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			index = i
			break
		}
	}
	kv := &KeyValuePair{
		Key:   k,
		Value: v,
		Ttl:   ttl,
	}

	if index != -1 {
		b.writeQueue[index] = kv
	} else {
		b.writeQueue = append(b.writeQueue, kv)
	}
	return nil
}

func (b *Batch) Commit() error {
	for i := range b.writeQueue {
		_, err := b.db.Set(b.writeQueue[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) Rollback() error {
	b.writeQueue = nil
	return nil
}
