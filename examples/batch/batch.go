package main

import (
	"fmt"

	"github.com/manosriram/nimbusdb"
)

const (
	DirPath = "/Users/manosriram/nimbusdb/test_data"
)

func main() {
	d, _ := nimbusdb.Open(&nimbusdb.Options{Path: DirPath})
	defer d.Close()

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("test1"),
		Value: []byte("test2"),
	}
	d.Set(kv)

	b := d.NewBatch()

	kv.Key = []byte("test3")
	kv.Value = []byte("test4")
	b.Set(kv)

	pairs := d.All()
	for i, pair := range pairs {
		fmt.Printf("%d. %s %v %v\n", i+1, pair.Key, pair.Value, pair.Ttl)
	}

	b.Exists([]byte("test3")) // true

	b.Commit() // or b.Rollback() to discard all writes
}
