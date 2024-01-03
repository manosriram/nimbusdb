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

	key := []byte("test1")
	value := []byte("test2")
	d.Set(key, value)

	b := d.NewBatch()

	key = []byte("test3")
	value = []byte("test4")
	b.Set(key, value)

	pairs := d.All()
	for i, pair := range pairs {
		fmt.Printf("%d. %s %v %v\n", i+1, pair.Key, pair.Value, pair.Ttl)
	}

	b.Exists([]byte("test3")) // true

	b.Commit() // or b.Rollback() to discard all writes
}
