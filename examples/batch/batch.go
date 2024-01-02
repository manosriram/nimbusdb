package main

import (
	"fmt"
	"time"

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

	b.Set([]byte("test3"), []byte("test4"))
	b.SetWithTTL([]byte("test3"), []byte("test4"), 1*time.Hour)

	pairs := d.All()
	for i, pair := range pairs {
		fmt.Printf("%d. %s %v %v\n", i+1, pair.Key, pair.Value, pair.Ttl)
	}
	// b.Rollback()

	b.Commit()
}
