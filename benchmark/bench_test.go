package main

import (
	"fmt"
	"log"
	"testing"

	"github.com/manosriram/nimbusdb"
)

func BenchmarkGetSet(b *testing.B) {
	const (
		DirPath = "/Users/manosriram/nimbusdb/test_data/"
	)

	d, err := nimbusdb.Open(&nimbusdb.Options{Path: DirPath})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte("testvalue"),
		}
		d.Set(kv)
		d.Get(kv.Key)
	}
}
