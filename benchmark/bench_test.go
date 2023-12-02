package main

import (
	"fmt"
	"log"
	"testing"

	"github.com/manosriram/nimbusdb"
	"github.com/manosriram/nimbusdb/utils"
)

func BenchmarkGetSet(b *testing.B) {
	opts := &nimbusdb.Options{
		Path: utils.DbDir(),
	}
	d, err := nimbusdb.Open(opts)
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
