package main

import (
	"fmt"
	"log"
	"testing"

	"github.com/manosriram/nimbusdb"
)

func BenchmarkGetSet(t *testing.B) {
	const (
		DirPath = "/Users/manosriram/go/src/nimbusdb/test_data/"
	)

	d, err := nimbusdb.Open(DirPath, true)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 200000; i++ {
		x := fmt.Sprintf("%d", i)
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(x),
			Value: []byte("testvalue"),
		}
		d.Set(kv)
		d.Get(kv.Key)
	}
}
