package main

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/manosriram/nimbusdb"
)

var PWD, _ = os.Getwd()
var TEST_DATAFILE_PATH = fmt.Sprintf("%s/../test_data/", PWD)

var kv = &nimbusdb.KeyValuePair{
	Key:   []byte("testkey"),
	Value: []byte("testvalue"),
}

func BenchmarkGetSet(t *testing.B) {
	d, err := nimbusdb.Open(TEST_DATAFILE_PATH)
	if err != nil {
		log.Fatal(err)
	}
	_, err = d.Set(kv)
	if err != nil {
		log.Fatal(err)
	}
	_, err = d.Get(kv.Key)
	if err != nil {
		log.Fatal(err)
	}
}
