package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/manosriram/nimbusdb"
	"github.com/manosriram/nimbusdb/utils"
	"github.com/stretchr/testify/assert"
)

var db *nimbusdb.Db
var opts = &nimbusdb.Options{
	Path: utils.DbDir(),
}

func o() func() {
	var err error
	db, err = nimbusdb.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return func() {
		_ = db.Close()
		_ = os.RemoveAll(opts.Path)
	}
}

func BenchmarkGetSet(b *testing.B) {
	closer := o()
	defer closer()
	b.Run("set", set)
	b.Run("get", get)
}

func set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(fmt.Sprintf("%d", rand.Int())),
			Value: []byte("testvalue"),
		}
		db.Set(kv)
	}
}

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("rosedb-test-key-%09d", i))
}

func get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(GetTestKey(i)),
			Value: []byte("testvalue"),
		}
		_, err := db.Set(kv)
		assert.Nil(b, err)
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(GetTestKey(rand.Int())),
			Value: []byte("testvalue"),
		}
		_, err := db.Get(kv.Key)
		if err != nil && err.Error() != nimbusdb.KEY_NOT_FOUND {
			log.Fatal(err)
		}
	}
}
