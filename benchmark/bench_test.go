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
		key := []byte(fmt.Sprintf("%d", rand.Int()))
		value := []byte("testvalue")
		db.Set(key, value)
	}
}

func get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		_, err := db.Set([]byte(utils.GetTestKey(i)), []byte("testvalue"))
		assert.Nil(b, err)
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get([]byte(utils.GetTestKey(rand.Int())))
		if err != nil && err != nimbusdb.ERROR_KEY_NOT_FOUND {
			log.Fatal(err)
		}
	}
}
