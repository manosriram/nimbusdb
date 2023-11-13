package main

import (
	"fmt"

	"github.com/manosriram/nimbusdb"
)

func main() {
	d, err := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("test2"),
		Value: []byte("1235"),
	}
	v, err := d.Set(kv)
	fmt.Println(v, err)
}
