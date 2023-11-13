package main

import (
	"fmt"

	"github.com/manosriram/nimbusdb"
)

func main() {
	d, _ := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("test"),
		Value: []byte("1234"),
	}
	v, err := d.Set(kv)
	fmt.Println(v, err)
}
