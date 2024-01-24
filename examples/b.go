package main

import (
	"fmt"

	"github.com/manosriram/nimbusdb"
)

func main() {
	d, _ := nimbusdb.Open(&nimbusdb.Options{
		Path:        "./",
		ShouldWatch: true,
	})

	ch, _ := d.NewWatch()

	k := []byte("testkey1")
	v := []byte("testvalue1")
	d.Set(k, v)

	fmt.Printf("%v\n", <-ch)
}
