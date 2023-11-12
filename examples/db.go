package main

import (
	"fmt"

	db "github.com/manosriram/treedb"
)

func main() {
	d, _ := db.Open("/Users/manosriram/go/src/treedb/data/")
	fmt.Println(d)
}
