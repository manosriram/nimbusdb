// package main

// import (
// "bytes"
// "fmt"

// "github.com/google/btree"
// )

// type item struct {
// key []byte
// v   string
// }

// func (it item) Less(i btree.Item) bool {
// return bytes.Compare(it.key, i.(*item).key) < 0
// }

// func main() {
// tr := btree.New(5)
// it := &item{
// key: []byte("key1"),
// v:   "value1",
// }
// tr.ReplaceOrInsert(it)
// z := &item{
// key: []byte("key1"),
// }
// itx := tr.Get(z)
// fmt.Println(itx.(*item).v)
// }
