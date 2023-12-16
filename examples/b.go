// package main

// import (
// "fmt"
// "time"

// "github.com/hashicorp/golang-lru/v2/expirable"
// )

// type X struct {
// A int
// B int
// }

// func main() {
// x := X{A: 1, B: 2}

// // make cache with 10ms TTL and 5 max keys
// cache := expirable.NewLRU[string, []*X](5, nil, time.Millisecond*10)

// // set value under key1.
// cache.Add("key1", []*X{&x})

// // get value under key1
// r, ok := cache.Get("key1")

// // check for OK value
// if ok {
// fmt.Printf("%T", cache)
// fmt.Println(r[0])
// // fmt.Printf("value before expiration is found: %v, value: %q\n", ok, r)
// }
// }
