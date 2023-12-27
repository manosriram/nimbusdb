// package main

// import (
// "fmt"
// "time"
// )

// func HasTimestampExpired(timestamp int64) bool {
// tstamp := time.Unix(0, timestamp).UnixNano()
// now := time.Now().UnixNano()
// return tstamp < now
// }

// func main() {
// fmt.Println(HasTimestampExpired(1704307674058084000))
// }
