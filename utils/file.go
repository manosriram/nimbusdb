package utils

import "fmt"

func Encode(d interface{}) []byte {
	switch d.(type) {
	case string:
		return []byte(fmt.Sprintf("%s", d))
	case int, uint32, uint64, int32, int64:
		return []byte(fmt.Sprintf("%d", d))
	default:
		return d.([]byte)
	}
}
