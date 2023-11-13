package utils

import (
	"fmt"
	"strconv"
)

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

func StringToInt(s []byte) (int, error) {
	return strconv.Atoi(fmt.Sprintf("%s", s))
}
