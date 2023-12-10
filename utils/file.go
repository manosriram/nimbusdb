package utils

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	BlockSize = 10
)

func TimeUntilUnixNano(tstamp int64) time.Duration {
	return time.Until(time.Unix(0, tstamp))
}

func HasTimestampExpired(timestamp int64) bool {
	tstamp := time.Unix(0, timestamp).UnixNano()
	now := time.Now().UnixNano()
	return tstamp < now
}

func Int32ToByte(n int32) []byte {
	b := make([]byte, binary.MaxVarintLen32)
	binary.LittleEndian.PutUint32(b, uint32(n))
	return b
}

func Int64ToByte(n int64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	binary.LittleEndian.PutUint64(b, uint64(n))
	return b
}

func ByteToInt32(b []byte) int32 {
	if lesser := len(b) < binary.MaxVarintLen32; lesser {
		b = b[:cap(b)]
	}
	return int32(binary.LittleEndian.Uint32(b))
}

func ByteToInt64(b []byte) int64 {
	if lesser := len(b) < binary.MaxVarintLen64; lesser {
		b = b[:cap(b)]
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func Encode(d interface{}) []byte {
	switch d.(type) {
	case string:
		return []byte(fmt.Sprintf("%s", d))
	case int64:
		return Int64ToByte(d.(int64))
	case int32:
		return Int32ToByte(d.(int32))
	case int:
		z := d.(int)
		return Int32ToByte(int32(z))
	default:
		return d.([]byte)
	}
}

func ReadFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func StringToInt(s []byte) (int, error) {
	return strconv.Atoi(fmt.Sprintf("%s", s))
}

func GetFilenameWithoutExtension(filename string) string {
	return filename[strings.LastIndex(filename, "/")+1:]
}

func JoinPaths(pathA, pathB string) string {
	return filepath.Join(pathA, pathB)
}

func DbDir() string {
	tmp, _ := os.MkdirTemp(".", "nimbusdb_temp")
	return tmp
}

func Recover() {
	if err := recover(); err != nil {
		log.Printf("panic occurred: %v\n", err)
		os.Exit(0)
	}
}

func GetBlockNumber(offset int64) int64 {
	return offset % BlockSize
}
