package utils

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	lock    = sync.Mutex{}
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

func GetTestKey(i int) []byte {
	sb := stringBuilderPool.Get().(*strings.Builder)
	defer stringBuilderPool.Put(sb)
	defer sb.Reset()
	sb.WriteString("nimbusdb_test_key_")
	numStr := strconv.Itoa(i)
	sb.WriteString(numStr)
	finalKey := sb.String()

	return []byte(finalKey)
}

func GetSwapFilePath(dbDirPath string, idFilePath string) string {
	sb := stringBuilderPool.Get().(*strings.Builder)
	defer stringBuilderPool.Put(sb)
	defer sb.Reset()
	sb.WriteString(dbDirPath)
	sb.WriteString(strings.Split(idFilePath, ".")[0])
	swapFilePath := sb.String()

	return swapFilePath
}

func TimeUntilUnixNano(tstamp int64) time.Duration {
	return time.Until(time.Unix(0, tstamp))
}

func HasTimestampExpired(timestamp int64) bool {
	tstamp := time.Unix(0, timestamp).UnixNano()
	now := time.Now().UnixNano()
	return tstamp < now
}

func UInt32ToByte(n uint32) []byte {
	b := make([]byte, binary.MaxVarintLen32)
	binary.LittleEndian.PutUint32(b, n)
	return b
}

func UInt64ToByte(n uint64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	binary.LittleEndian.PutUint64(b, n)
	return b
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

func ByteToUInt64(b []byte) uint64 {
	if lesser := len(b) < binary.MaxVarintLen64; lesser {
		b = b[:cap(b)]
	}
	return binary.LittleEndian.Uint64(b)
}

func ByteToUInt32(b []byte) uint32 {
	if lesser := len(b) < binary.MaxVarintLen32; lesser {
		b = b[:cap(b)]
	}
	return binary.LittleEndian.Uint32(b)
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
		sb := stringBuilderPool.Get().(*strings.Builder)
		defer stringBuilderPool.Put(sb)
		defer sb.Reset()
		sb.WriteString(d.(string))
		ss := sb.String()
		return []byte(ss)
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
