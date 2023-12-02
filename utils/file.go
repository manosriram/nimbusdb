package utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func HasTimestampExpired(timestamp int64) bool {
	tstamp := time.Unix(0, timestamp).UnixNano()
	now := time.Now().UnixNano()
	return tstamp < now
}

func Int64ToByte(n int64) []byte {
	data := uint64(n)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	enc.Encode(data)
	return buf.Bytes()
}

func ByteToInt64(b []byte) int64 {
	var x uint64
	debuf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(debuf)
	dec.Decode(&x)

	return int64(x)
}

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
	fmt.Println("t = ", tmp)
	return tmp
}
