package db

import (
	"fmt"
	"os"
)

const (
	DATAFILE_SUFFIX = ".datafile"
)

type Segment struct {
	tstamp string
	ksize  uint32
	vsize  uint32
	k      []byte
	v      []byte
}

type Db struct {
	dirPath               string
	activeDataFileSegment map[string]Segment
}

func Open(path string) (*Db, error) {
	db := &Db{}

	d, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	fmt.Println(d)

	db.dirPath = path
	db.activeDataFileSegment = make(map[string]Segment, 0)

	return db, nil
}

// func main() {
// db, _ := Open("/Users/manosriram/go/src/diskdb/data")
// fmt.Println(db)
// }
