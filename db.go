package db

import (
	"fmt"
	"os"
	"strings"
)

const (
	ACTIVE_SEGMENT_DATAFILE_SUFFIX = "dfile"
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
	activeDataFileSegment map[string]*Segment
}

func Open(path string) (*Db, error) {
	db := &Db{}

	d, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	if len(d) == 0 {
		// no files in path
		// create an active segment file
	}

	for _, y := range d {
		if y.IsDir() {
			continue
		}

		z, zz := y.Info()
		if strings.Split(z.Name(), ".")[1] == ACTIVE_SEGMENT_DATAFILE_SUFFIX {
			fmt.Println("got active segment file")
			// db.activeDataFileSegment[z.Name()] = &Segment{

			// }
		}
		fmt.Println("in = ", z, zz)
	}

	db.dirPath = path
	db.activeDataFileSegment = make(map[string]Segment, 0)

	return db, nil
}
