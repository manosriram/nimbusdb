package nimbusdb

import (
	"os"
	"testing"
	"time"

	"github.com/manosriram/nimbusdb/utils"
	"github.com/stretchr/testify/assert"
)

var opts = &Options{
	Path: utils.DbDir(),
}

const (
	EXPIRY_DURATION = 1 * time.Second
)

func Test_Batch_Open(t *testing.T) {
	d, err := Open(opts)
	b := d.NewBatch()
	defer d.Close()
	defer b.Close()

	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)
	assert.NotEqual(t, b, nil)

	kv := &KeyValuePair{
		Key:   []byte("testkey1"),
		Value: []byte("testvalue1"),
		Ttl:   EXPIRY_DURATION,
	}
	err = b.Set(kv)
	assert.Equal(t, err, nil)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}
