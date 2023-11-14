package main

import (
	"testing"

	"github.com/manosriram/nimbusdb"
	"github.com/stretchr/testify/assert"
)

func TestDbOpen(t *testing.T) {
	d, err := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)
}

func Test_SingleDigitSize_Set(t *testing.T) {
	d, err := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	v, err := d.Set(kv)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue"))
}

func Test_SingleDigitSize_Get(t *testing.T) {
	d, err := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	v, err := d.Get(kv.Key)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, kv.Value)
}

func Test_MultiDigitSize_Set(t *testing.T) {
	d, err := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("test_value_whose_size_is_greater_than_9"),
	}
	v, err := d.Set(kv)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, kv.Value)
}

func Test_MultiDigitSize_Get(t *testing.T) {
	d, err := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("test_value_whose_size_is_greater_than_9"),
	}
	v, err := d.Get(kv.Key)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, kv.Value)
}
