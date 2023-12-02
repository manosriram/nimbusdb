package main

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/manosriram/nimbusdb"
	"github.com/stretchr/testify/assert"
)

var keys [][]byte
var opts nimbusdb.Options
var TestDir = "/Users/manosriram/nimbusdb"
var TestPath = "/Users/manosriram/nimbusdb/test_data"

func TestDbOpen(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)
}

func Test_InMemory_SetGet(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey1"),
		Value: []byte("testvalue1"),
	}
	v, err := d.Set(kv)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue1"))

	va, err := d.Get(kv.Key)
	assert.Equal(t, nil, err)
	assert.Equal(t, kv.Value, va)
}

func Test_Set(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
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

func Test_Get(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	va, err := d.Get(kv.Key)
	assert.Equal(t, nil, err)
	assert.Equal(t, kv.Value, va)
	t.Cleanup(func() {
		os.RemoveAll(TestDir)
	})
}

func Test_StressSet(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 10000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(uuid.NewString()),
			Value: []byte("testvalue"),
		}
		keys = append(keys, kv.Key)
		v, err := d.Set(kv)
		assert.Equal(t, err, nil)
		assert.Equal(t, v, []byte("testvalue"))
	}
}

func Test_StressGet(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 10000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   keys[i],
			Value: []byte("testvalue"),
		}
		v, err := d.Get(kv.Key)
		assert.Equal(t, err, nil)
		assert.Equal(t, v, kv.Value)
	}
	t.Cleanup(func() {
		os.RemoveAll(TestDir)
	})
}

func Test_ConcurrentSet(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("testvalue%d", i)),
		}
		go func() {
			defer wg.Done()
			v, err := d.Set(kv)
			assert.Equal(t, nil, err)
			assert.Equal(t, kv.Value, v)
		}()
	}
	wg.Wait()
}

func Test_ConcurrentGet(t *testing.T) {
	d, err := nimbusdb.Open(&nimbusdb.Options{Path: TestPath})
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		// fmt.Println("getting ", i)
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("testvalue%d", i)),
		}
		go func() {
			defer wg.Done()
			v, err := d.Get(kv.Key)
			assert.Equal(t, nil, err)
			assert.Equal(t, kv.Value, v)
		}()
	}
	wg.Wait()
	t.Cleanup(func() {
		os.RemoveAll(TestDir)
	})
}
