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

var PWD, _ = os.Getwd()
var TEST_DATAFILE_PATH = fmt.Sprintf("%s/../test_data/", PWD)
var CONCURRENT_TEST_DATAFILE_PATH = fmt.Sprintf("%s/../concurrent_test_data/", PWD)
var keys [][]byte

func TestDbOpen(t *testing.T) {
	d, err := nimbusdb.Open(TEST_DATAFILE_PATH)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)
}

func Test_Set(t *testing.T) {
	d, err := nimbusdb.Open(TEST_DATAFILE_PATH)
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
	d, err := nimbusdb.Open(TEST_DATAFILE_PATH)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	va, err := d.Get(kv.Key)
	assert.Equal(t, nil, err)
	assert.Equal(t, kv.Value, va)
}

func Test_StressSet(t *testing.T) {
	d, err := nimbusdb.Open(TEST_DATAFILE_PATH)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(uuid.NewString()),
			Value: []byte("testvalue"),
		}
		// fmt.Println(string(kv.Key))
		keys = append(keys, kv.Key)
		v, err := d.Set(kv)
		assert.Equal(t, err, nil)
		assert.Equal(t, v, []byte("testvalue"))
	}
}

func Test_StressGet(t *testing.T) {
	d, err := nimbusdb.Open(TEST_DATAFILE_PATH)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		// fmt.Println(string(keys[i]))
		kv := &nimbusdb.KeyValuePair{
			Key:   keys[i],
			Value: []byte("testvalue"),
		}
		v, err := d.Get(kv.Key)
		assert.Equal(t, err, nil)
		assert.Equal(t, v, kv.Value)
	}
}

func Test_ConcurrentSet(t *testing.T) {
	d, err := nimbusdb.Open(CONCURRENT_TEST_DATAFILE_PATH)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 100000

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

	assert.Equal(t, d.Count(), int64(numGoRoutines))
}

func Test_ConcurrentGet(t *testing.T) {
	d, err := nimbusdb.Open(CONCURRENT_TEST_DATAFILE_PATH)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 100000

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
			_, err := d.Get(kv.Key)
			if err != nil {
				fmt.Println(err)
			}
			// assert.Equal(t, nil, err)
			// assert.Equal(t, kv.Value, v)
		}()
	}
	wg.Wait()

	assert.Equal(t, d.Count(), int64(numGoRoutines))
}
