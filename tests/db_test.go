package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/manosriram/nimbusdb"
	"github.com/manosriram/nimbusdb/utils"
	"github.com/stretchr/testify/assert"
)

var opts = &nimbusdb.Options{
	Path: utils.DbDir(),
}

const (
	EXPIRY_DURATION = 1 * time.Second
)

func TestDbOpen(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)
}

func Test_InMemory_SetGet_With_Expiry(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey1"),
		Value: []byte("testvalue1"),
		Ttl:   EXPIRY_DURATION,
	}
	v, err := d.Set(kv)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue1"))

	time.Sleep(2 * time.Second)
	va, err := d.Get(kv.Key)
	assert.NotEqual(t, nil, err)
	assert.NotEqual(t, kv.Value, va)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_InMemory_SetGet(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
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
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_InMemory_Stress_SetGet(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte("testkey"),
		}

		_, err := d.Set(kv)
		assert.Nil(t, err)
	}

	for i := 0; i < 100000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte("testkey"),
		}
		va, err := d.Get(kv.Key)
		assert.Nil(t, err)
		assert.Equal(t, kv.Value, va)
	}
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Set(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	_, err = d.Set(kv)
	assert.Nil(t, err)
}

func Test_Get(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	va, err := d.Get(kv.Key)
	assert.Nil(t, err)
	assert.Equal(t, kv.Value, va)
}

func Test_Delete(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	err = d.Delete(kv.Key)
	assert.Equal(t, nil, err)

	_, err = d.Get(kv.Key)
	assert.Equal(t, err.Error(), nimbusdb.KEY_NOT_FOUND)
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_InMemory_Delete(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	kv := &nimbusdb.KeyValuePair{
		Key:   []byte("testkey_for_delete"),
		Value: []byte("testvalue1"),
	}
	v, err := d.Set(kv)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue1"))

	va, err := d.Get(kv.Key)
	assert.Equal(t, nil, err)
	assert.Equal(t, kv.Value, va)

	err = d.Delete(kv.Key)
	assert.Equal(t, nil, err)

	va, err = d.Get(kv.Key)
	assert.Equal(t, err.Error(), nimbusdb.KEY_NOT_FOUND)
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_StressSet(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte("testvalue"),
		}
		_, err := d.Set(kv)
		assert.Nil(t, err)
	}
}

func Test_StressGet(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte("testvalue"),
		}
		v, err := d.Get(kv.Key)
		assert.Nil(t, err)
		assert.Equal(t, kv.Value, v)
	}
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_ConcurrentSet(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte(fmt.Sprintf("testvalue%d", i)),
		}
		go func() {
			defer wg.Done()
			_, err := d.Set(kv)
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}

func Test_ConcurrentGet(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte(fmt.Sprintf("testvalue%d", i)),
		}
		go func() {
			defer wg.Done()
			v, err := d.Get(kv.Key)
			assert.Nil(t, err)
			assert.Equal(t, kv.Value, v)
		}()
	}
	wg.Wait()
}

func Test_ConcurrentDelete(t *testing.T) {
	d, err := nimbusdb.Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		kv := &nimbusdb.KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte(fmt.Sprintf("testvalue%d", i)),
		}
		go func() {
			defer wg.Done()
			err := d.Delete(kv.Key)
			assert.Nil(t, err)

			_, err = d.Get(kv.Key)
			assert.Equal(t, nimbusdb.KEY_NOT_FOUND, err.Error())
		}()
	}
	wg.Wait()
}
