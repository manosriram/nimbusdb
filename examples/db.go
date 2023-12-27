package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/manosriram/nimbusdb"
)

const (
	// DirPath = "/Users/manosriram/nimbusdb/test_data"
	DirPath = "../tests/nimbusdb_temp1107229867/"
)

func main() {
	d, _ := nimbusdb.Open(&nimbusdb.Options{Path: DirPath})
	defer d.Close()
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("> ")
		text, _ := reader.ReadString('\n')

		text = strings.TrimSpace(text)

		if text == "set" {
			key, _ := reader.ReadString('\n')
			value, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			kv := &nimbusdb.KeyValuePair{
				Key:   []byte(key),
				Value: []byte(value),
				// ExpiresIn: 5 * time.Second,
			}
			_, err := d.Set(kv)
			fmt.Println(err)
		} else if text == "delete" {
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			d.Delete([]byte(key))
		} else if text == "all" {
			pairs := d.All()
			for i, pair := range pairs {
				fmt.Printf("%d. %s %v %v\n", i+1, pair.Key, pair.Value, pair.Ttl)
			}
		} else if text == "exit" {
			os.Exit(1)
		} else if text == "get" {
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			kv := &nimbusdb.KeyValuePair{
				Key: []byte(key),
			}
			z, err := d.Get(kv.Key)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(string(z))
		} else if text == "sync" {
			d.Sync()
		} else if text == "keyreader" {
			prefix := ""
			d.KeyReader(prefix, func(k []byte) {
				fmt.Printf("%s\n", string(k))
			})
		} else if text == "keyvaluereader" {
			keyPrefix := ""
			d.KeyValueReader(keyPrefix, func(k []byte, v []byte) {
				fmt.Printf("%s %s\n", string(k), string(v))
			})
		}
	}
}
