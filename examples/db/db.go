package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/manosriram/nimbusdb"
)

const (
	DirPath = "/Users/manosriram/nimbusdb/test_data"
)

func main() {
	d, _ := nimbusdb.Open(&nimbusdb.Options{Path: DirPath})
	defer d.Close()
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("> ")
		text, _ := reader.ReadString('\n')

		text = strings.TrimSpace(text)

		switch text {
		case "set":
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
			break
		case "delete":
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			d.Delete([]byte(key))
			break
		case "all":
			pairs := d.All()
			for i, pair := range pairs {
				fmt.Printf("%d. %s %v %v\n", i+1, pair.Key, pair.Value, pair.Ttl)
			}
			break
		case "exit":
			os.Exit(1)
			break
		case "get":
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
			break
		case "sync":
			d.Sync()
			break
		case "keyreader":
			prefix := ""
			d.KeyReader(prefix, func(k []byte) {
				fmt.Printf("%s\n", string(k))
			})
			break
		case "keyvaluereader":
			keyPrefix := ""
			d.KeyValueReader(keyPrefix, func(k []byte, v []byte) {
				fmt.Printf("%s %s\n", string(k), string(v))
			})
			break
		}
	}
}
