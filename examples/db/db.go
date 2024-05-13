package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/manosriram/nimbusdb"
)

const (
	// DirPath = "/Users/manosriram/nimbusdb/test_data"
	DirPath = "./dd/"
)

func watchKeyChange(ch chan nimbusdb.WatcherEvent) {
	for event := range ch {
		switch event.EventType {
		case "CREATE":
			log.Printf("got event: %s for Key %s with Value %s\n", event.EventType, event.Key, event.NewValue)
			break
		case "UPDATE":
			log.Printf("got event: %s for Key %s with OldValue %s and NewValue %s\n", event.EventType, event.Key, event.OldValue, event.NewValue)
			break
		case "DELETE":
			log.Printf("got event: %s for Key %s\n", event.EventType, event.Key)
			break
		}
	}
}

func main() {
	d, _ := nimbusdb.Open(&nimbusdb.Options{Path: DirPath, WatchQueueSize: 10})
	defer d.Close()

	ch, _ := d.NewWatch()
	defer d.CloseWatch()

	// b, _ := d.NewBatch()
	// d.Set([]byte("asdlal"), []byte("asdlkjas"), &nimbusdb.Options{ShouldWatch: true})

	// b.Set([]byte("asdlal"), []byte("asdlkjas"))
	// b.Commit()

	go watchKeyChange(ch)

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
			k := []byte(key)
			v := []byte(value)
			_, err := d.Set(k, v)
			fmt.Println(err)
		case "delete":
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			d.Delete([]byte(key))
		case "all":
			pairs := d.All()
			for i, pair := range pairs {
				fmt.Printf("%d. %s %v %v\n", i+1, pair.Key, pair.Value, pair.Ttl)
			}
		case "exit":
			os.Exit(1)
		case "get":
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			k := []byte(key)
			z, err := d.Get(k)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(string(z))
		case "sync":
			d.Sync()
		case "keyreader":
			prefix := ""
			d.KeyReader(prefix, func(k []byte) {
				fmt.Printf("%s\n", string(k))
			})
		case "keyvaluereader":
			keyPrefix := ""
			d.KeyValueReader(keyPrefix, func(k []byte, v []byte) {
				fmt.Printf("%s %s\n", string(k), string(v))
			})
		}
	}
}
