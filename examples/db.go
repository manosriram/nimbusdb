package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/manosriram/nimbusdb"
)

const (
	DirPath = "/Users/manosriram/go/src/nimbusdb/test_data/"
)

func main() {
	d, _ := nimbusdb.Open(DirPath, true)
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
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
			}
			_, err := d.Set(kv)
			fmt.Println(err)
		} else if text == "all" {
			d.All()
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
		} else if text == "seek" {
			// offset, _ := reader.ReadString('\n')
			// o, _ := strconv.Atoi(fmt.Sprintf("%d", offset))
			// kdValue := nimbusdb.KeyDirValue{
			// offset: 31,
			// }
			// s := d.SeekOffsetFromDataFile(31)
			// fmt.Println("kv = ", string(s.Key()), string(s.Value()))
		} else if text == "size" {
			fmt.Println(d.Count())
		} else if text == "stat" {
			d.CreateActiveDatafile(DirPath)
		} else if text == "sync" {
			d.Sync()
		}
	}
}
