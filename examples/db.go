package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/manosriram/nimbusdb"
)

func main() {
	d, _ := nimbusdb.Open("/Users/manosriram/go/src/nimbusdb/data/")
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')

		text = strings.TrimSpace(text)

		fmt.Println(text)
		if text == "set" {
			key, _ := reader.ReadString('\n')
			value, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			kv := &nimbusdb.KeyValuePair{
				Key:   []byte(key),
				Value: []byte(value),
			}
			d.Set(kv)
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
		}
	}
}
