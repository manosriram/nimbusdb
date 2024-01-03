package main

import (
	"fmt"

	"golang.org/x/exp/slices"
)

func main() {
	x := []int{1, 2, 3}

	slices.Delete(x, 0, 1)
	fmt.Println(x)
}
