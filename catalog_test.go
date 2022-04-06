package bptree

import (
	"fmt"
	"os"
	"testing"
)



func TestCatalog(t *testing.T) {
	f, err := os.OpenFile("notes.txt", os.O_RDWR, 0755)
	if err != nil {
		fmt.Println(err)
	}
	if err := f.Close(); err != nil {
		fmt.Println(err)
	}

}
