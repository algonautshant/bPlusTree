package bptree

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)



func TestCatalog(t *testing.T) {

	cat, err := InitializeCatalog("test.yaai")
	require.NoError(t, err)
	err = cat.close()
	require.NoError(t, err)

	
	f, err := os.OpenFile("notes.txt", os.O_RDWR, 0755)
	if err != nil {
		fmt.Println(err)
	}
	if err := f.Close(); err != nil {
		fmt.Println(err)
	}

}
