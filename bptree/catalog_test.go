package bptree

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatalog(t *testing.T) {

	tmpdir := t.TempDir()

	pageSize := uint64(1024)
	numberOfBuffers := 100
	
	cat, err := initializeCatalog(tmpdir+"/test.yaai", false, pageSize, numberOfBuffers)
	require.True(t, errors.Is(err, os.ErrNotExist), "ErrNotExist expected")

	cat, err = initializeCatalog(tmpdir+"/test.yaai", true, pageSize, numberOfBuffers)
	require.NoError(t, err)

	err = cat.close()
	require.NoError(t, err)

	cat, err = initializeCatalog(tmpdir+"/test.yaai", false, pageSize, numberOfBuffers)
	require.NoError(t, err)

}
