package bptree

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)



func TestCatalog(t *testing.T) {

	tmpdir := t.TempDir()

	cat, err := InitializeCatalog(tmpdir + "/test.yaai", false)
	require.True(t, errors.Is(err, os.ErrNotExist), "ErrNotExist expected")

	cat, err = InitializeCatalog(tmpdir + "/test.yaai", true)
	require.NoError(t, err)


	err = cat.close()
	require.NoError(t, err)

}
