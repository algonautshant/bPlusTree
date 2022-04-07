package bptree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetIndexer(t *testing.T) {

	cat, err := InitializeCatalog("filename", true)
	require.NoError(t, err)
	require.NotNil(t, cat)
}
