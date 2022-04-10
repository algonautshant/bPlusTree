package indexer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/algonautshant/bPlusTree/bptree"
)

func TestGetIndexer(t *testing.T) {
	tmpdir := t.TempDir()

	indxr, err := GetIndexer(tmpdir+"/filename", true)
	require.NoError(t, err)
	require.NotNil(t, indxr)

	acct := bptree.AddressKey{}
	for x := uint64(0); x < uint64(1000000); x++ {
		acct[1] = byte(x)
		acct[len(acct)-1] = 99
		err = indxr.AddAccountBalance(acct, 1000000+x, 6000000+x)
		require.NoError(t, err)
	}

	for x := uint64(0); x < uint64(1000000); x++ {
		acct[1] = byte(x)
		acct[len(acct)-1] = 99
		result, _, err := indxr.GetAccountBalance(acct)
		require.NoError(t, err)
		require.NotNil(t, result)
	}
}
