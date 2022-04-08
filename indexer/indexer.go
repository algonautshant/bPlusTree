package indexer

import (
	"github.com/algonautshant/bPlusTree/bptree"
)


type IndexerInterface interface {
	AddAccountBalance(acct bptree.AddressKey, balance, round uint64) error
	GetAccountBalance(acct bptree.AddressKey) (result []bptree.RoundBalance, moreHandle uint64, err error)
	GetMoreAccountBalance(moreHandle uint64) (result []bptree.RoundBalance, newMoreHandle uint64, err error)
	Close() error
}


func GetIndexer(filename string, create bool) (idxr IndexerInterface, err error) {
	return bptree.InitializeIndexer(filename, create)
}

