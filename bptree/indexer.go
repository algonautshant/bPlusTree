package bptree

import (
	"fmt"
)


type Indexer struct {
	cat *catalog
}

type RoundBalance struct {
	round   uint64
	balance uint64
}

func (idxr *Indexer) AddAccountBalance(acct AddressKey, balance, round uint64) (err error) {
	return idxr.addAccountBalance(acct, balance, round)
}

func (idxr *Indexer)GetAccountBalance(acct AddressKey) (result []RoundBalance, moreHandle uint64, err error) {
	return
}

func (idxr *Indexer)	GetMoreAccountBalance(moreHandle uint64) (result []RoundBalance, newMoreHandle uint64, err error) {
	return
}

func (idxr *Indexer) Close() (err error) {
	return idxr.cat.close()
}




func (idxr *Indexer) addAccountBalance(acct AddressKey, balance, round uint64) (err error) {

	sm := idxr.cat.sm
	bm := idxr.cat.bm

	ah, err := bm.readPage(sm.header.accountsHeadOffset)
	if err != nil {
		return err
	}
	accountsHead, ok := ah.(*bPTreeAddressValuePage)
	if !ok {
		return fmt.Errorf("addAccountBalance expected bPTreeAddressValuePage got something else")
	}
	_, err = accountsHead.insertAddress(bm, acct, balance, round)
	if err != nil {
		return err
	}
	return nil
}
