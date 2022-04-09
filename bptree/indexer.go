package bptree

import (
	"fmt"
)

type Indexer struct {
	cat *catalog
}

func (idxr *Indexer) AddAccountBalance(acct AddressKey, balance, round uint64) (err error) {
	return idxr.addAccountBalance(acct, balance, round)
}

func (idxr *Indexer) GetAccountBalance(acct AddressKey) (result []RoundBalance, moreHandle FileOffsetPageIndex, err error) {
	p, err := idxr.cat.bm.readPage(idxr.cat.sm.header.accountsHeadOffset)
	if err != nil {
		return nil, 0, err
	}
	avp, ok  := p.(*bPTreeAddressValuePage)
	if !ok {
		return nil, 0, fmt.Errorf("GetAccountBalance expcected bPTreeAddressValuePage got something else")
	}
	valueAt, err := avp.searchAddress(idxr.cat.bm, acct)
	if err != nil {
		return nil, 0, err
	}
	if valueAt == 0 {
		return nil, 0, nil
	}
	result, moreHandle, err = getRoundBalances(idxr.cat.bm, valueAt)
	if err != nil {
		return nil, 0, err
	}
	return result, moreHandle, err
}

func (idxr *Indexer) GetMoreAccountBalance(moreHandle uint64) (result []RoundBalance, newMoreHandle FileOffsetPageIndex, err error) {
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
