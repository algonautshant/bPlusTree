package bptree

import (
	"encoding/binary"
	"fmt"
)

const (
	ADDRESS_SIZE = 32
	BPTREEKEYVALUEPAGE_STORAGE_ID = 0
	BPTREEKEYVALUEINDEXPAGE_STORAGE_ID = 1
)

type Address [ADDRESS_SIZE]byte

func (a *Address) compare(b *Address) int {
	for i := 0; i < ADDRESS_SIZE; i++ {
		if a[i] > b[i] {
			return 1
		} else if a[i] < b[i] {
			return -1
		}
	}
	return 0
}

type Page interface {
	IsLeaf() bool
	Unmarshal(b []byte)
	Marshal(b []byte) (numWritten int, err error)
}

type BPTreeKeyValuePage struct {
	isLeaf bool
	len    uint64
	keys   []uint64
	values []uint64
}

func (kv *BPTreeKeyValuePage) Key(i int) uint64 {
	return kv.keys[i]
}
func (kv *BPTreeKeyValuePage) Value(i int) uint64 {
	return kv.values[i]
}
func (kv *BPTreeKeyValuePage) IsLeaf() bool {
	return kv.isLeaf
}

// Unmarshal deserializes the page into BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeKeyValueIndexPage 
// byte 1: 0 isLeaf=false 1 isLeaf=true
// byte 2: len (uint64)
// byte 10: keys
// byte ...: values
func (kv *BPTreeKeyValuePage) Unmarshal(b []byte) {
	lastIndex := 0
	if b[lastIndex] == 0 {
		kv.isLeaf = false
	} else {
		kv.isLeaf = true
	}
	lastIndex++
	kv.len = binary.BigEndian.Uint64(b[lastIndex:lastIndex+8])
	kv.keys = make([]uint64, 0, kv.len)
	kv.values = make([]uint64, 0, kv.len)
	lastIndex+=8
	for x := uint64(0); x < kv.len; x++ {
		kv.keys = append(kv.keys,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
	for x := uint64(0); x < kv.len; x++ {
		kv.values = append(kv.values,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
}

func (kv *BPTreeKeyValuePage) storageID() byte {
	return  BPTREEKEYVALUEPAGE_STORAGE_ID
}

// Marshal deserializes the page from BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeKeyValueIndexPage 
// byte 1: 0 isLeaf=false 1 isLeaf=true
// byte 2: len (uint64)
// byte ...: keys
// byte ...: values
func (kv *BPTreeKeyValuePage) Marshal(b []byte) (numWritten int, err error) {
	offset := 0
	b[offset] = kv.storageID()
	offset++
	if kv.isLeaf {
		b[offset] = 1
	} else {
		b[offset] = 0
	}
	offset++
	binary.BigEndian.PutUint64(b[offset:offset+8], kv.len)
	offset += 8

	for _, x := range kv.keys {
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
		if offset > PAGE_SIZE {
			return 0, OversizeError{kv.len, "BPTreeKeyValuePage"}
		}
	}
	for _, x := range kv.values {
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
		if offset > PAGE_SIZE {
			return 0, OversizeError{kv.len, "BPTreeKeyValuePage"}
		}
	}
	return offset, nil
}

func (kv *BPTreeKeyValuePage) SearchKey(catalog Catalog, key uint64) (value uint64) {
	var i int
	var k uint64
	for i, k = range kv.keys {
		if k < key {
			continue
		}
		break
	}
	if i < len(kv.keys) && k == key {
		return kv.Key(i)
	}
	if kv.IsLeaf() {
		return
	}
	nextPage := catalog.GetPage(kv.Key(i))
	kvNP  := nextPage.(*BPTreeKeyValuePage)
	return kvNP.SearchKey(catalog, key)
}

type BPTreeKeyValueIndexPage struct {
	isLeaf bool
	len     uint64
	addresses    []Address
	values  []uint64
	indexes []uint64 // only in the leaf pages
}

func (kvi *BPTreeKeyValueIndexPage) Address(i int) Address {
	return kvi.addresses[i]
}
func (kvi *BPTreeKeyValueIndexPage) Value(i int) uint64 {
	return kvi.values[i]
}
func (kvi *BPTreeKeyValueIndexPage) Index(i int) uint64 {
	return kvi.indexes[i]
}
func (kvi *BPTreeKeyValueIndexPage) IsLeaf() bool {
	return kvi.isLeaf
}

// ReadFromDisk deserializes the page into BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeKeyValueIndexPage 
// byte 1: 0 isLeaf=false 1 isLeaf=true
// byte 2: len (uint64)
// byte 10: addresses
// byte ...: values
// byte ...: indexes
func (kvi *BPTreeKeyValueIndexPage) Unmarshal(b []byte) {
	lastIndex := 0
	if b[lastIndex] == 0 {
		kvi.isLeaf = false
	} else {
		kvi.isLeaf = true
	}
	lastIndex++

	kvi.len = binary.BigEndian.Uint64(b[lastIndex:lastIndex+8])
	lastIndex += 8
	kvi.addresses = make([]Address, kvi.len)
	kvi.values = make([]uint64, 0, kvi.len)
	kvi.indexes = make([]uint64, 0, kvi.len)
	for x := uint64(0); x < kvi.len; x++ {
		copy(kvi.addresses[x][:], b[lastIndex:lastIndex+ADDRESS_SIZE])
		lastIndex = lastIndex + ADDRESS_SIZE
	}
	for x := uint64(0); x < kvi.len; x++ {
		kvi.values = append(kvi.values,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
	for x := uint64(0); x < kvi.len; x++ {
		kvi.indexes = append(kvi.indexes,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
}

func (kvi *BPTreeKeyValueIndexPage) storageID() byte {
	return  BPTREEKEYVALUEINDEXPAGE_STORAGE_ID
}

// Marshal deserializes the page from BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeKeyValueIndexPage 
// byte 1: 0 isLeaf=false 1 isLeaf=true
// byte 2: len (uint64)
// byte 10: addresses
// byte ...: values
func (kvi *BPTreeKeyValueIndexPage) Marshal(b []byte) (numWritten int, err error) {
	offset := 0
	b[offset] = kvi.storageID()
	offset++
	if kvi.isLeaf {
		b[offset] = 1
	} else {
		b[offset] = 0
	}
	offset++
	binary.BigEndian.PutUint64(b[offset:offset+8], kvi.len)
	offset += 8

	for _, x := range kvi.addresses {
		copy(b[offset:offset+ADDRESS_SIZE], x[:])
		offset += ADDRESS_SIZE
		if offset > PAGE_SIZE {
			return 0, OversizeError{kvi.len, "BPTreeKeyValueIndexPage"}
		}
	}
	for _, x := range kvi.values {
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
		if offset > PAGE_SIZE {
			return 0, OversizeError{kvi.len, "BPTreeKeyValueIndexPage"}
		}
	}
	for _, x := range kvi.indexes {
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
		if offset > PAGE_SIZE {
			return 0, OversizeError{kvi.len, "BPTreeKeyValueIndexPage"}
		}
	}
	return offset, nil
}

func (kvi *BPTreeKeyValueIndexPage) SearchAddress(catalog Catalog, address Address) (value, index uint64) {
	var i int
	var a Address
	for i, a = range kvi.addresses {
		if a.compare(&address) < 0  {
			continue
		}
		break
	}
	if i < int(kvi.len) &&  a.compare(&address) == 0 {
		return kvi.Value(i), kvi.Index(i)
	}
	if kvi.IsLeaf() {
		return
	}
	nextPage := catalog.GetPage(kvi.Value(i))
	kviNP := nextPage.(*BPTreeKeyValueIndexPage)
	return kviNP.SearchAddress(catalog, address)
}

func Unmarshal(b []byte) (page Page, err error) {
	if b[0] == BPTREEKEYVALUEPAGE_STORAGE_ID {
		kv := BPTreeKeyValuePage{}
		kv.Unmarshal(b[1:])
		return &kv, nil
	}
	if b[0] == BPTREEKEYVALUEINDEXPAGE_STORAGE_ID {
		kvi := BPTreeKeyValueIndexPage{}
		kvi.Unmarshal(b[1:])
		return &kvi, nil
	}
	return nil, fmt.Errorf("Unknown page type: %d", b[0])
}
