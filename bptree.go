package bptree

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	ADDRESS_SIZE                       = 32
	BPTREEKEYVALUEPAGE_STORAGE_ID      = 0
	BPTREEKEYVALUEINDEXPAGE_STORAGE_ID = 1
	FLATKEYVALUEPAGE_STORAGE_ID        = 2

	FLATKEYVALUEPAGE_HEADER_SIZE  = 9
	FLATKEYVALUEPAGE_ELEMENT_SIZE = 16
)

type Address [ADDRESS_SIZE]byte

type Page interface {
	IsLeaf() bool
	Unmarshal(b []byte)
	Marshal(b []byte) (numWritten int, err error)
	storageID() byte
	MaxLength() uint64
}

type BPTreeKeyValuePage struct {
	isLeaf       bool
	numberOfKeys uint64
	keys         []uint64
	values       []uint64
}

type BPTreeAddressValuePage struct {
	isLeaf            bool
	numberOfAddresses uint64
	addresses         []Address
	values            []uint64
}

type FlatKeyValuePage struct {
	nextAvailableArrayIndex uint64 // this is the rounds/values arrray index
	rounds                  []uint64
	values                  []uint64
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
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
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
	kv.numberOfKeys = binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
	kv.keys = make([]uint64, 0, kv.numberOfKeys)
	kv.values = make([]uint64, 0, kv.numberOfKeys)
	lastIndex += 8
	for x := uint64(0); x < kv.numberOfKeys; x++ {
		kv.keys = append(kv.keys,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
	for x := uint64(0); x < kv.numberOfKeys; x++ {
		kv.values = append(kv.values,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
}

func (kv *BPTreeKeyValuePage) MaxLength() uint64 {
	return (PAGE_SIZE - 10) / 2 / 8
}

func (kv *BPTreeKeyValuePage) storageID() byte {
	return BPTREEKEYVALUEPAGE_STORAGE_ID
}

// Marshal deserializes the page from BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
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
	binary.BigEndian.PutUint64(b[offset:offset+8], kv.numberOfKeys)
	offset += 8

	for _, x := range kv.keys {
		if offset+8 > PAGE_SIZE {
			return 0, OversizeError{kv.numberOfKeys, "BPTreeKeyValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
	}
	for _, x := range kv.values {
		if offset+8 > PAGE_SIZE {
			return 0, OversizeError{kv.numberOfKeys, "BPTreeKeyValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
	}
	return offset, nil
}

func (kv *BPTreeKeyValuePage) SearchKey(bm *BufferManager, key uint64) (value uint64) {
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
	nextPage := bm.ReadPage(FileOffset(kv.Key(i)))
	kvNP := nextPage.(*BPTreeKeyValuePage)
	return kvNP.SearchKey(bm, key)
}

func (av *BPTreeAddressValuePage) Address(i int) Address {
	return av.addresses[i]
}
func (av *BPTreeAddressValuePage) Value(i int) uint64 {
	return av.values[i]
}
func (av *BPTreeAddressValuePage) IsLeaf() bool {
	return av.isLeaf
}

// ReadFromDisk deserializes the page into BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: 0 isLeaf=false 1 isLeaf=true
// byte 2: len (uint64)
// byte 10: addresses
// byte ...: values
func (av *BPTreeAddressValuePage) Unmarshal(b []byte) {
	lastIndex := 0
	if b[lastIndex] == 0 {
		av.isLeaf = false
	} else {
		av.isLeaf = true
	}
	lastIndex++

	av.numberOfAddresses = binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
	lastIndex += 8
	av.addresses = make([]Address, av.numberOfAddresses)
	av.values = make([]uint64, 0, av.numberOfAddresses)
	for x := uint64(0); x < av.numberOfAddresses; x++ {
		copy(av.addresses[x][:], b[lastIndex:lastIndex+ADDRESS_SIZE])
		lastIndex = lastIndex + ADDRESS_SIZE
	}
	for x := uint64(0); x < av.numberOfAddresses; x++ {
		av.values = append(av.values,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
}

func (av *BPTreeAddressValuePage) MaxLength() uint64 {
	return (PAGE_SIZE - 10) / (ADDRESS_SIZE + 8)
}

func (av *BPTreeAddressValuePage) storageID() byte {
	return BPTREEKEYVALUEINDEXPAGE_STORAGE_ID
}

// Marshal deserializes the page from BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: 0 isLeaf=false 1 isLeaf=true
// byte 2: len (uint64)
// byte 10: addresses
// byte ...: values
func (av *BPTreeAddressValuePage) Marshal(b []byte) (numWritten int, err error) {
	offset := 0
	b[offset] = av.storageID()
	offset++
	if av.isLeaf {
		b[offset] = 1
	} else {
		b[offset] = 0
	}
	offset++
	binary.BigEndian.PutUint64(b[offset:offset+8], av.numberOfAddresses)
	offset += 8

	for _, x := range av.addresses {
		if offset+ADDRESS_SIZE > PAGE_SIZE {
			return 0, OversizeError{av.numberOfAddresses, "BPTreeAddressValuePage"}
		}
		copy(b[offset:offset+ADDRESS_SIZE], x[:])
		offset += ADDRESS_SIZE
	}
	for _, x := range av.values {
		if offset+8 > PAGE_SIZE {
			return 0, OversizeError{av.numberOfAddresses, "BPTreeAddressValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
	}
	return offset, nil
}

func (av *BPTreeAddressValuePage) SearchAddress(bm BufferManager, address Address) (value uint64) {
	var i int
	var a Address
	for i, a = range av.addresses {
		if a.compare(&address) < 0 {
			continue
		}
		break
	}
	if i < int(av.numberOfAddresses) && a.compare(&address) == 0 {
		return av.Value(i)
	}
	if av.IsLeaf() {
		return
	}
	nextPage := bm.ReadPage(FileOffset(av.Value(i)))
	avNP := nextPage.(*BPTreeAddressValuePage)
	return avNP.SearchAddress(bm, address)
}

func Unmarshal(b []byte) (page Page, err error) {
	if b[0] == BPTREEKEYVALUEPAGE_STORAGE_ID {
		kv := &BPTreeKeyValuePage{}
		kv.Unmarshal(b[1:])
		return kv, nil
	}
	if b[0] == BPTREEKEYVALUEINDEXPAGE_STORAGE_ID {
		av := &BPTreeAddressValuePage{}
		av.Unmarshal(b[1:])
		return av, nil
	}
	if b[0] == FLATKEYVALUEPAGE_STORAGE_ID {
		fkv := &FlatKeyValuePage{}
		fkv.Unmarshal(b[1:])
		return fkv, nil
	}
	return nil, fmt.Errorf("Unknown page type: %d", b[0])
}

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

func (fp *FlatKeyValuePage) IsLeaf() bool {
	return true
}

func (fp *FlatKeyValuePage) MaxLength() uint64 {
	return (PAGE_SIZE - FLATKEYVALUEPAGE_HEADER_SIZE) / FLATKEYVALUEPAGE_ELEMENT_SIZE
}

func (fp *FlatKeyValuePage) storageID() byte {
	return FLATKEYVALUEPAGE_STORAGE_ID
}

// Unmrashal the data
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: lastIndex
// ... interleaved round-value round-value...
func (fp *FlatKeyValuePage) Unmarshal(b []byte) {
	lastIndex := 0
	fp.nextAvailableArrayIndex = binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
	lastIndex += 8
	numberOfElements := uint64((lastIndex - (PAGE_SIZE - 9)) / 16)
	fp.rounds = make([]uint64, numberOfElements)
	fp.values = make([]uint64, numberOfElements)
	for x := uint64(0); x < numberOfElements; x++ {
		fp.rounds[x] = binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
		lastIndex = lastIndex + 8
		fp.values[x] = binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
		lastIndex = lastIndex + 8
	}
}

// Unmrashal the data
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: lastIndex
// ... interleaved round-value round-value...
func (fp *FlatKeyValuePage) Marshal(b []byte) (numWritten int, err error) {
	offset := 0
	b[offset] = fp.storageID()
	offset++
	binary.BigEndian.PutUint64(b[offset:offset+8], fp.nextAvailableArrayIndex)
	offset += 8
	for i, x := range fp.rounds {
		if offset+8 > PAGE_SIZE {
			return 0, OversizeError{uint64(i), "FlatKeyValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
		if offset+8 > PAGE_SIZE {
			return 0, OversizeError{uint64(i), "FlatKeyValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], fp.values[i])
		offset += 8
	}
	return offset, nil
}

// In the storage, round 0 is a info value:
// (round,value)
// (0,0): free to write the next value
// (0,x): x < math.MaxUint64: values continued from index x (not the position pointed by the index)
// (0,x): x < math.MaxUint64: if this is the position pointed by the index, then this position value is the count
// index is the location of the last value (round != 0) of the location of the number of values,
//        and the prevous slot is the last value
//        it is calcuated as follows: PAGE_SIZE * fileIndex + rounds/values array index
// The first value will not have a continued from value. Moving back should not be done,
//        and it is recognized from the counter which is absent for 1.
func AddFlatKVPageValue(bm *BufferManager, index FileOffsetPageIndex, round, value uint64) (newIndex FileOffsetPageIndex, err error) {
	fileIndex := index.GetFileOffset()
	pageIndex := index.GetElementIndexInPage()

	// if this is the first element of this run
	if index == 0 {
		// get the page of 1 elements
		bm.flatKeyValuePageMu.Lock()
		defer bm.flatKeyValuePageMu.Unlock()

		nextRunSize := uint64(1)
		nextRunPageFileIndex := bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))]

		// if the page for this size is not available, allocate a new page
		if nextRunPageFileIndex == 0 {
			newPage := makeFlatKeyValuePage()

			// add the new values (no counter or continued from). Missing counter indicates count of 1.
			newPage.rounds[0] = round
			newPage.values[0] = value

			// allocate nextRunSize slots here
			newPage.nextAvailableArrayIndex = nextRunSize

			newFileIndex, err := bm.AddNewPage(newPage)
			if err != nil {
				return 0, err
			}
			// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
			newIndex := GetFileOffsetPageIndex(newFileIndex, 0)

			// register this page in flatKVFileEndOffset only if it can hold another of this size
			if newPage.nextAvailableArrayIndex+nextRunSize <= newPage.MaxLength() {
				bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))] = newFileIndex
			}
			return newIndex, nil
		}

		// otherwise, add to the existing page
		newPage := bm.ReadPage(nextRunPageFileIndex).(*FlatKeyValuePage) // TODO: add error check
		arrayIndex := newPage.nextAvailableArrayIndex

		// add the new values
		newPage.rounds[arrayIndex] = round
		newPage.values[arrayIndex] = value

		// allocate nextRunSize slots here
		newPage.nextAvailableArrayIndex = newPage.nextAvailableArrayIndex + nextRunSize

		// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
		newIndex := GetFileOffsetPageIndex(nextRunPageFileIndex, int(arrayIndex))

		// register this page in flatKVFileEndOffset only if it can hold another of this size
		if newPage.nextAvailableArrayIndex+nextRunSize <= newPage.MaxLength() {
			bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))] = nextRunPageFileIndex
		} else { // otherwise clear it
			bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))] = 0
		}
		return newIndex, nil
	}

	page := bm.ReadPage(fileIndex)
	fkvp := page.(*FlatKeyValuePage)

	numberOfValues := uint64(1)
	if fkvp.rounds[pageIndex] == 0 {
		// then the value is the count
		numberOfValues = fkvp.values[pageIndex]
	}

	// If the next slot is available, use it
	// If the run size is 1, and there is an avilabe space, then the
	// run sie is not doubled.
	if fkvp.rounds[pageIndex+1] == 0 && fkvp.values[pageIndex+1] == 0 {
		fkvp.rounds[pageIndex] = round
		fkvp.values[pageIndex] = value

		fkvp.rounds[pageIndex+1] = 0
		fkvp.values[pageIndex+1] = numberOfValues + 1

		return index.AddToElementIndex(1), nil
	}

	// If next slot is not available, fine a new page
	bm.flatKeyValuePageMu.Lock()
	defer bm.flatKeyValuePageMu.Unlock()

	// next run size for this is numberOfValues*2
	nextRunSize := numberOfValues
	if nextRunSize*2 <= fkvp.MaxLength() {
		nextRunSize = nextRunSize * 2
	}

	nextRunPageFileIndex := bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))]

	// if the page for this size is not available, allocate a new page
	if nextRunPageFileIndex == 0 {
		newPage := makeFlatKeyValuePage()

		// add a pointer to the previous values (continues from)
		newPage.rounds[0] = 0
		newPage.values[0] = uint64(index)

		// add the new values
		newPage.rounds[1] = round
		newPage.values[1] = value

		// add the count
		newPage.rounds[2] = 0
		newPage.values[2] = numberOfValues + 1

		// allocate nextRunSize slots here
		newPage.nextAvailableArrayIndex = nextRunSize

		newFileIndex, err := bm.AddNewPage(newPage)
		if err != nil {
			return 0, err
		}
		// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
		newIndex := GetFileOffsetPageIndex(newFileIndex, 2)

		// register this page in flatKVFileEndOffset only if it can hold another of this size
		if newPage.nextAvailableArrayIndex+nextRunSize <= fkvp.MaxLength() {
			bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))] = newFileIndex
		}
		return newIndex, nil
	}

	// otherwise, add to the existing page
	newPage := bm.ReadPage(nextRunPageFileIndex).(*FlatKeyValuePage)
	arrayIndex := newPage.nextAvailableArrayIndex

	// add a pointer to the previous values (continues from)
	newPage.rounds[arrayIndex] = 0
	newPage.values[arrayIndex] = uint64(index)

	// add the new values
	newPage.rounds[arrayIndex+1] = round
	newPage.values[arrayIndex+1] = value

	// add the count
	newPage.rounds[arrayIndex+2] = 0
	newPage.values[arrayIndex+2] = numberOfValues + 1

	// allocate nextRunSize slots here
	newPage.nextAvailableArrayIndex = newPage.nextAvailableArrayIndex + nextRunSize

	// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
	newIndex = GetFileOffsetPageIndex(nextRunPageFileIndex, int(arrayIndex + 2))

	// register this page in flatKVFileEndOffset only if it can hold another of this size
	if newPage.nextAvailableArrayIndex+nextRunSize <= fkvp.MaxLength() {
		bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))] = nextRunPageFileIndex
	} else { // otherwise clear it
		bm.sm.header.flatKVFileEndOffset[int(math.Log2(float64(nextRunSize)))] = 0
	}
	return newIndex, nil

}

func makeFlatKeyValuePage() *FlatKeyValuePage {
	fkvp := &FlatKeyValuePage{
		nextAvailableArrayIndex: FLATKEYVALUEPAGE_HEADER_SIZE,
	}
	fkvp.rounds = make([]uint64, fkvp.MaxLength())
	fkvp.values = make([]uint64, fkvp.MaxLength())
	return fkvp
}
