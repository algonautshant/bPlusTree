package bptree

import (
	"encoding/binary"
	"fmt"
)

const (
	ADDRESS_SIZE                       = 32 // algorand address size
	BPTREEKEYVALUEPAGE_STORAGE_ID      = 0
	BPTREEKEYVALUEINDEXPAGE_STORAGE_ID = 1
	FLATKEYVALUEPAGE_STORAGE_ID        = 2
)

type AddressKey [ADDRESS_SIZE]byte

type page interface {
	isLeaf() bool
	isPinned() bool
	unmarshal(b []byte, pageSize int)
	marshal(b []byte, pageSize int) (numWritten int, err error)
	storageID() byte
	maxNumberOfElements() uint64
}

type bPTreeKeyValuePage struct {
	leaf         bool
	numberOfKeys uint64
	keys         []uint64
	values       []uint64
	pinned       bool
}

type bPTreeAddressValuePage struct {
	t         int // 2t = max number of elements
	leaf      bool
	addresses []AddressKey // 2t-1 for non-leaf, 2t for leaf
	values    []uint64     // 2t
	pinned    bool
}

const BPTREEADDRESSVALUEPAGE_HEADER_SIZE = 18
const BPTREEADDRESSVALUEPAGE_ELEMENT_SIZE = ADDRESS_SIZE + 8

type flatKeyValuePage struct {
	nextAvailableArrayIndex uint64 // this is the rounds/values arrray index
	rounds                  []uint64
	values                  []uint64
	pinned                  bool
}

const FLATKEYVALUEPAGE_ELEMENT_SIZE = 16 // 1 round 1 value
const FLATKEYVALUEPAGE_HEADER_SIZE = 9   // flatKeyType (byte) nextAvailableArrayIndex (uint64)

type RoundBalance struct {
	Round   uint64
	Balance uint64
}

func (kv *bPTreeKeyValuePage) isLeaf() bool {
	return kv.leaf
}

func (kv *bPTreeKeyValuePage) isPinned() bool {
	return kv.pinned
}

var bPTreeKeyValuePage_maxNumberOfElements uint64

// unmarshal deserializes the page into BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: 0 leaf=false 1 leaf=true
// byte 2: len (uint64)
// byte 10: keys
// byte ...: values
func (kv *bPTreeKeyValuePage) unmarshal(b []byte, pageSize int) {
	lastIndex := 0
	if b[lastIndex] == 0 {
		kv.leaf = false
	} else {
		kv.leaf = true
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
	// TODO: XXX this is wrong. number of keys can be different in non-leaf nodes
	for x := uint64(0); x < kv.numberOfKeys; x++ {
		kv.values = append(kv.values,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
}

func (kv *bPTreeKeyValuePage) maxNumberOfElements() uint64 {
	return bPTreeKeyValuePage_maxNumberOfElements
}

func (kv *bPTreeKeyValuePage) storageID() byte {
	return BPTREEKEYVALUEPAGE_STORAGE_ID
}

// marshal deserializes the page from BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: 0 leaf=false 1 leaf=true
// byte 2: len (uint64)
// byte ...: keys
// byte ...: values
func (kv *bPTreeKeyValuePage) marshal(b []byte, pageSize int) (numWritten int, err error) {
	offset := 0
	b[offset] = kv.storageID()
	offset++
	if kv.leaf {
		b[offset] = 1
	} else {
		b[offset] = 0
	}
	offset++
	binary.BigEndian.PutUint64(b[offset:offset+8], kv.numberOfKeys)
	offset += 8

	for _, x := range kv.keys {
		if offset+8 > pageSize {
			return 0, OversizeError{kv.numberOfKeys, "BPTreeKeyValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
	}
	for _, x := range kv.values {
		if offset+8 > pageSize {
			return 0, OversizeError{kv.numberOfKeys, "BPTreeKeyValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
	}
	return offset, nil
}

func (av *bPTreeAddressValuePage) isLeaf() bool {
	return av.leaf
}

func (av *bPTreeAddressValuePage) isPinned() bool {
	return av.pinned
}

// ReadFromDisk deserializes the page into BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: 0 leaf=false 1 leaf=true
// byte 2: len (uint64)
// byte 10: addresses
// byte ...: values
func (av *bPTreeAddressValuePage) unmarshal(b []byte, pageSize int) {
	lastIndex := 0
	if b[lastIndex] == 0 {
		av.leaf = false
	} else {
		av.leaf = true
	}
	lastIndex++

	numberOfAddresses := binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
	lastIndex += 8
	numberOfValues := binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
	lastIndex += 8

	av.addresses = make([]AddressKey, numberOfAddresses)
	av.values = make([]uint64, 0, numberOfValues)
	for x := uint64(0); x < numberOfAddresses; x++ {
		copy(av.addresses[x][:], b[lastIndex:lastIndex+ADDRESS_SIZE])
		lastIndex = lastIndex + ADDRESS_SIZE
	}
	for x := uint64(0); x < numberOfValues; x++ {
		av.values = append(av.values,
			binary.BigEndian.Uint64(b[lastIndex:lastIndex+8]))
		lastIndex = lastIndex + 8
	}
}

var bPTreeAddressValuePage_maxNumberOfElements uint64

func (av *bPTreeAddressValuePage) maxNumberOfElements() uint64 {
	return bPTreeAddressValuePage_maxNumberOfElements
}

func (av *bPTreeAddressValuePage) storageID() byte {
	return BPTREEKEYVALUEINDEXPAGE_STORAGE_ID
}

// marshal deserializes the page from BPTreeKeyValuePage page
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: 0 leaf=false 1 leaf=true
// byte 2: len addresses (uint64)
// byte 10:len keys (uint64)
// byte 18: addresses
// byte ...: values
func (av *bPTreeAddressValuePage) marshal(b []byte, pageSize int) (numWritten int, err error) {
	offset := 0
	b[offset] = av.storageID()
	offset++
	if av.leaf {
		b[offset] = 1
	} else {
		b[offset] = 0
	}
	offset++
	binary.BigEndian.PutUint64(b[offset:offset+8], uint64(len(av.addresses)))
	offset += 8
	binary.BigEndian.PutUint64(b[offset:offset+8], uint64(len(av.values)))
	offset += 8
	for _, x := range av.addresses {
		if offset+ADDRESS_SIZE > pageSize {
			return 0, OversizeError{uint64(len(av.addresses)), "BPTreeAddressValuePage"}
		}
		copy(b[offset:offset+ADDRESS_SIZE], x[:])
		offset += ADDRESS_SIZE
	}
	for _, x := range av.values {
		if offset+8 > pageSize {
			return 0, OversizeError{uint64(len(av.values)), "BPTreeAddressValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
	}
	return offset, nil
}

func (av *bPTreeAddressValuePage) searchAddress(bm *bufferManager, address AddressKey) (valuesAt FileOffsetPageIndex, err error) {
	var i int
	var a AddressKey
	for i = 0; i < len(av.addresses); i++ {
		a = av.addresses[i]
		if a.compare(&address) < 0 {
			continue
		}
		break
	}
	if i < len(av.addresses) && a.compare(&address) == 0 {
		// found the address
		if av.leaf {
			return FileOffsetPageIndex(av.values[i]), nil
		}
	}
	if av.isLeaf() {
		// did not find the address
		return 0, nil
	}
	if a.compare(&address) == 0 {
		i++ // if the key is the same as the account, it will be in the right child
	}
	nextPage, err := bm.readPage(fileOffset(av.values[i]))
	if err != nil {
		return 0, err
	}
	avNP, ok := nextPage.(*bPTreeAddressValuePage)
	if !ok {
		return 0, fmt.Errorf("searchAddress expected bPTreeAddressValuePage, got something else")
	}
	// continue the search recursively
	return avNP.searchAddress(bm, address)
}

func getEmptyBPTreeAddressValuePage(bm *bufferManager) (fileIndex fileOffset, p *bPTreeAddressValuePage, err error) {
	np := &bPTreeAddressValuePage{
		leaf:      true,
		addresses: make([]AddressKey, 0, (&bPTreeAddressValuePage{}).maxNumberOfElements()),
		values:    make([]uint64, 0, (&bPTreeAddressValuePage{}).maxNumberOfElements()),
		t:         int((&bPTreeAddressValuePage{}).maxNumberOfElements() / 2),
	}
	fo, err := bm.addNewPage(np)
	if err != nil {
		return 0, nil, err
	}
	return fo, np, nil
}

func (av *bPTreeAddressValuePage) insertAddressNonFull(bm *bufferManager, address AddressKey, round, value uint64) (addedAt FileOffsetPageIndex, err error) {
	i := len(av.addresses) - 1
	// if is leaf
	if av.leaf {
		// first check if the address is already here. If not, insert it
		existingIndex := 0
		for ; existingIndex < len(av.addresses) && (&address).compare(&av.addresses[existingIndex]) > 0; existingIndex++ {
		}
		if existingIndex < len(av.addresses) && (&address).compare(&av.addresses[existingIndex]) == 0 {
			// the address is already in the tree. Just update the pointer to the flat page
			index, err := addFlatKVPageValue(bm, FileOffsetPageIndex(av.values[existingIndex]), round, value)
			if err != nil {
				return 0, err
			}
			av.values[existingIndex] = uint64(index)
			return index, nil
		}

		// will add a value here. increase the size of the addresses
		av.addresses = append(av.addresses, AddressKey{})
		for ; i >= 0 && (&address).compare(&av.addresses[i]) < 0; i-- {
			av.addresses[i+1] = av.addresses[i]
		}
		av.addresses[i+1] = address
		index, err := addFlatKVPageValue(bm, 0, round, value)
		if err != nil {
			return 0, err
		}
		// increase the size of the values
		av.values = append(av.values, uint64(0))
		for j := len(av.values) - 1; j > i+1; j-- {
			av.values[j] = av.values[j-1]
		}
		av.values[i+1] = uint64(index)
		return index, nil
	}

	// if not leaf, find the child
	for ; i >= 0 && (&address).compare(&(av.addresses[i])) < 0; i-- {
	}
	i++
	chp, err := bm.readPage(fileOffset(av.values[i]))
	if err != nil {
		return 0, err
	}
	childPage, ok := chp.(*bPTreeAddressValuePage)
	if !ok {
		return 0, fmt.Errorf("insertAddressNonFull expected bPTreeAddressValuePage got something else")
	}
	// if the child is full
	if len(childPage.addresses) == childPage.t*2-1 {
		err = av.splitChild(bm, i, childPage)
		if err != nil {
			return 0, err
		}
		if (&address).compare(&av.addresses[i]) > 0 {
			i++
		}
	}
	return childPage.insertAddressNonFull(bm, address, round, value)
}

func (av *bPTreeAddressValuePage) splitChild(bm *bufferManager, i int, yPage *bPTreeAddressValuePage) (err error) {
	t := av.t
	zOffset, zPage, err := getEmptyBPTreeAddressValuePage(bm)
	if err != nil {
		return err
	}
	zPage.leaf = yPage.leaf
	// split y between y and z, and move the middle element to x (av)
	middleAddress := yPage.addresses[t-1] // middle address to move to x

	for j := 0; j < t-1; j++ {
		zPage.addresses = append(zPage.addresses, yPage.addresses[j+t])
	}
	if !yPage.leaf {
		for j := 0; j <= t-1; j++ {
			zPage.values = append(zPage.values, yPage.values[j+t])
		}
	} else {
		for j := 0; j < t-1; j++ {
			zPage.values = append(zPage.values, yPage.values[j+t])
		}
	}

	// resize to half it's size. The rest copied to z and one moved up
	yPage.values = yPage.values[:t]
	yPage.addresses = yPage.addresses[:t-1]

	// expand the array
	av.values = append(av.values, uint64(0))
	// push the values one step forward to make room for the new value at pos i+1
	for j := len(av.values) - 1; j > i+1; j-- {
		av.values[j] = av.values[j-1]
	}
	av.values[i+1] = uint64(zOffset)

	// expand the addresses by one
	av.addresses = append(av.addresses, AddressKey{})
	// push the addresses one step forward to make room for the new address at pos i
	for j := len(av.addresses) - 1; j > i; j-- {
		av.addresses[j] = av.addresses[j-1]
	}
	// set at i the middle address remove from y
	av.addresses[i] = middleAddress
	return nil
}

func (av *bPTreeAddressValuePage) insertAddress(bm *bufferManager, address AddressKey, round, value uint64) (addedAt FileOffsetPageIndex, err error) {
	t := av.t
	if len(av.addresses) < t*2-1 {
		// if has space:
		return av.insertAddressNonFull(bm, address, round, value)
	}

	sOffset, sPage, err := getEmptyBPTreeAddressValuePage(bm)
	if err != nil {
		return 0, err
	}
	sPage.leaf = false
	sPage.pinned = true
	av.pinned = false
	oldRootFileOffset := bm.sm.header.accountsHeadOffset
	// set the new node as the new root for the accounts
	bm.sm.header.accountsHeadOffset = sOffset
	sPage.values = append(sPage.values, uint64(oldRootFileOffset))
	err = sPage.splitChild(bm, 0, av)
	if err != nil {
		return 0, err
	}
	addedAt, err = sPage.insertAddressNonFull(bm, address, round, value)
	if err != nil {
		return 0, err
	}

	return
}

func unmarshal(b []byte, pageSize int) (page page, err error) {
	if b[0] == BPTREEKEYVALUEPAGE_STORAGE_ID {
		kv := &bPTreeKeyValuePage{}
		kv.unmarshal(b[1:], pageSize)
		return kv, nil
	}
	if b[0] == BPTREEKEYVALUEINDEXPAGE_STORAGE_ID {
		av := &bPTreeAddressValuePage{}
		av.unmarshal(b[1:], pageSize)
		return av, nil
	}
	if b[0] == FLATKEYVALUEPAGE_STORAGE_ID {
		fkv := &flatKeyValuePage{}
		fkv.unmarshal(b[1:], pageSize)
		return fkv, nil
	}
	return nil, fmt.Errorf("Unknown page type: %d", b[0])
}

func (a *AddressKey) compare(b *AddressKey) int {
	for i := 0; i < ADDRESS_SIZE; i++ {
		if a[i] > b[i] {
			return 1
		} else if a[i] < b[i] {
			return -1
		}
	}
	return 0
}

func (fp *flatKeyValuePage) isLeaf() bool {
	return true
}

func (fp *flatKeyValuePage) isPinned() bool {
	return fp.pinned
}

var flatKeyValuePage_maxNumberOfElements uint64

func (fp *flatKeyValuePage) maxNumberOfElements() uint64 {
	return flatKeyValuePage_maxNumberOfElements
}

func (fp *flatKeyValuePage) storageID() byte {
	return FLATKEYVALUEPAGE_STORAGE_ID
}

// Unmrashal the data
// byte 0: BPTreeKeyValuePage / BPTreeAddressValuePage
// byte 1: lastIndex
// ... interleaved round-value round-value...
func (fp *flatKeyValuePage) unmarshal(b []byte, pageSize int) {
	lastIndex := 0
	fp.nextAvailableArrayIndex = binary.BigEndian.Uint64(b[lastIndex : lastIndex+8])
	lastIndex += 8
	numberOfElements := fp.maxNumberOfElements()
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
func (fp *flatKeyValuePage) marshal(b []byte, pageSize int) (numWritten int, err error) {
	offset := 0
	b[offset] = fp.storageID()
	offset++
	binary.BigEndian.PutUint64(b[offset:offset+8], fp.nextAvailableArrayIndex)
	offset += 8
	for i, x := range fp.rounds {
		if offset+8 > pageSize {
			return 0, OversizeError{uint64(i), "FlatKeyValuePage"}
		}
		binary.BigEndian.PutUint64(b[offset:offset+8], x)
		offset += 8
		if offset+8 > pageSize {
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
func addFlatKVPageValue(bm *bufferManager, index FileOffsetPageIndex, round, value uint64) (newIndex FileOffsetPageIndex, err error) {
	fileIndex := index.getFileOffset(bm.sm.header.headerSize, bm.sm.header.pageSize)
	pageIndex := index.getElementIndexInPage(bm.sm.header.headerSize, bm.sm.header.pageSize)

	// if this is the first element of this run
	if index == 0 {
		// get the page of 1 elements
		bm.flatKeyValuePageMu.Lock()
		defer bm.flatKeyValuePageMu.Unlock()

		nextRunSize := uint64(1)
		nextRunPageFileIndex := bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)]

		// if the page for this size is not available, allocate a new page
		if nextRunPageFileIndex == 0 {
			newPage := makeFlatKeyValuePage()

			// add the new values (no counter or continued from). Missing counter indicates count of 1.
			newPage.rounds[0] = round
			newPage.values[0] = value

			// allocate nextRunSize slots here
			newPage.nextAvailableArrayIndex = nextRunSize

			newFileIndex, err := bm.addNewPage(newPage)
			if err != nil {
				return 0, err
			}
			// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
			newIndex := getFileOffsetPageIndex(newFileIndex, 0)

			// register this page in flatKVLastBucketOffset only if it can hold another of this size
			if newPage.nextAvailableArrayIndex+nextRunSize <= newPage.maxNumberOfElements() {
				bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)] = newFileIndex
			}
			return newIndex, nil
		}

		// otherwise, add to the existing page (this is the first element, no counter added)
		np, err := bm.readPage(nextRunPageFileIndex)
		if err != nil {
			return 0, err
		}
		newPage, ok := np.(*flatKeyValuePage)
		if !ok {
			return 0, fmt.Errorf("addFlatKVPageValue expected flatKeyValuePage got something else")
		}

		arrayIndex := newPage.nextAvailableArrayIndex

		// add the new values
		newPage.rounds[arrayIndex] = round
		newPage.values[arrayIndex] = value

		// allocate nextRunSize slots here
		newPage.nextAvailableArrayIndex = newPage.nextAvailableArrayIndex + nextRunSize

		// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
		newIndex := getFileOffsetPageIndex(nextRunPageFileIndex, int(arrayIndex))

		// register this page in flatKVLastBucketOffset only if it can hold another of this size
		if newPage.nextAvailableArrayIndex+nextRunSize <= newPage.maxNumberOfElements() {
			bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)] = nextRunPageFileIndex
		} else { // otherwise clear it
			bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)] = 0
		}
		return newIndex, nil
	}

	// Now consider the continuing values, i.e. 2 extra slots are used, one for continued from, one for the count
	page, err := bm.readPage(fileIndex)
	if err != nil {
		return 0, err
	}
	fkvp := page.(*flatKeyValuePage)

	numberOfValues := uint64(1)
	if fkvp.rounds[pageIndex] == 0 {
		// then the value is the count
		numberOfValues = fkvp.values[pageIndex]
	}

	// If the next slot is available, use it
	if numberOfValues > 1 && pageIndex+1 < len(fkvp.rounds) && fkvp.rounds[pageIndex+1] == 0 && fkvp.values[pageIndex+1] == 0 {
		fkvp.rounds[pageIndex] = round
		fkvp.values[pageIndex] = value

		fkvp.rounds[pageIndex+1] = 0
		fkvp.values[pageIndex+1] = numberOfValues + 1

		return index.addToElementIndex(1), nil
	}

	// If next slot is not available, find a new page
	bm.flatKeyValuePageMu.Lock()
	defer bm.flatKeyValuePageMu.Unlock()

	// next run size for this is numberOfValues*2
	nextRunSize := numberOfValues + 1
	maxNumElts := fkvp.maxNumberOfElements()
	if nextRunSize+2 > maxNumElts { // +2 : one for continued at, one for the size
		nextRunSize = maxNumElts
	}

	bucketIdx := getFlatKVLastBucketOffsetIndex(nextRunSize)
	nextRunPageFileIndex := bm.sm.header.flatKVLastBucketOffset[bucketIdx]

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
		newPage.nextAvailableArrayIndex = nextRunSize + 2 // +2 : one for continued at, one for the size

		newFileIndex, err := bm.addNewPage(newPage)
		if err != nil {
			return 0, err
		}
		// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
		newIndex := getFileOffsetPageIndex(newFileIndex, 2)

		// register this page in flatKVLastBucketOffset only if it can hold another of this size
		if newPage.nextAvailableArrayIndex+nextRunSize+2 <= fkvp.maxNumberOfElements() {
			bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)] = newFileIndex
		} else {
			// otherwise clear this page from the available buckets
			bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)] = 0
		}
		return newIndex, nil
	}

	// otherwise, add to the existing page
	np, err := bm.readPage(nextRunPageFileIndex)
	if err != nil {
		return 0, err
	}
	newPage, ok := np.(*flatKeyValuePage)
	if !ok {
		return 0, fmt.Errorf("addFlatKVPageValue expected flatKeyValuePage got something else")
	}
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
	newPage.nextAvailableArrayIndex = newPage.nextAvailableArrayIndex + nextRunSize + 2

	// the new index is where the number of values is stored. It is composed of the page fileIndex and the array index
	newIndex = getFileOffsetPageIndex(nextRunPageFileIndex, int(arrayIndex+2))

	// register this page in flatKVLastBucketOffset only if it can hold another of this size
	if newPage.nextAvailableArrayIndex+nextRunSize+2 <= fkvp.maxNumberOfElements() {
		bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)] = nextRunPageFileIndex
	} else { // otherwise clear it
		bm.sm.header.flatKVLastBucketOffset[getFlatKVLastBucketOffsetIndex(nextRunSize)] = 0
	}
	return newIndex, nil

}

func makeFlatKeyValuePage() *flatKeyValuePage {
	fkvp := &flatKeyValuePage{
		nextAvailableArrayIndex: FLATKEYVALUEPAGE_HEADER_SIZE,
	}
	fkvp.rounds = make([]uint64, fkvp.maxNumberOfElements())
	fkvp.values = make([]uint64, fkvp.maxNumberOfElements())
	return fkvp
}

func getRoundBalances(bm *bufferManager, offset FileOffsetPageIndex) (result []RoundBalance, moreHandle FileOffsetPageIndex, err error) {
	p, err := bm.readPage(offset.getFileOffset(bm.sm.header.headerSize, bm.sm.header.pageSize))
	if err != nil {
		return nil, 0, err
	}
	page, ok := p.(*flatKeyValuePage)
	if !ok {
		return nil, 0, fmt.Errorf("getRoundBalance expected flatKeyValuePage got something else")
	}

	pageOffset := offset.getElementIndexInPage(bm.sm.header.headerSize, bm.sm.header.pageSize)
	if page.rounds[pageOffset] != 0 {
		result := make([]RoundBalance, 1, 1)
		// there is only a single value
		result[0] = RoundBalance{Round: page.rounds[pageOffset], Balance: page.values[pageOffset]}
		return result, 0, nil
	}

	result = make([]RoundBalance, 0, page.values[pageOffset])
	result, err = getRoundBalancesRecursive(bm, offset, result)
	if err != nil {
		return nil, 0, nil
	}
	return result, 0, nil
}

func getRoundBalancesRecursive(bm *bufferManager, offset FileOffsetPageIndex, result []RoundBalance) (retResult []RoundBalance, err error) {
	p, err := bm.readPage(offset.getFileOffset(bm.sm.header.headerSize, bm.sm.header.pageSize))
	if err != nil {
		return nil, err
	}
	page, ok := p.(*flatKeyValuePage)
	if !ok {
		return nil, fmt.Errorf("getRoundBalance expected flatKeyValuePage got something else")
	}

	pageOffset := offset.getElementIndexInPage(bm.sm.header.headerSize, bm.sm.header.pageSize)
	if page.rounds[pageOffset] != 0 {
		// there is only a single value
		result = append(result, RoundBalance{Round: page.rounds[pageOffset], Balance: page.values[pageOffset]})
		return result, nil
	}

	for pageOffset = pageOffset - 1; page.rounds[pageOffset] != 0; pageOffset-- {
		result = append(result, RoundBalance{Round: page.rounds[pageOffset], Balance: page.values[pageOffset]})
	}
	return getRoundBalancesRecursive(bm, FileOffsetPageIndex(page.values[pageOffset]), result)
}

func getFlatKVLastBucketOffsetIndex(forSize uint64) int {
	x := 0
	for forSize > 0 {
		forSize = forSize >> 1
		x++
	}
	x--
	return x
}
