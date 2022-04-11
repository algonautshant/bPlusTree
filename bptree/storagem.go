package bptree

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// FileOffset is where the page is written in the file
// It is HEADER_SIZE + PAGE_SIZE*(#pages before)
type fileOffset uint64

// FileOffsetPageIndex combines the page location in the file
// and an offset inside the page. The offset inside the page
// corresponds to the element array index and not actual byte index.
type FileOffsetPageIndex uint64

// GetElementIndexInPage returns the element array index inside the page
func (fopi FileOffsetPageIndex) getElementIndexInPage(headerSize, pageSize uint64) int {
	return int((uint64(fopi) - headerSize) % pageSize)
}

// GetFileOffset returns the offset of the page inside the file
func (fopi FileOffsetPageIndex) getFileOffset(headerSize, pageSize uint64) fileOffset {
	return fileOffset(uint64(fopi) - uint64(fopi.getElementIndexInPage(headerSize, pageSize)))
}

func getFileOffsetPageIndex(fileOffset fileOffset, elementIndex int) FileOffsetPageIndex {
	return FileOffsetPageIndex(uint64(fileOffset) + uint64(elementIndex))
}

func (fopi FileOffsetPageIndex) addToElementIndex(a int) FileOffsetPageIndex {
	return FileOffsetPageIndex(uint64(fopi) + uint64(a))
}

type storageManager struct {
	fd     *os.File
	header header
	mu     sync.Mutex
}

type header struct {
	pageSize               uint64
	headerSize             uint64
	flatTKBucketOffsetSize uint64
	numberOfPages          uint64
	newPageOffset          fileOffset
	firstPageOffset        fileOffset

	accountsHeadOffset fileOffset

	flatKVLastBucketOffset []fileOffset
}

func (sm *storageManager) writeHeader() error {
	hBuff := make([]byte, sm.header.headerSize, sm.header.headerSize)
	offset := 0
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], sm.header.pageSize)
	offset += 8
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], sm.header.headerSize)
	offset += 8
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], sm.header.flatTKBucketOffsetSize)
	offset += 8

	binary.BigEndian.PutUint64(hBuff[offset:offset+8], sm.header.numberOfPages)
	offset += 8
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], uint64(sm.header.newPageOffset))
	offset += 8
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], uint64(sm.header.accountsHeadOffset))
	offset += 8

	for _, x := range sm.header.flatKVLastBucketOffset {
		binary.BigEndian.PutUint64(hBuff[offset:offset+8], uint64(x))
		offset += 8
	}

	n, err := sm.fd.WriteAt(hBuff[:], 0)
	if err != nil {
		return fmt.Errorf("storageManager writeHeader: WriteAt error at 0: %w", err)
	}
	if n != int(sm.header.headerSize) {
		return fmt.Errorf("storageManager writeHeader: wrote %d instead of %d", n, sm.header.headerSize)
	}
	return nil
}

func (sm *storageManager) readHeader() error {
	sizes := make([]byte, 16)
	n, err := sm.fd.ReadAt(sizes[:], 0)
	if err != nil {
		return fmt.Errorf("storageManager readHeader: ReadAt 0 error: %w", err)
	}
	if n != int(16) {
		return fmt.Errorf("storageManager readHeader: should read %d read %d", 16, n)
	}
	offset := 0
	sm.header.pageSize = binary.BigEndian.Uint64(sizes[offset : offset+8])
	offset += 8
	sm.header.headerSize = binary.BigEndian.Uint64(sizes[offset : offset+8])

	hBuff := make([]byte, sm.header.headerSize-16)
	n, err = sm.fd.ReadAt(hBuff[:], 16)
	if err != nil {
		return fmt.Errorf("storageManager readHeader: ReadAt 0 error: %w", err)
	}
	if n != int(sm.header.headerSize-16) {
		return fmt.Errorf("storageManager readHeader: should read %d read %d", sm.header.headerSize-16, n)
	}

	offset = 0
	sm.header.flatTKBucketOffsetSize = binary.BigEndian.Uint64(hBuff[offset : offset+8])
	offset += 8

	sm.header.numberOfPages = binary.BigEndian.Uint64(hBuff[offset : offset+8])
	offset += 8
	sm.header.newPageOffset = fileOffset(binary.BigEndian.Uint64(hBuff[offset : offset+8]))
	offset += 8
	sm.header.accountsHeadOffset = fileOffset(binary.BigEndian.Uint64(hBuff[offset : offset+8]))
	offset += 8

	for x := 0; x < int(sm.header.flatTKBucketOffsetSize); x++ {
		sm.header.flatKVLastBucketOffset = append(sm.header.flatKVLastBucketOffset,
			fileOffset(binary.BigEndian.Uint64(hBuff[offset:offset+8])))
		offset += 8
	}
	return nil
}

func openStorageManager(filename string) (sm *storageManager, err error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return nil, fmt.Errorf("storagem openStorageManager: OpenFile %s error: %w", filename, err)
	}
	sm = &storageManager{fd: f}
	err = sm.readHeader()
	if err != nil {
		return nil, err
	}
	// TODO: write these in the header, and veryfiy they correspond to the code when loaded
	bPTreeKeyValuePage_maxNumberOfElements = (sm.header.pageSize - 10) / 2 / 8
	flatKeyValuePage_maxNumberOfElements = (sm.header.pageSize - FLATKEYVALUEPAGE_HEADER_SIZE) / FLATKEYVALUEPAGE_ELEMENT_SIZE
	bPTreeAddressValuePage_maxNumberOfElements = (sm.header.pageSize - BPTREEADDRESSVALUEPAGE_HEADER_SIZE) / BPTREEADDRESSVALUEPAGE_ELEMENT_SIZE
	return sm, nil
}

func initStorageManager(filename string, pageSize uint64) (sm *storageManager, err error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("storagem initStorageManager: OpenFile %s error: %w", filename, err)
	}
	headerSize := uint64(48 + 8 * getFlatKVLastBucketOffsetIndex(pageSize))  // 32 + 8*flatTKBucketOffsetSize
	flatTKBucketOffsetSize := uint64(getFlatKVLastBucketOffsetIndex(pageSize)) //ceil(log2(PAGE_SIZE))
	sm = &storageManager{fd: f}
	sm.header = header{
		pageSize:               pageSize,
		headerSize:             headerSize,
		flatTKBucketOffsetSize: flatTKBucketOffsetSize,
		numberOfPages:          0,
		newPageOffset:          fileOffset(headerSize),
		firstPageOffset:        fileOffset(headerSize),
		flatKVLastBucketOffset: make([]fileOffset, flatTKBucketOffsetSize, flatTKBucketOffsetSize),
	}
	err = sm.writeHeader()
	if err != nil {
		return nil, err
	}
	bPTreeKeyValuePage_maxNumberOfElements = (pageSize - 10) / 2 / 8
	flatKeyValuePage_maxNumberOfElements = (pageSize - FLATKEYVALUEPAGE_HEADER_SIZE) / FLATKEYVALUEPAGE_ELEMENT_SIZE
	bPTreeAddressValuePage_maxNumberOfElements = (pageSize - BPTREEADDRESSVALUEPAGE_HEADER_SIZE) / BPTREEADDRESSVALUEPAGE_ELEMENT_SIZE

	return sm, nil
}

func (sm *storageManager) newPage() (newPageFileOffset fileOffset, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.header.numberOfPages++
	newPageFileOffset = sm.header.newPageOffset
	sm.header.newPageOffset += fileOffset(sm.header.pageSize)
	err = sm.writeHeader()
	if err != nil {
		return 0, err
	}
	return newPageFileOffset, nil
}

func (sm *storageManager) writeFirstPage(page page) error {
	np, err := sm.newPage()
	if err != nil {
		return err
	}
	if np != sm.header.firstPageOffset {
		return fmt.Errorf("storageManager writeFirstPage: the first page should have offset %d but got %d", sm.header.headerSize, np)
	}
	return nil
}

func (sm *storageManager) writePage(page page, fileOffset fileOffset) error {
	buffer := make([]byte, sm.header.pageSize, sm.header.pageSize)
	bytes, err := page.marshal(buffer, int(sm.header.pageSize))
	if err != nil {
		return err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	n, err := sm.fd.WriteAt(buffer[:bytes], int64(fileOffset))
	if err != nil {
		return fmt.Errorf("storageManager writePage: WriteAt %d error: %w", fileOffset, err)
	}
	if n != bytes {
		return fmt.Errorf("storageManager writePage: failed to write page at %d: wrote %d / %d",
			fileOffset, n, bytes)
	}
	return nil
}

func (sm *storageManager) readFirstPage() (page page, err error) {
	return sm.readPage(sm.header.firstPageOffset)
}

func (sm *storageManager) readPage(fileOffset fileOffset) (page page, err error) {
	buffer := make([]byte, int(sm.header.pageSize), int(sm.header.pageSize))

	sm.mu.Lock()
	defer sm.mu.Unlock()
	n, err := sm.fd.ReadAt(buffer[:], int64(fileOffset))
	buffer = buffer[:n]
	if err != nil {
		if err.Error() != "EOF" {
			return nil, fmt.Errorf("storageManager readPage: ReadAt %d error: %w", fileOffset, err)
		}
	}
	return unmarshal(buffer[:], int(sm.header.pageSize))
}

func (sm *storageManager) close() error {
	err := sm.fd.Close()
	return err
}
