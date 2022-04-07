package bptree

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const (
	PAGE_SIZE   = 4096
	HEADER_SIZE = 36 // 24 + log2(PAGE_SIZE)
)

// FileOffset is where the page is written in the file
// It is HEADER_SIZE + PAGE_SIZE*(#pages before)
type fileOffset uint64

// FileOffsetPageIndex combines the page location in the file
// and an offset inside the page. The offset inside the page
// corresponds to the element array index and not actual byte index.
type fileOffsetPageIndex uint64

// GetElementIndexInPage returns the element array index inside the page
func (fopi fileOffsetPageIndex) getElementIndexInPage() int {
	return int((uint64(fopi) - HEADER_SIZE) % PAGE_SIZE)
}

// GetFileOffset returns the offset of the page inside the file
func (fopi fileOffsetPageIndex) getFileOffset() fileOffset {
	return fileOffset(uint64(fopi) - uint64(fopi.getElementIndexInPage()))
}

func getFileOffsetPageIndex(fileOffset fileOffset, elementIndex int) fileOffsetPageIndex {
	return fileOffsetPageIndex(uint64(fileOffset) + uint64(elementIndex))
}

func (fopi fileOffsetPageIndex) addToElementIndex(a int) fileOffsetPageIndex {
	return fileOffsetPageIndex(uint64(fopi) + uint64(a))
}

type storageManager struct {
	fd     *os.File
	header header
	mu     sync.Mutex
}

type header struct {
	pageSize        uint64
	numberOfPages   uint64
	nextPageOffset  fileOffset
	firstPageOffset fileOffset

	flatKVFileEndOffset []fileOffset
}

func (sm *storageManager) writeHeader() error {
	var hBuff [HEADER_SIZE]byte
	offset := 0
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], sm.header.pageSize)
	offset += 8
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], sm.header.numberOfPages)
	offset += 8
	binary.BigEndian.PutUint64(hBuff[offset:offset+8], uint64(sm.header.nextPageOffset))
	offset += 8

	for _, x := range sm.header.flatKVFileEndOffset {
		binary.BigEndian.PutUint64(hBuff[offset:offset+8], uint64(x))
		offset += 8
	}

	n, err := sm.fd.WriteAt(hBuff[:], 0)
	if err != nil {
		return err
	}
	if n != HEADER_SIZE {
		return fmt.Errorf("failed to write the header")
	}
	return nil
}

func (sm *storageManager) readHeader() error {
	var hBuff [HEADER_SIZE]byte
	n, err := sm.fd.ReadAt(hBuff[:], 0)
	if err != nil {
		return err
	}
	if n != HEADER_SIZE {
		return fmt.Errorf("failed to read the header")
	}

	offset := 0
	sm.header.pageSize = binary.BigEndian.Uint64(hBuff[offset : offset+8])
	offset += 8
	sm.header.numberOfPages = binary.BigEndian.Uint64(hBuff[offset : offset+8])
	offset += 8
	sm.header.nextPageOffset = fileOffset(binary.BigEndian.Uint64(hBuff[offset : offset+8]))
	offset += 8

	for x := 0; x < 11; x++ {
		sm.header.flatKVFileEndOffset = append(sm.header.flatKVFileEndOffset,
			fileOffset(binary.BigEndian.Uint64(hBuff[offset:offset+8])))
		offset += 8
	}
	return nil
}

func openStorageManager(filename string) (sm *storageManager, err error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	sm = &storageManager{fd: f}
	err = sm.readHeader()
	if err != nil {
		return nil, err
	}
	return sm, nil
}

func initStorageManager(filename string) (sm *storageManager, err error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	sm = &storageManager{fd: f}
	sm.header = header{
		pageSize:                PAGE_SIZE,
		numberOfPages:           0,
		nextPageOffset:          HEADER_SIZE,
		firstPageOffset:         HEADER_SIZE,
		flatKVFileEndOffset: make([]fileOffset, 0),
	}
	err = sm.writeHeader()
	if err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *storageManager) newPage() (newPageFileOffset fileOffset, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.header.numberOfPages++
	newPageFileOffset = sm.header.nextPageOffset
	sm.header.nextPageOffset += fileOffset(sm.header.pageSize)
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
		return fmt.Errorf("the first page should have offset %d but got %d", HEADER_SIZE, np)
	}
	return nil
}

func (sm *storageManager) writePage(page page, fileOffset fileOffset) error {
	var buffer [PAGE_SIZE]byte
	bytes, err := page.marshal(buffer[:])
	if err != nil {
		return err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	n, err := sm.fd.WriteAt(buffer[:bytes], int64(fileOffset))
	if err != nil {
		return err
	}
	if n != bytes {
		return fmt.Errorf("failed writing page at %d: wrote %d / %d",
			fileOffset, n, bytes)
	}
	return nil
}

func (sm *storageManager) readFirstPage() (page page, err error) {
	return sm.readPage(sm.header.firstPageOffset)
}

func (sm *storageManager) readPage(fileOffset fileOffset) (page page, err error) {
	var buffer [PAGE_SIZE]byte

	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, err = sm.fd.ReadAt(buffer[:], int64(fileOffset))
	if err != nil {
		return nil, err
	}
	return unmarshal(buffer[:])
}

func (sm *storageManager) close() error {
	err := sm.fd.Close()
	return err
}
