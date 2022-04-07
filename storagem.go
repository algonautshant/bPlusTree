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
type FileOffset uint64

// FileOffsetPageIndex combines the page location in the file
// and an offset inside the page. The offset inside the page
// corresponds to the element array index and not actual byte index.
type FileOffsetPageIndex uint64

// GetElementIndexInPage returns the element array index inside the page
func (fopi FileOffsetPageIndex) GetElementIndexInPage() int {
	return int((uint64(fopi) - HEADER_SIZE) % PAGE_SIZE)
}

// GetFileOffset returns the offset of the page inside the file
func (fopi FileOffsetPageIndex) GetFileOffset() FileOffset {
	return FileOffset(uint64(fopi) - uint64(fopi.GetElementIndexInPage()))
}

func GetFileOffsetPageIndex(fileOffset FileOffset, elementIndex int) FileOffsetPageIndex {
	return FileOffsetPageIndex(uint64(fileOffset) + uint64(elementIndex))
}

func (fopi FileOffsetPageIndex) AddToElementIndex(a int) FileOffsetPageIndex {
	return FileOffsetPageIndex(uint64(fopi) + uint64(a))
}

type StorageManager struct {
	fd     *os.File
	header header
	mu     sync.Mutex
}

type header struct {
	pageSize        uint64
	numberOfPages   uint64
	nextPageOffset  FileOffset
	firstPageOffset FileOffset

	flatKVFileEndOffset []FileOffset
}

func (sm *StorageManager) writeHeader() error {
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

func (sm *StorageManager) readHeader() error {
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
	sm.header.nextPageOffset = FileOffset(binary.BigEndian.Uint64(hBuff[offset : offset+8]))
	offset += 8

	for x := 0; x < 11; x++ {
		sm.header.flatKVFileEndOffset = append(sm.header.flatKVFileEndOffset,
			FileOffset(binary.BigEndian.Uint64(hBuff[offset:offset+8])))
		offset += 8
	}
	return nil
}

func openStorageManager(filename string) (sm *StorageManager, err error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	sm = &StorageManager{fd: f}
	err = sm.readHeader()
	if err != nil {
		return nil, err
	}
	return sm, nil
}

func initStorageManager(filename string) (sm StorageManager, err error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return StorageManager{}, err
	}
	sm = StorageManager{fd: f}
	sm.header = header{
		pageSize:                PAGE_SIZE,
		numberOfPages:           0,
		nextPageOffset:          HEADER_SIZE,
		firstPageOffset:         HEADER_SIZE,
		flatKVFileEndOffset: make([]FileOffset, 0),
	}
	err = sm.writeHeader()
	if err != nil {
		return StorageManager{}, err
	}
	return sm, nil
}

func (sm *StorageManager) newPage() (fileOffset FileOffset, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.header.numberOfPages++
	fileOffset = sm.header.nextPageOffset
	sm.header.nextPageOffset += FileOffset(sm.header.pageSize)
	err = sm.writeHeader()
	if err != nil {
		return 0, err
	}
	return fileOffset, nil
}

func (sm *StorageManager) writeFirstPage(page Page) error {
	np, err := sm.newPage()
	if err != nil {
		return err
	}
	if np != sm.header.firstPageOffset {
		return fmt.Errorf("the first page should have offset %d but got %d", HEADER_SIZE, np)
	}
	return nil
}

func (sm *StorageManager) writePage(page Page, fileOffset FileOffset) error {
	var buffer [PAGE_SIZE]byte
	bytes, err := page.Marshal(buffer[:])
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

func (sm *StorageManager) readFirstPage() (page Page, err error) {
	return sm.readPage(sm.header.firstPageOffset)
}

func (sm *StorageManager) readPage(fileOffset FileOffset) (page Page, err error) {
	var buffer [PAGE_SIZE]byte

	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, err = sm.fd.ReadAt(buffer[:], int64(fileOffset))
	if err != nil {
		return nil, err
	}
	return Unmarshal(buffer[:])
}

func (sm *StorageManager) close() error {
	err := sm.fd.Close()
	return err
}
