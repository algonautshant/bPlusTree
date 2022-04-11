package bptree

import (
	"errors"
	"os"
	"fmt"
)

const (
	NUMBER_OF_KEYS_PER_PAGE     = 126
	NUMBER_OF_BUFFER_POOL_PAGES = 4
)

type OversizeError struct {
	elements uint64
	caller   string
}

func (ose OversizeError) Error() string {
	return fmt.Sprintf("Page overflow %d elements in %s", ose.elements, ose.caller)
}

type catalog struct {
	dbFilename string
	sm         *storageManager
	bm         *bufferManager
}

// initializeCatalog initializes a new storage. It writes the first page
// with page 0 at index 0 in the file.
func initializeCatalog(filename string, createNew bool, pageSize uint64, numberOfBufferpoolPages int) (cat *catalog, err error) {
	
	sm, err := openStorageManager(filename)
	if createNew && errors.Is(err, os.ErrNotExist) {
		sm, err = initStorageManager(filename, pageSize)		
	}
	if err != nil {
		return nil, err
	}
	catPage0 := initCatalogPages()
	sm.writeFirstPage(catPage0)
	bm := getBufferManager(sm, numberOfBufferpoolPages)

	fileIndex, headAccountPage, err  := getEmptyBPTreeAddressValuePage(bm)
	if err != nil {
		return nil, err
	}
	headAccountPage.pinned = true
	sm.header.accountsHeadOffset = fileIndex
	return &catalog{
		dbFilename: filename,
		sm:         sm,
		bm:         bm,
	}, nil

}

func initCatalogPages() page {
	page0 := bPTreeKeyValuePage{
		leaf:       true,
		numberOfKeys: 1,
	}
	page0.keys = append(page0.keys, 0)
	page0.values = append(page0.keys, 0)
	return &page0
}

func (c *catalog) close() error {
	err := c.bm.close()
	if err != nil {
		return err
	}
	return c.sm.close()
}
