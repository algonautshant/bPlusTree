package bptree

import (
	"fmt"
)

const (
	NUMBER_OF_KEYS_PER_PAGE     = 126
	NUMBER_OF_BUFFER_POOL_PAGES = 400000
)

type OversizeError struct {
	elements uint64
	caller   string
}

func (ose OversizeError) Error() string {
	return fmt.Sprintf("Page overflow %d elements in %s", ose.elements, ose.caller)
}

type Catalog struct {
	dbFilename string
	sm         StorageManager
	bm         BufferManager
}

// InitializeCatalog initializes a new storage. It writes the first page
// with page 0 at index 0 in the file.
func InitializeCatalog(filename string) (cat *Catalog, err error) {
	sm, err := initStorageManager(filename)
	if err != nil {
		return nil, err
	}
	catPage0 := initCatalogPages()
	sm.writeFirstPage(catPage0)
	bm := BufferManager{}
	return &Catalog{
		dbFilename: filename,
		sm:         sm,
		bm:         bm,
	}, nil

}

func initCatalogPages() Page {
	page0 := BPTreeKeyValuePage{
		isLeaf:       true,
		numberOfKeys: 1,
	}
	page0.keys = append(page0.keys, 0)
	page0.values = append(page0.keys, 0)
	return &page0
}

func initAccountsPages() Page {
	page1 := BPTreeAddressValuePage{
		isLeaf: true,
		numberOfAddresses:    0,
	}
	return &page1
}

func (c *Catalog) close() error {
	err := c.bm.close()
	if err != nil {
		return err
	}
	return c.sm.close()
}
