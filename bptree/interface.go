package bptree

func InitializeIndexer(filename string, createNew bool, pageSize uint64, numberOfBufferpoolPages int) (indexer *Indexer, err error) {
	cat, err := initializeCatalog(filename, createNew, pageSize, numberOfBufferpoolPages)
	if err != nil {
		return nil, err
	}
	indxr := &Indexer{cat: cat}
	return indxr, nil
}

