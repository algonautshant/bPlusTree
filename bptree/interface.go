package bptree

func InitializeIndexer(filename string, createNew bool) (indexer *Indexer, err error) {
	cat, err := initializeCatalog(filename, createNew)
	if err != nil {
		return nil, err
	}
	indxr := &Indexer{cat: cat}
	return indxr, nil
}

