package bptree

import (
	"sync"
)

type bufferManager struct {
	pool                   []page
	dirty                  []bool
	bufferIndexToFileOffset map[uint64]fileOffset
	fileOffsetToBufferIndex map[fileOffset]uint64
	mru                    mru

	sm *storageManager
	mu sync.Mutex

	flatKeyValuePageMu sync.Mutex
}

func getBufferManager(sm *storageManager) bufferManager {
	bm := bufferManager{
		pool:                   make([]page, 0, NUMBER_OF_BUFFER_POOL_PAGES),
		dirty:                  make([]bool, NUMBER_OF_BUFFER_POOL_PAGES),
		fileOffsetToBufferIndex: make(map[fileOffset]uint64, NUMBER_OF_BUFFER_POOL_PAGES),
		bufferIndexToFileOffset: make(map[uint64]fileOffset, NUMBER_OF_BUFFER_POOL_PAGES),
		sm:                     sm,
		mru:                    getMru(),
	}
	return bm
}

// readPage reads a page if not in the pool, and returns the pool index
func (bm *bufferManager) readPage(fileIndex fileOffset) page {
	if i, found := bm.fileOffsetToBufferIndex[fileIndex]; found {
		bm.mru.updateUse(i)
		return bm.pool[i]
	}
	page, err := bm.sm.readPage(fileIndex)
	if err != nil {
		// TODO log the error
		return nil
	}
	return page
}

func (bm *bufferManager) addNewPage(page page) (fileIndex fileOffset, err error) {
	fileIndex, err = bm.sm.newPage()
	if err != nil {
		return 0, err
	}
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if len(bm.pool) < NUMBER_OF_BUFFER_POOL_PAGES {
		bm.pool = append(bm.pool, page)
		bm.dirty[len(bm.pool)-1] = true
		bm.mru.addUse(uint64(len(bm.pool) - 1))
		bm.fileOffsetToBufferIndex[fileIndex] = uint64(len(bm.pool) - 1)
		bm.bufferIndexToFileOffset[uint64(len(bm.pool)-1)] = fileIndex
		return
	} else {
		freePoolIdx, err := bm.evictPage()
		if err != nil {
			return 0, err
		}
		bm.pool[freePoolIdx] = page
		bm.dirty[freePoolIdx] = true
		bm.mru.addUse(freePoolIdx)
		bm.fileOffsetToBufferIndex[fileIndex] = freePoolIdx
		bm.bufferIndexToFileOffset[freePoolIdx] = fileIndex
		return 0, nil
	}
}

func (bm *bufferManager) evictPage() (freePoolIdx uint64, err error) {
	// expects the lock is already held
	pageToEvict := bm.mru.removeLeastUsed() // todo: take care of error/undo mru
	if bm.dirty[pageToEvict] {
		err := bm.sm.writePage(bm.pool[pageToEvict], bm.bufferIndexToFileOffset[pageToEvict])
		if err != nil {
			return 0, err
		}
		bm.dirty[pageToEvict] = false
	}
	delete(bm.fileOffsetToBufferIndex, bm.bufferIndexToFileOffset[pageToEvict])
	delete(bm.bufferIndexToFileOffset, pageToEvict)
	return pageToEvict, nil
}

func (bm *bufferManager) close() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for len(bm.bufferIndexToFileOffset) > 0 {
		_, err := bm.evictPage()
		if err != nil {
			return err
		}
	}
	return nil
}
