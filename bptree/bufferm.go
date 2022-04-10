package bptree

import (
	"fmt"
	"sync"
)

type bufferManager struct {
	pool                   []page
	dirty                  []bool
	bufferIndexToFileOffset map[int]fileOffset
	fileOffsetToBufferIndex map[fileOffset]int
	mru                    mru

	sm *storageManager
	mu sync.Mutex

	flatKeyValuePageMu sync.Mutex
}

func getBufferManager(sm *storageManager) *bufferManager {
	bm := &bufferManager{
		pool:                   make([]page, 0, NUMBER_OF_BUFFER_POOL_PAGES),
		dirty:                  make([]bool, NUMBER_OF_BUFFER_POOL_PAGES),
		fileOffsetToBufferIndex: make(map[fileOffset]int, NUMBER_OF_BUFFER_POOL_PAGES),
		bufferIndexToFileOffset: make(map[int]fileOffset, NUMBER_OF_BUFFER_POOL_PAGES),
		sm:                     sm,
		mru:                    getMru(),
	}
	return bm
}

// readPage reads a page if not in the pool, and returns the pool index
func (bm *bufferManager) readPage(fileIndex fileOffset) (page, error) {
	if i, found := bm.fileOffsetToBufferIndex[fileIndex]; found {
		err := bm.mru.updateUse(i)
		if err != nil {
			return nil, err
		}
		return bm.pool[i], nil
	}
	page, err := bm.sm.readPage(fileIndex)
	if err != nil {
		return nil, err
	}
	return page, err
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
		bm.mru.addUse(len(bm.pool) - 1)
		bm.fileOffsetToBufferIndex[fileIndex] = len(bm.pool) - 1
		bm.bufferIndexToFileOffset[len(bm.pool)-1] = fileIndex
		return
	} else {
		freePoolIdx, err := bm.evictPage(false)
		if err != nil {
			return 0, err
		}
		bm.pool[freePoolIdx] = page
		bm.dirty[freePoolIdx] = true
		bm.mru.addUse(freePoolIdx)
		bm.fileOffsetToBufferIndex[fileIndex] = freePoolIdx
		bm.bufferIndexToFileOffset[freePoolIdx] = fileIndex
		return fileIndex, nil
	}
}

func (bm *bufferManager) evictPage(forcePinned bool) (freePoolIdx int, err error) {
	// expects the lock is already held
	pageToEvict := 0
	for {
		// todo : take care of the case where all the pages are pinned
		pageToEvict = bm.mru.removeLeastUsed() // todo: take care of error/undo mru
		if !forcePinned && bm.pool[pageToEvict].isPinned() {
			bm.mru.addUse(pageToEvict)
			continue
		}
		break
	}
	if bm.dirty[pageToEvict] {
		err := bm.sm.writePage(bm.pool[pageToEvict], bm.bufferIndexToFileOffset[pageToEvict])
		if err != nil {
			return 0, fmt.Errorf("evictPage: writePage error: %v", err)
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
		_, err := bm.evictPage(true)
		if err != nil {
			return err
		}
	}
	return nil
}
