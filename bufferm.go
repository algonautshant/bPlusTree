package bptree

import (
	"sync"
)

type BufferManager struct {
	pool                   []Page
	dirty                  []bool
	bufferIndexToFileIndex map[uint64]uint64
	fileIndexToBufferIndex map[uint64]uint64
	mru                    mru

	sm *StorageManager
	mu sync.Mutex
}

func getBufferManager(sm *StorageManager) BufferManager {
	bm := BufferManager{
		pool:                   make([]Page, 0, NUMBER_OF_BUFFER_POOL_PAGES),
		dirty:                  make([]bool, NUMBER_OF_BUFFER_POOL_PAGES),
		fileIndexToBufferIndex: make(map[uint64]uint64, NUMBER_OF_BUFFER_POOL_PAGES),
		bufferIndexToFileIndex: make(map[uint64]uint64, NUMBER_OF_BUFFER_POOL_PAGES),
		sm:                     sm,
		mru:                    getMru(),
	}
	return bm
}

// ReadPage reads a page if not in the pool, and returns the pool index
func (mp *BufferManager) ReadPage(sm *StorageManager, fileIndex uint64) uint64 {
	if i, found := mp.fileIndexToBufferIndex[fileIndex]; found {
		mp.mru.updateUse(i)
		return i
	}
	return 0
}

func (bm *BufferManager) AddNewPage(page Page) error {
	fileIndex, err := bm.sm.newPage()
	if err != nil {
		return err
	}
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if len(bm.pool) < NUMBER_OF_BUFFER_POOL_PAGES {
		bm.pool = append(bm.pool, page)
		bm.dirty[len(bm.pool)-1] = true
		bm.mru.addUse(uint64(len(bm.pool)-1))
		bm.fileIndexToBufferIndex[fileIndex] = uint64(len(bm.pool) - 1)
		bm.bufferIndexToFileIndex[uint64(len(bm.pool)-1)] = fileIndex
		return nil
	} else {
		freePoolIdx, err := bm.evictPage()
		if err != nil {
			return err
		}
		bm.pool[freePoolIdx] = page
		bm.dirty[freePoolIdx] = true
		bm.mru.addUse(freePoolIdx)
		bm.fileIndexToBufferIndex[fileIndex] = freePoolIdx
		bm.bufferIndexToFileIndex[freePoolIdx] = fileIndex
		return nil
	}
}

func (bm *BufferManager) evictPage() (freePoolIdx uint64, err error) {
	// expects the lock is already held
	pageToEvict := bm.mru.removeLeastUsed() // todo: take care of error/undo mru
	if bm.dirty[pageToEvict] {
		err := bm.sm.writePage(bm.pool[pageToEvict], bm.bufferIndexToFileIndex[pageToEvict])
		if err != nil {
			return 0, err
		}
		bm.dirty[pageToEvict] = false
	}
	delete(bm.fileIndexToBufferIndex, bm.bufferIndexToFileIndex[pageToEvict])
	delete(bm.bufferIndexToFileIndex, pageToEvict)
	return pageToEvict, nil
}
