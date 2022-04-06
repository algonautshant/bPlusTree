package bptree

type mru struct {
	mruListHead           *llist
	mruListTail           *llist
	bufferIndexToListNode map[uint64]*llist
}

type llist struct {
	next        *llist
	prev        *llist
	bufferIndex uint64
}

func getMru() mru {
	head := &llist{}
	tail := &llist{}
	head.next = tail
	head.prev = nil
	tail.next = nil
	tail.prev = head
	u := mru{
		mruListHead:           head,
		mruListTail:           tail,
		bufferIndexToListNode: make(map[uint64]*llist),
	}

	return u
}

func (u *mru) addUse(bufferIndex uint64) {
	node := &llist{bufferIndex: bufferIndex}
	u.bufferIndexToListNode[bufferIndex] = node

	// add it to the beginning
	node.prev = u.mruListHead
	node.next = u.mruListHead.next
	u.mruListHead.next.prev = node
	u.mruListHead.next = node
}

func (u *mru) removeUse(bufferIndex uint64) {
	node := u.bufferIndexToListNode[bufferIndex]
	// remove the node
	node.prev.next = node.next
	node.next.prev = node.prev
	delete(u.bufferIndexToListNode, bufferIndex)
}

func (u *mru) updateUse(bufferIndex uint64) {
	node := u.bufferIndexToListNode[bufferIndex]
	// remove the node
	node.prev.next = node.next
	node.next.prev = node.prev

	// add it to the beginning
	node.prev = u.mruListHead
	node.next = u.mruListHead.next
	u.mruListHead.next.prev = node
	u.mruListHead.next = node
}

func (u *mru) removeLeastUsed() uint64{
	luBufferIdx := u.mruListTail.bufferIndex
	u.removeUse(luBufferIdx)
	return luBufferIdx
}
