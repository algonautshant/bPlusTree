package bptree

import (
	"fmt"
)

type mru struct {
	mruListHead           *llist
	mruListTail           *llist
	bufferIndexToListNode map[int]*llist
}

type llist struct {
	next        *llist
	prev        *llist
	bufferIndex int
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
		bufferIndexToListNode: make(map[int]*llist),
	}

	return u
}

func (u *mru) addUse(bufferIndex int) {
	node := &llist{bufferIndex: bufferIndex}
	u.bufferIndexToListNode[bufferIndex] = node

	// add it to the beginning
	node.prev = u.mruListHead
	node.next = u.mruListHead.next
	u.mruListHead.next.prev = node
	u.mruListHead.next = node
}

func (u *mru) removeUse(bufferIndex int) error{
	node, ok := u.bufferIndexToListNode[bufferIndex]
	if !ok {
		return fmt.Errorf("mru removeUse: buffer index %d not here", bufferIndex)
	}
	// remove the node
	node.prev.next = node.next
	node.next.prev = node.prev
	delete(u.bufferIndexToListNode, bufferIndex)
	return nil
}

func (u *mru) updateUse(bufferIndex int) error {
	node, ok := u.bufferIndexToListNode[bufferIndex]
	if !ok {
		return fmt.Errorf("mru updateUse: buffer index %d not here", bufferIndex)
	}
	// remove the node
	node.prev.next = node.next
	node.next.prev = node.prev

	// add it to the beginning
	node.prev = u.mruListHead
	node.next = u.mruListHead.next
	u.mruListHead.next.prev = node
	u.mruListHead.next = node
	return nil
}

func (u *mru) removeLeastUsed() int{
	luBufferIdx := u.mruListTail.prev.bufferIndex
	u.removeUse(luBufferIdx)
	return luBufferIdx
}
