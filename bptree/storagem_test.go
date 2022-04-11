package bptree

import (
	"testing"

	"github.com/stretchr/testify/require"
)



func TestPageOffsetFileOffset(t *testing.T) {
	headerSize := uint64(99)
	pageSize := uint64(1024)
	po := FileOffsetPageIndex(headerSize+84*pageSize+666)

	require.Equal(t, 666, po.getElementIndexInPage(headerSize, pageSize))
	require.Equal(t, 84, int((uint64(po.getFileOffset(headerSize, pageSize))-headerSize)/pageSize))
}

