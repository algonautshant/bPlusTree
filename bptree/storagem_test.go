package bptree

import (
	"testing"

	"github.com/stretchr/testify/require"
)



func TestPageOffsetFileOffset(t *testing.T) {
	
	po := FileOffsetPageIndex(HEADER_SIZE+84*PAGE_SIZE+666)

	require.Equal(t, 666, po.getElementIndexInPage())
	require.Equal(t, 84, int((po.getFileOffset()-HEADER_SIZE)/PAGE_SIZE))
}

