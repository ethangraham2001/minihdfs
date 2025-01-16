// Package datanode describes the structures needed for the DataNodes in HDFS.
package datanode

import (
	"fmt"

	"github.com/ethangraham2001/minihdfs/common"
)

// Represents a DataNode
type DataNode struct {
	store     store
	blockSize uint32
}

// NewVolatileDataNode returns a new DataNode that stores its data in memory
// instead of persisting it anywhere
func NewVolatileDataNode() DataNode {
	return DataNode{
		store: &inMemoryStore{},
	}
}

func (dataNode DataNode) WriteBlock(blockID common.BlockID, data []byte) error {
	if len(data) > int(common.BLOCK_SIZE) {
		return fmt.Errorf("block size cannot exceed BLOCK_SIZE = %d", common.BLOCK_SIZE)
	}

	return dataNode.store.Write(blockID, data)
}

func (dataNode DataNode) ReadBlock(blockID common.BlockID) ([]byte, error) {
	return dataNode.store.Read(blockID)
}
