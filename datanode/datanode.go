// Package datanode describes the structures needed for the DataNodes in HDFS.
package datanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ethangraham2001/minihdfs/common"
)

// Represents a DataNode
type DataNode struct {
	store     store
	blockSize uint32
	port      int
}

// NewVolatileDataNode returns a new DataNode that stores its data in memory
// instead of persisting it anywhere
func NewVolatileDataNode(port int) DataNode {
	return DataNode{
		store:     NewInMemoryStore(),
		blockSize: uint32(common.BLOCK_SIZE),
		port:      port,
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

func (dataNode DataNode) registerWithNameNode() {
	request := common.RegisterReq{Port: dataNode.port}
	buff, _ := json.Marshal(request)
	req, err := http.NewRequest("POST", common.NAMENODE_ADDR+"/register", bytes.NewBuffer(buff))
	if err != nil {
		panic("failed to create register request")
	}

	client := &http.Client{}
	_, err = client.Do(req)
	if err != nil {
		panic("failed to register with NameNode")
	}
}
