// Package common describes some common functionality shared between DataNode
// and NameNode
package common

import "fmt"

type BlockID uint64

const BLOCK_SIZE uint64 = 64 * MB
const MB uint64 = 1048576

const NAMENODE_PORT int = 50000

var NAMENODE_ADDR string = fmt.Sprintf("http://localhost:%d", NAMENODE_PORT)

type NameNodeReq struct {
	Filepath string `json:"filepath"`
}

type RequestBlockResp struct {
	NewBlockID uint64   `json:"new_block_id"`
	DataNodes  []string `json:"datanodes"`
}

type ReadFileResp struct {
	Blocks []BlockMapping `json:"blocks"`
}

type BlockMapping struct {
	BlockID       BlockID  `json:"block_id"`
	DataNodeAddrs []string `json:"datanodes"`
}
