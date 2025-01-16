// Package common describes some common functionality shared between DataNode
// and NameNode
package common

import "fmt"

type BlockID uint64

const BLOCK_SIZE uint32 = KB
const MB uint32 = 1048576
const KB uint32 = 1024

const NAMENODE_PORT int = 50000

const REPLICATION_FACTOR = 2

var NAMENODE_ADDR string = fmt.Sprintf("http://localhost:%d", NAMENODE_PORT)

type RegisterReq struct {
	Port int `json:"port"`
}

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
