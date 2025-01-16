// Package namenode describes the structures needed for the NameNodes in HDFS.
package namenode

import (
	"errors"
	"log"
	"strings"

	"github.com/ethangraham2001/minihdfs/common"
)

type NameNode struct {
	root fsElem
	// maps BlockID to the addresses of DataNodes holding the relevant block
	blockIDMap    map[common.BlockID][]string
	blockIDCtr    common.BlockID
	dataNodeAddrs []string
	roundRobin    int
}

func NewNameNode() NameNode {
	return NameNode{
		root: &directory{
			children: make(map[string]fsElem),
		},
		blockIDMap: make(map[common.BlockID][]string),
	}
}

func (nameNode *NameNode) getAndIncBlockCtr() common.BlockID {
	res := nameNode.blockIDCtr
	nameNode.blockIDCtr += 1
	return res
}

func (nameNode *NameNode) createFile(filepath string) error {
	split := strings.Split(filepath, "/")
	currElem := nameNode.root

	for i, s := range split {
		if i == len(split)-1 && !currElem.isFile() {
			dir, _ := currElem.(*directory)
			dir.children[s] = &file{}
			return nil
		}

		if currElem.isFile() {
			return errors.New("invalid filepath - file with same name as subdir")
		}

		dir := currElem.(*directory)
		nextElem, exists := dir.children[s]
		if !exists {
			nextElem = newDir()
			dir.children[s] = nextElem
		}
		currElem = nextElem
	}
	return nil
}

func (nameNode *NameNode) findFile(filepath string) (*file, error) {
	split := strings.Split(filepath, "/")
	currElem := nameNode.root
	for _, s := range split {
		if currElem.isFile() {
			return &file{}, errors.New("find file - invalid filepath")
		}

		dir := currElem.(*directory)
		nextElem, exists := dir.children[s]
		if !exists {
			return &file{}, errors.New("find file - invalid filepath")
		}
		currElem = nextElem
	}

	if !currElem.isFile() {
		return &file{}, errors.New("filepath points to a directory")
	}

	return currElem.(*file), nil
}

func (nameNode *NameNode) getFileBlockIDs(filepath string) ([]common.BlockID, error) {
	file, err := nameNode.findFile(filepath)
	if err != nil {
		return []common.BlockID{}, err
	}
	return file.blocks, nil
}

func (nameNode *NameNode) allocateNewBlockToFile(filepath string) (common.BlockID, error) {
	file, err := nameNode.findFile(filepath)
	if err != nil {
		return 0, err
	}
	newBlockID := nameNode.getAndIncBlockCtr()
	file.blocks = append(file.blocks, newBlockID)
	return newBlockID, nil
}

func (nameNode *NameNode) getFileBlocks(filepath string) ([]common.BlockID, error) {
	file, err := nameNode.findFile(filepath)
	if err != nil {
		return []common.BlockID{}, err
	}

	return file.blocks, nil
}

func (nameNode *NameNode) allocNDataNodes(n int, blockID common.BlockID) []string {
	dataNodes := make([]string, n)
	for i := range n {
		dataNodeAddr := nameNode.getNextDataNodeAddr()
		log.Printf("next datanode: %s", dataNodeAddr)
		dataNodes[i] = dataNodeAddr
	}
	nameNode.blockIDMap[blockID] = dataNodes
	return dataNodes
}

func (nameNode *NameNode) getNextDataNodeAddr() string {
	res := nameNode.dataNodeAddrs[nameNode.roundRobin]
	nameNode.roundRobin += 1
	nameNode.roundRobin %= len(nameNode.dataNodeAddrs)
	return res
}
