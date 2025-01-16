package namenode

import "github.com/ethangraham2001/minihdfs/common"

type fsElem interface {
	isFile() bool
}

type directory struct {
	children map[string]fsElem
}

func (dir *directory) isFile() bool {
	return false
}

func newDir() *directory {
	return &directory{
		children: make(map[string]fsElem),
	}
}

type file struct {
	blocks []common.BlockID
}

func (f *file) isFile() bool {
	return true
}
