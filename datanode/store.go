package datanode

import (
	"errors"

	"github.com/ethangraham2001/minihdfs/common"
)

type store interface {
	// Returns the data pointed to by common.BlockID, or returns an error if not contained
	Read(blockID common.BlockID) ([]byte, error) // Writes data into the block pointed to by BlockID
	Write(blockID common.BlockID, data []byte) error
	ContainsBlock(blockID common.BlockID) bool
}

type inMemoryStore struct {
	data map[common.BlockID][]byte
}

func NewInMemoryStore() store {
	return &inMemoryStore{
		data: make(map[common.BlockID][]byte),
	}
}

func (s *inMemoryStore) Read(blockId common.BlockID) ([]byte, error) {
	data, exists := s.data[blockId]
	if !exists {
		return []byte{}, errors.New("Data not contained in store")
	}
	return data, nil
}

func (s *inMemoryStore) Write(blockID common.BlockID, data []byte) error {
	if _, exists := s.data[blockID]; exists {
		return errors.New("Data already contained in block")
	}

	s.data[blockID] = data
	return nil
}

func (s *inMemoryStore) ContainsBlock(blockID common.BlockID) bool {
	_, exists := s.data[blockID]
	return exists
}
