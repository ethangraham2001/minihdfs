package datanode

import (
	"errors"
)

type store interface {
	// Returns the data pointed to by BlockID, or returns an error if not contained
	Read(blockID BlockID) ([]byte, error)
	// Writes data into the block pointed to by BlockID
	Write(blockID BlockID, data []byte) error
}

type inMemoryStore struct {
	data map[BlockID][]byte
}

func (s *inMemoryStore) Read(blockId BlockID) ([]byte, error) {
	data, exists := s.data[blockId]
	if !exists {
		return []byte{}, errors.New("Data not contained in store")
	}
	return data, nil
}

func (s *inMemoryStore) Write(blockID BlockID, data []byte) error {
	if _, exists := s.data[blockID]; exists {
		return errors.New("Data already contained in block")
	}

	s.data[blockID] = data
	return nil
}
