package storage

import (
	"sync"

	"github.com/google/btree"
)

// Item represents a key-value pair stored in the B-Tree.
type Item struct {
	Key   string
	Value []byte
}

// Storage is a thread-safe in-memory B-Tree wrapper.
type Storage struct {
	mu   sync.RWMutex
	tree *btree.BTreeG[Item]
}

// NewStorage creates a new thread-safe storage engine.
func NewStorage() *Storage {
	return &Storage{
		tree: btree.NewG[Item](32, func(a, b Item) bool {
			return a.Key < b.Key
		}),
	}
}

// Put inserts or updates a key-value pair.
func (s *Storage) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tree.ReplaceOrInsert(Item{Key: key, Value: value})
}

// Get retrieves a value by its key.
func (s *Storage) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, found := s.tree.Get(Item{Key: key})
	if !found {
		return nil, false
	}
	return item.Value, true
}

// Delete removes a key-value pair from the storage.
func (s *Storage) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found := s.tree.Delete(Item{Key: key})
	return found
}
