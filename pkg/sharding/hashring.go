package sharding

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type HashRing struct {
	mu           sync.RWMutex
	replicas     int
	ring         []uint32
	nodes        map[uint32]string
	physicalNodes map[string]bool
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas:      replicas,
		nodes:         make(map[uint32]string),
		physicalNodes: make(map[string]bool),
	}
}

func (h *HashRing) AddNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.physicalNodes[node] {
		return
	}

	h.physicalNodes[node] = true
	for i := 0; i < h.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s-%d", node, i)))
		h.ring = append(h.ring, hash)
		h.nodes[hash] = node
	}
	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

func (h *HashRing) RemoveNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.physicalNodes[node] {
		return
	}

	delete(h.physicalNodes, node)
	newRing := make([]uint32, 0)
	for i := 0; i < h.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s-%d", node, i)))
		delete(h.nodes, hash)
	}

	for _, hash := range h.ring {
		if _, ok := h.nodes[hash]; ok {
			newRing = append(newRing, hash)
		}
	}
	h.ring = newRing
}

func (h *HashRing) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	if idx == len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx]]
}
