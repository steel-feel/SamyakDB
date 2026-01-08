package sharding

import (
	"strconv"
	"testing"
)

func TestHashRing(t *testing.T) {
	hr := NewHashRing(3) // 3 replicas for simplicity in test
	
	nodes := []string{"node1", "node2", "node3"}
	for _, n := range nodes {
		hr.AddNode(n)
	}

	// Distribution check
	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		node := hr.GetNode("key" + strconv.Itoa(i))
		counts[node]++
	}

	for _, n := range nodes {
		if counts[n] == 0 {
			t.Errorf("Node %s got no keys", n)
		}
		t.Logf("Node %s: %d keys", n, counts[n])
	}

	// Deterministic check
	key := "test-key"
	nodeA := hr.GetNode(key)
	nodeB := hr.GetNode(key)
	if nodeA != nodeB {
		t.Errorf("GetNode is not deterministic: %s != %s", nodeA, nodeB)
	}

	// Removal check
	hr.RemoveNode("node1")
	for i := 0; i < 100; i++ {
		node := hr.GetNode("key" + strconv.Itoa(i))
		if node == "node1" {
			t.Errorf("Node1 still owns keys after removal")
		}
	}
}
