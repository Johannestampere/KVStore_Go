package consistenthash_test

import (
	"fmt"
	"slices"
	"testing"

	"kvstore/internal/consistenthash"
)

func TestNewRingRejectsInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name         string
		nodeIDs      []string
		virtualNodes int
	}{
		{name: "no nodes", virtualNodes: 1},
		{name: "empty node ID", nodeIDs: []string{"node-a", ""}, virtualNodes: 1},
		{name: "duplicate node ID", nodeIDs: []string{"node-a", "node-a"}, virtualNodes: 1},
		{name: "zero virtual nodes", nodeIDs: []string{"node-a"}, virtualNodes: 0},
		{name: "negative virtual nodes", nodeIDs: []string{"node-a"}, virtualNodes: -1},
		{name: "point count overflow", nodeIDs: []string{"node-a", "node-b"}, virtualNodes: int(^uint(0) >> 1)},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ring, err := consistenthash.NewRing(testCase.nodeIDs, testCase.virtualNodes)
			if err == nil || ring != nil {
				t.Fatalf("NewRing() = (%v, %v), want nil ring and an error", ring, err)
			}
		})
	}
}

func TestGetNodesClockwiseOrder(t *testing.T) {
	ring := newRing(t, []string{"node-a", "node-b", "node-c"}, 1)
	// Fixed SHA-256 fixtures catch routing changes independently of the implementation.
	cases := []struct {
		name     string
		key      string
		expected []string
	}{
		{name: "between points", key: "key:0", expected: []string{"node-a", "node-c", "node-b"}},
		{name: "last point then wrap", key: "key:12", expected: []string{"node-c", "node-b", "node-a"}},
		{name: "beyond last point", key: "wrap:2", expected: []string{"node-b", "node-a", "node-c"}},
		{name: "exact first point", key: "node-b\x000", expected: []string{"node-b", "node-a", "node-c"}},
		{name: "exact middle point", key: "node-a\x000", expected: []string{"node-a", "node-c", "node-b"}},
		{name: "exact last point", key: "node-c\x000", expected: []string{"node-c", "node-b", "node-a"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			nodes := getNodes(t, ring, testCase.key, 3)
			if !slices.Equal(nodes, testCase.expected) {
				t.Errorf("owners = %v, want %v", nodes, testCase.expected)
			}
		})
	}
}

func TestGetNodesWithVirtualNodes(t *testing.T) {
	ring := newRing(t, []string{"node-a", "node-b", "node-c"}, 8)
	cases := []struct {
		key      string
		expected []string
	}{
		{key: "", expected: []string{"node-b", "node-c", "node-a"}},
		{key: "user:1", expected: []string{"node-c", "node-a", "node-b"}},
		{key: "user:42", expected: []string{"node-c", "node-b", "node-a"}},
		{key: "名前", expected: []string{"node-c", "node-a", "node-b"}},
	}
	for _, testCase := range cases {
		for count := 1; count <= 3; count++ {
			t.Run(fmt.Sprintf("key=%q/count=%d", testCase.key, count), func(t *testing.T) {
				nodes := getNodes(t, ring, testCase.key, count)
				if !slices.Equal(nodes, testCase.expected[:count]) {
					t.Errorf("owners = %v, want %v", nodes, testCase.expected[:count])
				}
			})
		}
	}
}

func TestGetNodesSingleNode(t *testing.T) {
	ring := newRing(t, []string{"only-node"}, 32)
	for _, key := range []string{"", "user:1", "名前"} {
		nodes := getNodes(t, ring, key, 1)
		if !slices.Equal(nodes, []string{"only-node"}) {
			t.Errorf("owners for %q = %v, want [only-node]", key, nodes)
		}
	}
}

func TestGetNodesRejectsInvalidReplicaCount(t *testing.T) {
	ring := newRing(t, []string{"node-a", "node-b", "node-c"}, 8)
	for _, count := range []int{-1, 0, 4} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			nodes, err := ring.GetNodes("user:1", count)
			if err == nil || nodes != nil {
				t.Fatalf("GetNodes() = (%v, %v), want nil owners and an error", nodes, err)
			}
		})
	}
}

func TestGetNodesRejectsEmptyRing(t *testing.T) {
	var ring consistenthash.Ring
	nodes, err := ring.GetNodes("user:1", 1)
	if err == nil || nodes != nil {
		t.Fatalf("GetNodes() = (%v, %v), want nil owners and an error", nodes, err)
	}
}

func TestRingIgnoresNodeInputOrder(t *testing.T) {
	orders := [][]string{
		{"node-a", "node-b", "node-c"},
		{"node-a", "node-c", "node-b"},
		{"node-b", "node-a", "node-c"},
		{"node-b", "node-c", "node-a"},
		{"node-c", "node-a", "node-b"},
		{"node-c", "node-b", "node-a"},
	}
	reference := newRing(t, orders[0], 32)
	for _, order := range orders {
		ring := newRing(t, order, 32)
		for index := 0; index < 100; index++ {
			key := fmt.Sprintf("key:%d", index)
			expected := getNodes(t, reference, key, 3)
			nodes := getNodes(t, ring, key, 3)
			if !slices.Equal(nodes, expected) {
				t.Fatalf("order %v, key %q: owners = %v, want %v", order, key, nodes, expected)
			}
		}
	}
}

func TestRingDoesNotShareMutableSlices(t *testing.T) {
	nodeIDs := []string{"node-c", "node-a", "node-b"}
	originalIDs := slices.Clone(nodeIDs)
	ring := newRing(t, nodeIDs, 8)
	if !slices.Equal(nodeIDs, originalIDs) {
		t.Fatalf("constructor changed input IDs: got %v, want %v", nodeIDs, originalIDs)
	}
	expected := getNodes(t, ring, "user:1", 3)
	nodeIDs[0] = "replaced"
	owners := getNodes(t, ring, "user:1", 3)
	owners[0] = "replaced"

	if actual := getNodes(t, ring, "user:1", 3); !slices.Equal(actual, expected) {
		t.Errorf("caller mutation changed owners: got %v, want %v", actual, expected)
	}
}

func TestAddingNodePreservesExistingOwnership(t *testing.T) {
	for _, virtualNodes := range []int{1, 32} {
		t.Run(fmt.Sprintf("virtualNodes=%d", virtualNodes), func(t *testing.T) {
			before := newRing(t, []string{"node-a", "node-b", "node-c"}, virtualNodes)
			after := newRing(t, []string{"node-a", "node-b", "node-c", "node-d"}, virtualNodes)
			moved := 0
			const keyCount = 1000
			for index := 0; index < keyCount; index++ {
				key := fmt.Sprintf("key:%d", index)
				previousOwners := getNodes(t, before, key, 3)
				newOwners := getNodes(t, after, key, 4)
				if previousOwners[0] != newOwners[0] {
					moved++
					if newOwners[0] != "node-d" {
						t.Fatalf("key %q moved from %q to existing node %q", key, previousOwners[0], newOwners[0])
					}
				}
				remaining := slices.DeleteFunc(newOwners, func(nodeID string) bool { return nodeID == "node-d" })
				if !slices.Equal(remaining, previousOwners) {
					t.Fatalf("key %q: existing owner order changed from %v to %v", key, previousOwners, remaining)
				}
			}
			if moved == 0 || moved == keyCount {
				t.Errorf("moved %d of %d keys, want both changed and unchanged owners", moved, keyCount)
			}
		})
	}
}

func TestRemovingNodePreservesRemainingOwnership(t *testing.T) {
	for _, virtualNodes := range []int{1, 32} {
		t.Run(fmt.Sprintf("virtualNodes=%d", virtualNodes), func(t *testing.T) {
			before := newRing(t, []string{"node-a", "node-b", "node-c"}, virtualNodes)
			after := newRing(t, []string{"node-a", "node-c"}, virtualNodes)
			moved := 0
			const keyCount = 1000
			for index := 0; index < keyCount; index++ {
				key := fmt.Sprintf("key:%d", index)
				previousOwners := getNodes(t, before, key, 3)
				newOwners := getNodes(t, after, key, 2)
				if previousOwners[0] != newOwners[0] {
					moved++
					if previousOwners[0] != "node-b" {
						t.Fatalf("key %q moved away from remaining node %q", key, previousOwners[0])
					}
				}
				expected := slices.DeleteFunc(previousOwners, func(nodeID string) bool { return nodeID == "node-b" })
				if !slices.Equal(newOwners, expected) {
					t.Fatalf("key %q: owners = %v, want %v", key, newOwners, expected)
				}
			}
			if moved == 0 || moved == keyCount {
				t.Errorf("moved %d of %d keys, want both changed and unchanged owners", moved, keyCount)
			}
		})
	}
}

func TestConcurrentLookups(t *testing.T) {
	ring := newRing(t, []string{"node-a", "node-b", "node-c"}, 32)
	for worker := 0; worker < 16; worker++ {
		key := fmt.Sprintf("key:%d", worker)
		expected := getNodes(t, ring, key, 3)
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			for iteration := 0; iteration < 100; iteration++ {
				nodes := getNodes(t, ring, key, 3)
				if !slices.Equal(nodes, expected) {
					t.Fatalf("owners = %v, want %v", nodes, expected)
				}
				nodes[0] = "caller-owned"
			}
		})
	}
}

func ExampleRing_GetNodes() {
	ring, err := consistenthash.NewRing([]string{"node-a", "node-b", "node-c"}, 8)
	if err != nil {
		fmt.Println(err)
		return
	}
	nodes, err := ring.GetNodes("user:42", 2)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(nodes)
	// Output: [node-c node-b]
}

func newRing(t *testing.T, nodeIDs []string, virtualNodes int) *consistenthash.Ring {
	t.Helper()
	ring, err := consistenthash.NewRing(nodeIDs, virtualNodes)
	if err != nil {
		t.Fatalf("create ring: %v", err)
	}
	return ring
}

func getNodes(t *testing.T, ring *consistenthash.Ring, key string, count int) []string {
	t.Helper()
	nodes, err := ring.GetNodes(key, count)
	if err != nil {
		t.Fatalf("get owners for %q: %v", key, err)
	}
	return nodes
}
