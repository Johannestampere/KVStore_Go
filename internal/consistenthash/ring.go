// Package consistenthash maps keys to node IDs on an immutable hash ring.
package consistenthash

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

// Ring selects distinct owners and supports concurrent lookups.
type Ring struct {
	points    []ringPoint
	nodeCount int
}

type ringPoint struct {
	position uint64
	nodeID   string
}

// NewRing builds a ring from unique, non-empty IDs and a positive virtual-node count.
func NewRing(nodeIDs []string, virtualNodes int) (*Ring, error) {
	if err := validateNodeIDs(nodeIDs); err != nil {
		return nil, err
	}
	if virtualNodes < 1 {
		return nil, errors.New("virtual-node count must be positive")
	}
	maxInt := int(^uint(0) >> 1)
	if virtualNodes > maxInt/len(nodeIDs) {
		return nil, errors.New("total ring-point count exceeds integer capacity")
	}

	points := make([]ringPoint, 0, len(nodeIDs)*virtualNodes)
	for _, nodeID := range nodeIDs {
		for index := 0; index < virtualNodes; index++ {
			identity := nodeID + "\x00" + strconv.Itoa(index)
			points = append(points, ringPoint{
				position: hashPosition(identity),
				nodeID:   nodeID,
			})
		}
	}
	sort.Slice(points, func(left, right int) bool {
		if points[left].position == points[right].position {
			// Hash collisions must resolve identically on every node.
			return points[left].nodeID < points[right].nodeID
		}
		return points[left].position < points[right].position
	})
	return &Ring{points: points, nodeCount: len(nodeIDs)}, nil
}

// GetNodes returns count distinct owners in clockwise order.
// Empty keys are valid; count must be between one and the physical node count.
func (ring *Ring) GetNodes(key string, count int) ([]string, error) {
	if count < 1 {
		return nil, errors.New("replica count must be positive")
	}
	if count > ring.nodeCount {
		return nil, fmt.Errorf("requested %d replicas, ring has %d nodes", count, ring.nodeCount)
	}

	position := hashPosition(key)
	start := sort.Search(len(ring.points), func(index int) bool {
		return ring.points[index].position >= position
	})
	nodes := make([]string, 0, count)
	selected := make(map[string]struct{}, count)
	for offset := 0; offset < len(ring.points) && len(nodes) < count; offset++ {
		nodeID := ring.points[(start+offset)%len(ring.points)].nodeID
		if _, exists := selected[nodeID]; exists {
			continue
		}
		selected[nodeID] = struct{}{}
		nodes = append(nodes, nodeID)
	}
	return nodes, nil
}

func validateNodeIDs(nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return errors.New("hash ring requires at least one node")
	}
	seen := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID == "" {
			return errors.New("node ID must not be empty")
		}
		if _, exists := seen[nodeID]; exists {
			return fmt.Errorf("duplicate node ID %q", nodeID)
		}
		seen[nodeID] = struct{}{}
	}
	return nil
}

func hashPosition(value string) uint64 {
	digest := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint64(digest[:8])
}
