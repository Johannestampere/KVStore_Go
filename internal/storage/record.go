package storage

import (
	"cmp"
	"errors"
	"strings"
)

// Version orders observed updates by counter, then by originating node ID.
type Version struct {
	Counter uint64 `json:"counter"`
	NodeID  string `json:"node_id"`
}

// Compare returns -1, 0, or 1 when version precedes, equals, or follows other.
func (version Version) Compare(other Version) int {
	if order := cmp.Compare(version.Counter, other.Counter); order != 0 {
		return order
	}
	return strings.Compare(version.NodeID, other.NodeID)
}

// Record holds a value or a versioned deletion marker.
type Record struct {
	Key     string  `json:"key"`
	Value   string  `json:"value"`
	Version Version `json:"version"`
	Deleted bool    `json:"deleted"`
}

// Validate rejects invalid versions and deletion markers containing values.
func (record Record) Validate() error {
	if record.Version.Counter == 0 {
		return errors.New("record version counter must be positive")
	}
	if err := validateNodeID(record.Version.NodeID); err != nil {
		return err
	}
	if record.Deleted && record.Value != "" {
		return errors.New("deleted record must have an empty value")
	}
	return nil
}

func validateNodeID(nodeID string) error {
	if nodeID == "" || strings.TrimSpace(nodeID) != nodeID {
		return errors.New("node ID must be non-empty and have no surrounding whitespace")
	}
	return nil
}
