package storage_test

import (
	"math"
	"testing"

	"kvstore/internal/storage"
)

func TestVersionCompare(t *testing.T) {
	cases := []struct {
		name        string
		left, right storage.Version
		order       int
	}{
		{"same version", storage.Version{Counter: 3, NodeID: "a"}, storage.Version{Counter: 3, NodeID: "a"}, 0},
		{"counter before node ID", storage.Version{Counter: 2, NodeID: "z"}, storage.Version{Counter: 3, NodeID: "a"}, -1},
		{"node ID breaks tie", storage.Version{Counter: 3, NodeID: "a"}, storage.Version{Counter: 3, NodeID: "b"}, -1},
		{"maximum counter", storage.Version{Counter: math.MaxUint64, NodeID: "a"}, storage.Version{Counter: 1, NodeID: "z"}, 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if order := testCase.left.Compare(testCase.right); order != testCase.order {
				t.Errorf("Compare = %d, want %d", order, testCase.order)
			}
			if reverse := testCase.right.Compare(testCase.left); reverse != -testCase.order {
				t.Errorf("reverse Compare = %d, want %d", reverse, -testCase.order)
			}
		})
	}
}

func TestRecordValidation(t *testing.T) {
	cases := []struct {
		name   string
		record storage.Record
		valid  bool
	}{
		{"empty key and value", storage.Record{Version: storage.Version{Counter: 1, NodeID: "a"}}, true},
		{"tombstone", storage.Record{Key: "key", Version: storage.Version{Counter: 1, NodeID: "a"}, Deleted: true}, true},
		{"zero counter", storage.Record{Version: storage.Version{NodeID: "a"}}, false},
		{"missing node ID", storage.Record{Version: storage.Version{Counter: 1}}, false},
		{"node ID whitespace", storage.Record{Version: storage.Version{Counter: 1, NodeID: " a"}}, false},
		{"tombstone with value", storage.Record{Value: "old", Version: storage.Version{Counter: 1, NodeID: "a"}, Deleted: true}, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.record.Validate(); (err == nil) != testCase.valid {
				t.Fatalf("Validate = %v, valid = %v", err, testCase.valid)
			}
		})
	}
}

func TestNewMemoryStoreRequiresNodeIdentity(t *testing.T) {
	for _, nodeID := range []string{"", " ", " node-a", "node-a\n"} {
		if _, err := storage.NewMemoryStore(nodeID); err == nil {
			t.Errorf("accepted node ID %q", nodeID)
		}
	}
}
