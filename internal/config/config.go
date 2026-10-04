// Package config loads the cluster topology used at node startup.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"kvstore/internal/cluster"
	"kvstore/internal/consistenthash"
)

// Config contains validated membership, ownership, and the local node identity.
type Config struct {
	Local      cluster.Member
	Membership *cluster.Membership
	Ring       *consistenthash.Ring
}

type fileConfig struct {
	Members      []cluster.Member `json:"members"`
	VirtualNodes int              `json:"virtual_nodes"`
}

// Load reads a shared JSON configuration and selects the local node by ID.
func Load(path, nodeID string) (*Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster config: %w", err)
	}
	document, err := decodeConfig(contents)
	if err != nil {
		return nil, err
	}
	membership, err := cluster.NewMembership(document.Members)
	if err != nil {
		return nil, fmt.Errorf("cluster membership: %w", err)
	}
	local, found := membership.Lookup(nodeID)
	if !found {
		return nil, fmt.Errorf("local node %q is not a configured member", nodeID)
	}
	ring, err := consistenthash.NewRing(membership.NodeIDs(), document.VirtualNodes)
	if err != nil {
		return nil, fmt.Errorf("cluster hash ring: %w", err)
	}
	return &Config{Local: local, Membership: membership, Ring: ring}, nil
}

func decodeConfig(contents []byte) (fileConfig, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var document fileConfig
	if err := decoder.Decode(&document); err != nil {
		return fileConfig{}, fmt.Errorf("decode cluster config: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return fileConfig{}, fmt.Errorf("read end of cluster config: %w", err)
		}
		return fileConfig{}, errors.New("cluster config must contain one JSON object")
	}
	return document, nil
}
