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
	"kvstore/internal/replication"
)

// Config contains validated membership, ownership, and the local node identity.
type Config struct {
	Local             cluster.Member
	Membership        *cluster.Membership
	Ring              *consistenthash.Ring
	ReplicationFactor int
	ReadQuorum        int
	WriteQuorum       int
}

type fileConfig struct {
	Members           []cluster.Member `json:"members"`
	VirtualNodes      int              `json:"virtual_nodes"`
	ReplicationFactor *int             `json:"replication_factor"`
	ReadQuorum        json.RawMessage  `json:"read_quorum"`
	WriteQuorum       json.RawMessage  `json:"write_quorum"`
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
	factor := document.ReplicationFactor
	if factor == nil || *factor < 1 || *factor > len(membership.NodeIDs()) {
		return nil, errors.New("replication_factor must be between one and the member count")
	}
	readQuorum, err := decodeQuorum(document.ReadQuorum, *factor)
	if err != nil {
		return nil, fmt.Errorf("read_quorum: %w", err)
	}
	writeQuorum, err := decodeQuorum(document.WriteQuorum, *factor)
	if err != nil {
		return nil, fmt.Errorf("write_quorum: %w", err)
	}
	if err := replication.ValidateQuorums(*factor, readQuorum, writeQuorum); err != nil {
		return nil, err
	}
	return &Config{Local: local, Membership: membership, Ring: ring, ReplicationFactor: *factor, ReadQuorum: readQuorum, WriteQuorum: writeQuorum}, nil
}

func decodeQuorum(encoded json.RawMessage, replicationFactor int) (int, error) {
	if len(encoded) == 0 {
		return replicationFactor/2 + 1, nil
	}
	var quorum int
	if err := json.Unmarshal(encoded, &quorum); err != nil {
		return 0, err
	}
	return quorum, nil
}

func decodeConfig(contents []byte) (fileConfig, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	defaultFactor := 3
	document := fileConfig{ReplicationFactor: &defaultFactor}
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
