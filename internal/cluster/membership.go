// Package cluster describes static cluster membership.
package cluster

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// Member identifies a node and its advertised HTTP base URL.
type Member struct {
	ID      string
	Address string
}

// Membership is an immutable node directory safe for concurrent reads.
type Membership struct {
	members map[string]Member
}

// NewMembership validates and copies a non-empty set of uniquely identified nodes.
func NewMembership(members []Member) (*Membership, error) {
	if len(members) == 0 {
		return nil, errors.New("membership requires at least one node")
	}
	directory := make(map[string]Member, len(members))
	for _, member := range members {
		if member.ID == "" || strings.TrimSpace(member.ID) != member.ID {
			return nil, errors.New("node ID must be non-empty and have no surrounding whitespace")
		}
		if _, exists := directory[member.ID]; exists {
			return nil, fmt.Errorf("duplicate node ID %q", member.ID)
		}
		if err := validateAddress(member.Address); err != nil {
			return nil, fmt.Errorf("node %q address: %w", member.ID, err)
		}
		directory[member.ID] = member
	}
	return &Membership{members: directory}, nil
}

// Lookup returns the member and whether its ID is configured.
func (membership *Membership) Lookup(nodeID string) (Member, bool) {
	member, found := membership.members[nodeID]
	return member, found
}

// NodeIDs returns a sorted copy of the configured IDs.
func (membership *Membership) NodeIDs() []string {
	ids := make([]string, 0, len(membership.members))
	for nodeID := range membership.members {
		ids = append(ids, nodeID)
	}
	slices.Sort(ids)
	return ids
}

func validateAddress(address string) error {
	endpoint, err := url.Parse(address)
	if err != nil {
		return errors.New("invalid URL syntax")
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return errors.New("scheme must be http or https")
	}
	if endpoint.Hostname() == "" {
		return errors.New("host must not be empty")
	}
	if endpoint.User != nil {
		return errors.New("credentials are not allowed")
	}
	if endpoint.EscapedPath() != "" && endpoint.EscapedPath() != "/" {
		return errors.New("address must not contain an application path")
	}
	if endpoint.ForceQuery || endpoint.RawQuery != "" || strings.Contains(address, "#") {
		return errors.New("query strings and fragments are not allowed")
	}
	if strings.HasPrefix(endpoint.Host, "[") {
		ip, err := netip.ParseAddr(endpoint.Hostname())
		if err != nil || !ip.Is6() {
			return errors.New("bracketed host must be a valid IPv6 address")
		}
	} else if strings.Contains(endpoint.Hostname(), ":") {
		return errors.New("IPv6 addresses must be bracketed")
	}
	if strings.HasSuffix(endpoint.Host, ":") {
		return errors.New("explicit port must not be empty")
	}
	if port := endpoint.Port(); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return errors.New("port must be between 1 and 65535")
		}
	}
	return nil
}
