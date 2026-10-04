package cluster_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"kvstore/internal/cluster"
)

func TestNewMembershipRejectsInvalidMembers(t *testing.T) {
	cases := []struct {
		name    string
		members []cluster.Member
	}{
		{name: "no members"},
		{name: "empty ID", members: []cluster.Member{{Address: "http://localhost:8001"}}},
		{name: "blank ID", members: []cluster.Member{{ID: " \t", Address: "http://localhost:8001"}}},
		{name: "surrounding whitespace", members: []cluster.Member{{ID: " node-a", Address: "http://localhost:8001"}}},
		{
			name: "duplicate ID",
			members: []cluster.Member{
				{ID: "node-a", Address: "http://localhost:8001"},
				{ID: "node-a", Address: "http://localhost:8002"},
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			membership, err := cluster.NewMembership(testCase.members)
			if err == nil || membership != nil {
				t.Fatalf("NewMembership() = (%v, %v), want nil membership and an error", membership, err)
			}
		})
	}
}

func TestNewMembershipAcceptsHTTPAddresses(t *testing.T) {
	for _, address := range []string{
		"http://127.0.0.1:8001",
		"http://localhost:8001/",
		"http://node-a:8001",
		"http://node-a.example",
		"https://node-a.example:443",
		"https://node-a.example/",
		"http://[::1]:8001",
		"http://[2001:db8::1]",
		"http://localhost:1",
		"http://localhost:65535",
	} {
		t.Run(address, func(t *testing.T) {
			expected := cluster.Member{ID: "node-a", Address: address}
			membership := newMembership(t, []cluster.Member{expected})
			member, found := membership.Lookup(expected.ID)
			if !found || member != expected {
				t.Errorf("Lookup() = (%v, %t), want (%v, true)", member, found, expected)
			}
		})
	}
}

func TestNewMembershipRejectsInvalidAddresses(t *testing.T) {
	cases := []struct {
		name    string
		address string
	}{
		{name: "empty", address: ""},
		{name: "missing scheme", address: "localhost:8001"},
		{name: "relative URL", address: "/node-a"},
		{name: "unsupported scheme", address: "ftp://localhost:8001"},
		{name: "missing host", address: "http://:8001"},
		{name: "opaque URL", address: "http:localhost:8001"},
		{name: "credentials", address: "http://user:password@localhost:8001"},
		{name: "application path", address: "http://localhost:8001/kv"},
		{name: "encoded path", address: "http://localhost:8001/%2F"},
		{name: "query", address: "http://localhost:8001?node=a"},
		{name: "empty query", address: "http://localhost:8001?"},
		{name: "fragment", address: "http://localhost:8001#node"},
		{name: "empty fragment", address: "http://localhost:8001#"},
		{name: "zero port", address: "http://localhost:0"},
		{name: "large port", address: "http://localhost:65536"},
		{name: "negative port", address: "http://localhost:-1"},
		{name: "non-numeric port", address: "http://localhost:abc"},
		{name: "empty port", address: "http://localhost:"},
		{name: "invalid IPv6", address: "http://[not-an-ip]:8001"},
		{name: "unbracketed IPv6", address: "http://::1:8001"},
		{name: "malformed URL", address: "http://[::1"},
		{name: "host whitespace", address: "http://bad host:8001"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			membership, err := cluster.NewMembership([]cluster.Member{{ID: "node-a", Address: testCase.address}})
			if err == nil || membership != nil {
				t.Fatalf("NewMembership() = (%v, %v), want nil membership and an error", membership, err)
			}
			if !strings.Contains(err.Error(), "node-a") {
				t.Errorf("address error does not identify the node: %v", err)
			}
		})
	}
}

func TestMembershipLookup(t *testing.T) {
	members := []cluster.Member{
		{ID: "node-a", Address: "http://localhost:8001"},
		{ID: "node-b", Address: "http://localhost:8002"},
	}
	membership := newMembership(t, members)
	for _, expected := range members {
		member, found := membership.Lookup(expected.ID)
		if !found || member != expected {
			t.Errorf("Lookup(%q) = (%v, %t), want (%v, true)", expected.ID, member, found, expected)
		}
	}
	for _, nodeID := range []string{"missing", ""} {
		member, found := membership.Lookup(nodeID)
		if found || member != (cluster.Member{}) {
			t.Errorf("Lookup(%q) = (%v, %t), want an empty member and false", nodeID, member, found)
		}
	}
}

func TestMembershipNodeIDsAreSorted(t *testing.T) {
	members := []cluster.Member{
		{ID: "node-c", Address: "http://localhost:8003"},
		{ID: "node-a", Address: "http://localhost:8001"},
		{ID: "node-b", Address: "http://localhost:8002"},
	}
	expected := []string{"node-a", "node-b", "node-c"}
	for range len(members) {
		membership := newMembership(t, members)
		if ids := membership.NodeIDs(); !slices.Equal(ids, expected) {
			t.Errorf("NodeIDs() = %v, want %v", ids, expected)
		}
		members = append(members[1:], members[0])
	}
}

func TestMembershipDoesNotShareMutableState(t *testing.T) {
	expected := cluster.Member{ID: "node-a", Address: "http://localhost:8001"}
	members := []cluster.Member{expected}
	membership := newMembership(t, members)
	if members[0] != expected {
		t.Fatalf("constructor changed its input: %v", members)
	}
	members[0] = cluster.Member{ID: "replacement", Address: "http://localhost:9001"}
	member, _ := membership.Lookup("node-a")
	member.Address = "http://localhost:9002"
	ids := membership.NodeIDs()
	ids[0] = "replacement"

	actual, found := membership.Lookup("node-a")
	if !found || actual != expected {
		t.Errorf("caller mutation changed membership: got (%v, %t), want (%v, true)", actual, found, expected)
	}
	if ids := membership.NodeIDs(); !slices.Equal(ids, []string{"node-a"}) {
		t.Errorf("caller mutation changed IDs: %v", ids)
	}
}

func TestMembershipZeroValue(t *testing.T) {
	var membership cluster.Membership
	if member, found := membership.Lookup("node-a"); found || member != (cluster.Member{}) {
		t.Errorf("Lookup() = (%v, %t), want an empty member and false", member, found)
	}
	if ids := membership.NodeIDs(); len(ids) != 0 {
		t.Errorf("NodeIDs() = %v, want no IDs", ids)
	}
}

func TestMembershipConcurrentReads(t *testing.T) {
	expected := cluster.Member{ID: "node-a", Address: "http://localhost:8001"}
	membership := newMembership(t, []cluster.Member{expected})
	for worker := 0; worker < 16; worker++ {
		t.Run(fmt.Sprintf("reader_%d", worker), func(t *testing.T) {
			t.Parallel()
			for range 100 {
				member, found := membership.Lookup("node-a")
				if !found || member != expected {
					t.Fatalf("Lookup() = (%v, %t), want (%v, true)", member, found, expected)
				}
				ids := membership.NodeIDs()
				if !slices.Equal(ids, []string{"node-a"}) {
					t.Fatalf("NodeIDs() = %v, want [node-a]", ids)
				}
				ids[0] = "caller-owned"
			}
		})
	}
}

func newMembership(t *testing.T, members []cluster.Member) *cluster.Membership {
	t.Helper()
	membership, err := cluster.NewMembership(members)
	if err != nil {
		t.Fatalf("create membership: %v", err)
	}
	return membership
}
