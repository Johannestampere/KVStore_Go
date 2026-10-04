package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"kvstore/internal/cluster"
	"kvstore/internal/config"
)

func TestLoadSelectsLocalMember(t *testing.T) {
	path := writeConfig(t, `{
		"virtual_nodes": 8,
		"members": [
			{"id":"node-c","address":"http://localhost:8003"},
			{"id":"node-a","address":"http://localhost:8001"},
			{"id":"node-b","address":"http://localhost:8002"}
		]
	}`)
	expectedIDs := []string{"node-a", "node-b", "node-c"}
	for _, expected := range []cluster.Member{
		{ID: "node-a", Address: "http://localhost:8001"},
		{ID: "node-b", Address: "http://localhost:8002"},
	} {
		loaded, err := config.Load(path, expected.ID)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if loaded.Local != expected {
			t.Errorf("local member = %v, want %v", loaded.Local, expected)
		}
		if ids := loaded.Membership.NodeIDs(); !slices.Equal(ids, expectedIDs) {
			t.Errorf("membership IDs = %v, want %v", ids, expectedIDs)
		}
		owners, err := loaded.Ring.GetNodes("user:42", 3)
		if err != nil {
			t.Fatalf("select owners: %v", err)
		}
		if expected := []string{"node-c", "node-b", "node-a"}; !slices.Equal(owners, expected) {
			t.Errorf("owners = %v, want %v", owners, expected)
		}
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	const member = `{"id":"node-a","address":"http://localhost:8001"}`
	const valid = `{"virtual_nodes":8,"members":[` + member + `]}`
	cases := []struct {
		name     string
		contents string
	}{
		{name: "empty file", contents: ""},
		{name: "malformed JSON", contents: `{"members":`},
		{name: "null", contents: `null`},
		{name: "array", contents: `[]`},
		{name: "unknown field", contents: `{"virtual_nodes":8,"members":[` + member + `],"typo":true}`},
		{name: "unknown member field", contents: `{"virtual_nodes":8,"members":[{"id":"node-a","address":"http://localhost:8001","typo":true}]}`},
		{name: "missing members", contents: `{"virtual_nodes":8}`},
		{name: "empty members", contents: `{"virtual_nodes":8,"members":[]}`},
		{name: "null member", contents: `{"virtual_nodes":8,"members":[null]}`},
		{name: "duplicate members", contents: `{"virtual_nodes":8,"members":[` + member + `,` + member + `]}`},
		{name: "invalid address", contents: `{"virtual_nodes":8,"members":[{"id":"node-a","address":"localhost:8001"}]}`},
		{name: "missing virtual nodes", contents: `{"members":[` + member + `]}`},
		{name: "zero virtual nodes", contents: `{"virtual_nodes":0,"members":[` + member + `]}`},
		{name: "negative virtual nodes", contents: `{"virtual_nodes":-1,"members":[` + member + `]}`},
		{name: "null virtual nodes", contents: `{"virtual_nodes":null,"members":[` + member + `]}`},
		{name: "fractional virtual nodes", contents: `{"virtual_nodes":1.5,"members":[` + member + `]}`},
		{name: "multiple objects", contents: valid + `{}`},
		{name: "trailing null", contents: valid + `null`},
		{name: "trailing garbage", contents: valid + `garbage`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			loaded, err := config.Load(writeConfig(t, testCase.contents), "node-a")
			if err == nil || loaded != nil {
				t.Fatalf("Load() = (%v, %v), want nil config and an error", loaded, err)
			}
		})
	}
}

func TestLoadRejectsUnknownLocalNode(t *testing.T) {
	path := writeConfig(t, `{"virtual_nodes":8,"members":[{"id":"node-a","address":"http://localhost:8001"}]}`)
	for _, nodeID := range []string{"", "missing", " node-a"} {
		loaded, err := config.Load(path, nodeID)
		if err == nil || loaded != nil {
			t.Fatalf("Load(%q) = (%v, %v), want nil config and an error", nodeID, loaded, err)
		}
		if !strings.Contains(err.Error(), "local node") {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestLoadReportsMissingFile(t *testing.T) {
	loaded, err := config.Load(filepath.Join(t.TempDir(), "missing.json"), "node-a")
	if loaded != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load() = (%v, %v), want nil config and a missing-file error", loaded, err)
	}
}

func TestLoadAcceptsTrailingWhitespace(t *testing.T) {
	path := writeConfig(t, "{\"virtual_nodes\":8,\"members\":[{\"id\":\"node-a\",\"address\":\"http://localhost:8001\"}]}\n\t ")
	if _, err := config.Load(path, "node-a"); err != nil {
		t.Fatalf("Load(): %v", err)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
