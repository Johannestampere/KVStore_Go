package replication_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/cluster"
	"kvstore/internal/consistenthash"
	"kvstore/internal/replication"
	"kvstore/internal/storage"
	"kvstore/internal/transport"
)

type testNode struct {
	id     string
	server *httptest.Server
	store  *storage.MemoryStore
}

type testCluster struct {
	nodes []*testNode
	ring  *consistenthash.Ring
}

func TestReplicatedKeyLifecycles(t *testing.T) {
	cluster := newCluster(t, 5*time.Second, nil)
	keys := []string{"user/42 name", "%2F", ".", "..", "日本語?#", strings.Repeat("k", 4<<10)}
	for index := range 12 {
		keys = append(keys, fmt.Sprintf("user:%d", index))
	}
	for _, key := range keys {
		name := key
		if len(name) > 40 {
			name = "maximum key size"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			owners := cluster.replicas(t, key)
			entry := cluster.outside(owners)
			target := "/kv/" + escapeKey(key)
			var previous storage.Record
			for _, value := range []string{"Alice", ""} {
				payload, err := json.Marshal(map[string]string{"value": value})
				if err != nil {
					t.Fatal(err)
				}
				request(t, entry, http.MethodPut, target, string(payload), http.StatusNoContent)
				var expected storage.Record
				for _, node := range cluster.nodes {
					record, found := node.store.GetRecord(key)
					if found != slices.Contains(owners, node) {
						t.Fatalf("unexpected placement on %s", node.id)
					}
					if found {
						if expected.Version.Counter == 0 {
							expected = record
						}
						if record != expected || record.Version.NodeID != entry.id || record.Value != value || record.Deleted || record.Version.Compare(previous.Version) <= 0 {
							t.Fatalf("unexpected replica on %s: %+v", node.id, record)
						}
					}
					body := request(t, node, http.MethodGet, target, "", http.StatusOK)
					var result struct{ Key, Value string }
					if err := json.Unmarshal(body, &result); err != nil {
						t.Fatal(err)
					}
					if result.Key != key || result.Value != value {
						t.Fatalf("unexpected response: %.100s", body)
					}
				}
				previous = expected
			}
			request(t, owners[0], http.MethodDelete, target, "", http.StatusNoContent)
			var deletion storage.Record
			for _, node := range owners {
				record, found := node.store.GetRecord(key)
				if deletion.Version.Counter == 0 {
					deletion = record
				}
				if !found || record != deletion || !record.Deleted || record.Value != "" || record.Version.NodeID != owners[0].id || record.Version.Compare(previous.Version) <= 0 {
					t.Fatalf("invalid replicated deletion: %+v", record)
				}
				if applied, err := node.store.Apply(previous); applied || !errors.Is(err, storage.ErrStaleRecord) {
					t.Fatalf("stale update: %v %v", applied, err)
				}
			}
			for _, node := range cluster.nodes {
				request(t, node, http.MethodGet, target, "", http.StatusNotFound)
			}
		})
	}
}

func TestReplicatedPutAtPublicBodyLimit(t *testing.T) {
	cluster := newCluster(t, 5*time.Second, nil)
	key := "large-value"
	entry := cluster.outside(cluster.replicas(t, key))
	value := strings.Repeat("<", (1<<20)-len(`{"value":""}`))
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"`+value+`"}`, http.StatusNoContent)
	body := request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusOK)
	var result struct{ Value string }
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.Value != value {
		t.Fatal("replication changed large value")
	}
}

func TestUnavailableReplicaFailsAllReplicaPolicy(t *testing.T) {
	cluster := newCluster(t, time.Second, nil)
	key := "failure"
	owners := cluster.replicas(t, key)
	entry := cluster.outside(owners)
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"original"}`, http.StatusNoContent)
	owners[0].server.Close()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		request(t, entry, method, "/kv/"+key, `{"value":"replacement"}`, http.StatusServiceUnavailable)
	}
	for _, node := range cluster.nodes {
		value, found := node.store.Get(key)
		if found != slices.Contains(owners, node) || (found && value != "original") {
			t.Fatalf("failed observation changed storage on %s", node.id)
		}
	}
}

func TestSlowReplicaReturnsGatewayTimeout(t *testing.T) {
	slow := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			return
		}
		<-request.Context().Done()
	})
	cluster := newCluster(t, 200*time.Millisecond, map[string]http.Handler{"a": slow})
	key := cluster.keyForReplica(t, "a")
	entry := cluster.outside(cluster.replicas(t, key))
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		request(t, entry, method, "/kv/"+key, `{"value":"value"}`, http.StatusGatewayTimeout)
	}
}

func TestPartialWriteCanBeVisibleAfterFailure(t *testing.T) {
	failing := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Method == http.MethodPut {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusNotFound)
		if _, err := io.WriteString(writer, `{"error":"key not found"}`); err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	cluster := newCluster(t, time.Second, map[string]http.Handler{"a": failing})
	key := cluster.keyForReplica(t, "a")
	owners := cluster.replicas(t, key)
	entry := cluster.outside(owners)
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"partial"}`, http.StatusServiceUnavailable)
	for _, node := range owners {
		value, found := node.store.Get(key)
		if found != (node.id != "a") || (found && value != "partial") {
			t.Fatalf("unexpected partial write on %s", node.id)
		}
	}
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusOK)
}

func TestMalformedReplicaResponseReturnsBadGateway(t *testing.T) {
	malformed := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(writer, `{"key":"wrong","value":"value"}`); err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	cluster := newCluster(t, time.Second, map[string]http.Handler{"a": malformed})
	key := cluster.keyForReplica(t, "a")
	request(t, cluster.outside(cluster.replicas(t, key)), http.MethodGet, "/kv/"+key, "", http.StatusBadGateway)
}

func TestInternalRecordEndpointStaysLocalAndPreservesVersion(t *testing.T) {
	cluster := newCluster(t, time.Second, nil)
	key := "local-only"
	nonReplica := cluster.outside(cluster.replicas(t, key))
	record := storage.Record{Key: key, Value: "local", Version: storage.Version{Counter: 100, NodeID: "writer"}}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		request(t, nonReplica, http.MethodPut, "/internal/records/"+key, string(payload), http.StatusNoContent)
	}
	if actual, found := nonReplica.store.GetRecord(key); !found || actual != record {
		t.Fatalf("internal apply altered version: %+v", actual)
	}
	request(t, nonReplica, http.MethodGet, "/kv/"+key, "", http.StatusNotFound)
	request(t, nonReplica, http.MethodGet, "/internal/records/"+key, "", http.StatusOK)
	record.Version.Counter--
	payload, err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	request(t, nonReplica, http.MethodPut, "/internal/records/"+key, string(payload), http.StatusConflict)
}

func newCluster(t *testing.T, timeout time.Duration, overrides map[string]http.Handler) *testCluster {
	t.Helper()
	result := &testCluster{}
	var members []cluster.Member
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		server := httptest.NewUnstartedServer(nil)
		t.Cleanup(server.Close)
		store, err := storage.NewMemoryStore(id)
		if err != nil {
			t.Fatal(err)
		}
		result.nodes = append(result.nodes, &testNode{id: id, server: server, store: store})
		members = append(members, cluster.Member{ID: id, Address: "http://" + server.Listener.Addr().String()})
	}
	membership, err := cluster.NewMembership(members)
	if err != nil {
		t.Fatal(err)
	}
	result.ring, err = consistenthash.NewRing(membership.NodeIDs(), 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range result.nodes {
		client, err := transport.NewHTTPNodeClient(timeout)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.CloseIdleConnections)
		coordinator, err := replication.NewCoordinator(replication.Options{LocalID: node.id, Store: node.store, Membership: membership, Ring: result.ring, Client: client, ReplicationFactor: 3, Timeout: 2 * timeout})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.Handle("/internal/", api.NewReplicaHandler(node.store))
		mux.Handle("/", api.NewHandler(coordinator))
		node.server.Config.Handler = mux
		if override, found := overrides[node.id]; found {
			node.server.Config.Handler = override
		}
		node.server.Start()
		node.server.Client().Timeout = 15 * time.Second
	}
	return result
}

func (cluster *testCluster) replicas(t *testing.T, key string) []*testNode {
	t.Helper()
	ids, err := cluster.ring.GetNodes(key, 3)
	if err != nil {
		t.Fatal(err)
	}
	var owners []*testNode
	for _, node := range cluster.nodes {
		if slices.Contains(ids, node.id) {
			owners = append(owners, node)
		}
	}
	return owners
}

func (cluster *testCluster) outside(owners []*testNode) *testNode {
	for _, node := range cluster.nodes {
		if !slices.Contains(owners, node) {
			return node
		}
	}
	panic("test cluster has no node outside replica set")
}

func (cluster *testCluster) keyForReplica(t *testing.T, id string) string {
	t.Helper()
	for index := range 1000 {
		key := fmt.Sprint(index)
		for _, owner := range cluster.replicas(t, key) {
			if owner.id == id {
				return key
			}
		}
	}
	t.Fatalf("no key for replica %s", id)
	return ""
}

func escapeKey(key string) string {
	if key == "." {
		return "%2E"
	}
	if key == ".." {
		return "%2E%2E"
	}
	return url.PathEscape(key)
}

func request(t *testing.T, node *testNode, method, path, body string, expectedStatus int) []byte {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, node.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := node.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close response: %v", err)
		}
	}()
	encoded, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expectedStatus {
		t.Fatalf("%s %.100s: status %d, want %d; body %.100s", method, path, response.StatusCode, expectedStatus, encoded)
	}
	return encoded
}
