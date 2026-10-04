package routing_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/cluster"
	"kvstore/internal/consistenthash"
	"kvstore/internal/routing"
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

func TestCrossNodeKeyLifecycles(t *testing.T) {
	cluster := newCluster(t, 5*time.Second, nil)
	keys := []string{"user/42 name", "%2F", ".", "..", "日本語?#", strings.Repeat("k", 4<<10)}
	for index := 0; index < 24; index++ {
		keys = append(keys, fmt.Sprintf("user:%d", index))
	}
	for _, key := range keys {
		name := key
		if len(name) > 40 {
			name = "maximum key size"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			owner := cluster.owner(t, key)
			entry := cluster.other(owner)
			target := "/kv/" + escapeKey(key)
			for _, value := range []string{"Alice", ""} {
				payload, err := json.Marshal(map[string]string{"value": value})
				if err != nil {
					t.Fatal(err)
				}
				request(t, entry, http.MethodPut, target, string(payload), http.StatusNoContent)
				for _, node := range cluster.nodes {
					stored, found := node.store.Get(key)
					if found != (node == owner) || (found && stored != value) {
						t.Fatalf("wrong placement on %s: (%q, %v)", node.id, stored, found)
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
			}
			previous, found := owner.store.GetRecord(key)
			if !found || previous.Deleted || previous.Version.NodeID != owner.id {
				t.Fatalf("owner did not assign a version: %+v", previous)
			}
			request(t, entry, http.MethodDelete, target, "", http.StatusNoContent)
			deleted, found := owner.store.GetRecord(key)
			if !found || !deleted.Deleted || deleted.Value != "" || deleted.Version.Compare(previous.Version) <= 0 {
				t.Fatalf("delete did not retain a newer marker: %+v", deleted)
			}
			if applied, err := owner.store.Apply(previous); applied || err != nil {
				t.Fatalf("stale write after HTTP delete = (%v, %v)", applied, err)
			}
			for _, node := range cluster.nodes {
				request(t, node, http.MethodGet, target, "", http.StatusNotFound)
			}
		})
	}
}

func TestForwardedPutAtPublicBodyLimit(t *testing.T) {
	cluster := newCluster(t, 5*time.Second, nil)
	key := "large-value"
	owner := cluster.owner(t, key)
	entry := cluster.other(owner)
	value := strings.Repeat("<", (1<<20)-len(`{"value":""}`))
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"`+value+`"}`, http.StatusNoContent)
	body := request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusOK)
	var result struct{ Value string }
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.Value != value {
		t.Fatal("forwarding changed the large value")
	}
}

func TestUnavailableOwnerDoesNotChangePlacement(t *testing.T) {
	cluster := newCluster(t, time.Second, nil)
	key := "owner-failure"
	owner := cluster.owner(t, key)
	entry := cluster.other(owner)
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"original"}`, http.StatusNoContent)
	owner.server.Close()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		request(t, entry, method, "/kv/"+key, `{"value":"replacement"}`, http.StatusServiceUnavailable)
	}
	for _, node := range cluster.nodes {
		value, found := node.store.Get(key)
		if found != (node == owner) || (found && value != "original") {
			t.Fatalf("failure changed placement on %s", node.id)
		}
	}
}

func TestSlowOwnerReturnsGatewayTimeout(t *testing.T) {
	slow := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			return
		}
		<-request.Context().Done()
	})
	cluster := newCluster(t, 200*time.Millisecond, map[string]http.Handler{"a": slow})
	key := cluster.keyForOwner(t, "a")
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		request(t, cluster.nodes[1], method, "/kv/"+key, `{"value":"value"}`, http.StatusGatewayTimeout)
	}
}

func TestInternalEndpointNeverForwards(t *testing.T) {
	cluster := newCluster(t, time.Second, nil)
	key := "internal-local-access"
	owner := cluster.owner(t, key)
	nonOwner := cluster.other(owner)
	request(t, nonOwner, http.MethodPut, "/internal/kv/"+key, `{"value":"local-only"}`, http.StatusNoContent)
	if value, found := nonOwner.store.Get(key); !found || value != "local-only" {
		t.Fatal("internal write did not reach local storage")
	}
	if _, found := owner.store.Get(key); found {
		t.Fatal("internal write was forwarded")
	}
	request(t, nonOwner, http.MethodGet, "/internal/kv/"+key, "", http.StatusOK)
	request(t, nonOwner, http.MethodGet, "/kv/"+key, "", http.StatusNotFound)
	request(t, nonOwner, http.MethodDelete, "/internal/kv/"+key, "", http.StatusNoContent)
	if _, found := nonOwner.store.Get(key); found {
		t.Fatal("internal delete did not change local storage")
	}
}

func newCluster(t *testing.T, timeout time.Duration, overrides map[string]http.Handler) *testCluster {
	t.Helper()
	result := &testCluster{}
	var members []cluster.Member
	for _, id := range []string{"a", "b", "c"} {
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
		router, err := routing.NewRouter(routing.Options{LocalID: node.id, Store: node.store, Membership: membership, Ring: result.ring, Client: client})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.Handle("/internal/", api.NewInternalHandler(node.store))
		mux.Handle("/", api.NewHandler(router))
		node.server.Config.Handler = mux
		if override, found := overrides[node.id]; found {
			node.server.Config.Handler = override
		}
		node.server.Start()
		node.server.Client().Timeout = 10 * time.Second
	}
	return result
}

func (cluster *testCluster) owner(t *testing.T, key string) *testNode {
	t.Helper()
	owners, err := cluster.ring.GetNodes(key, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range cluster.nodes {
		if node.id == owners[0] {
			return node
		}
	}
	t.Fatal("owner missing from test cluster")
	return nil
}

func (cluster *testCluster) other(owner *testNode) *testNode {
	for _, node := range cluster.nodes {
		if node != owner {
			return node
		}
	}
	panic("test cluster has no other node")
}

func (cluster *testCluster) keyForOwner(t *testing.T, id string) string {
	t.Helper()
	for index := 0; index < 1000; index++ {
		key := fmt.Sprintf("key:%d", index)
		if cluster.owner(t, key).id == id {
			return key
		}
	}
	t.Fatalf("no test key found for %s", id)
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
