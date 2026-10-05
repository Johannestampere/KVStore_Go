package replication_test

import (
	"io"
	"net/http"
	"testing"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/storage"
)

func TestQuorumServesWithOneReplicaDownAndFailsWithTwo(t *testing.T) {
	cluster := newClusterWithQuorums(t, time.Second, nil, 2, 2)
	key := "quorum-outage"
	owners := cluster.replicas(t, key)
	entry := cluster.outside(owners)
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"original"}`, http.StatusNoContent)
	owners[0].server.Close()
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusOK)
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"updated"}`, http.StatusNoContent)
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusOK)
	for _, owner := range owners[1:] {
		if value, found := owner.store.Get(key); !found || value != "updated" {
			t.Fatalf("live replica %s missed write", owner.id)
		}
	}
	request(t, entry, http.MethodDelete, "/kv/"+key, "", http.StatusNoContent)
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusNotFound)
	for _, owner := range owners[1:] {
		if record, found := owner.store.GetRecord(key); !found || !record.Deleted {
			t.Fatalf("live replica %s missed deletion", owner.id)
		}
	}
	owners[1].server.Close()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		request(t, entry, method, "/kv/"+key, `{"value":"unacknowledged"}`, http.StatusServiceUnavailable)
	}
	if _, found := entry.store.GetRecord(key); found {
		t.Fatal("failure moved data to coordinator outside replica set")
	}
}

func TestQuorumIgnoresSlowReplica(t *testing.T) {
	slow := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			return
		}
		<-request.Context().Done()
	})
	cluster := newClusterWithQuorums(t, 300*time.Millisecond, map[string]http.Handler{"a": slow}, 2, 2)
	key := cluster.keyForReplica(t, "a")
	entry := cluster.outside(cluster.replicas(t, key))
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusNotFound)
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"value"}`, http.StatusNoContent)
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusOK)
	request(t, entry, http.MethodDelete, "/kv/"+key, "", http.StatusNoContent)
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusNotFound)
}

func TestQuorumToleratesOneMalformedReplica(t *testing.T) {
	malformed := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(writer, `{`); err != nil {
			t.Errorf("write malformed response: %v", err)
		}
	})
	cluster := newClusterWithQuorums(t, time.Second, map[string]http.Handler{"a": malformed}, 2, 2)
	key := cluster.keyForReplica(t, "a")
	entry := cluster.outside(cluster.replicas(t, key))
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"value"}`, http.StatusNoContent)
	request(t, entry, http.MethodGet, "/kv/"+key, "", http.StatusOK)
}

func TestAcknowledgedHTTPWriteContinuesAfterResponseCloses(t *testing.T) {
	store, err := storage.NewMemoryStore("a")
	if err != nil {
		t.Fatal(err)
	}
	local := api.NewReplicaHandler(store)
	started, release, finished := make(chan struct{}), make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	slow := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPut {
			close(started)
			defer close(finished)
			select {
			case <-release:
			case <-request.Context().Done():
				return
			}
		}
		local.ServeHTTP(writer, request)
	})
	cluster := newClusterWithQuorums(t, 2*time.Second, map[string]http.Handler{"a": slow}, 2, 2)
	key := cluster.keyForReplica(t, "a")
	entry := cluster.outside(cluster.replicas(t, key))
	request(t, entry, http.MethodPut, "/kv/"+key, `{"value":"value"}`, http.StatusNoContent)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("third replica was not contacted")
	}
	if _, found := store.GetRecord(key); found {
		t.Fatal("blocked replica already contains write")
	}
	// Release without closing so the deferred close also unblocks failed tests.
	release <- struct{}{}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("background HTTP request did not finish")
	}
	record, found := store.GetRecord(key)
	if !found || record.Value != "value" {
		t.Fatal("client response closure canceled the third write")
	}
	for _, owner := range cluster.replicas(t, key) {
		if owner.id == "a" {
			continue
		}
		if actual, found := owner.store.GetRecord(key); !found || actual != record {
			t.Fatalf("background replica differs from %s", owner.id)
		}
	}
}
