package routing_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"kvstore/internal/cluster"
	"kvstore/internal/consistenthash"
	"kvstore/internal/routing"
	"kvstore/internal/storage"
)

type nodeClientSpy struct {
	calls      int
	member     cluster.Member
	key, value string
	ctx        context.Context
	err        error
}

func (client *nodeClientSpy) Put(ctx context.Context, member cluster.Member, key, value string) error {
	client.calls++
	client.ctx, client.member, client.key, client.value = ctx, member, key, value
	return client.err
}

func (client *nodeClientSpy) Get(ctx context.Context, member cluster.Member, key string) (string, bool, error) {
	client.calls++
	client.ctx, client.member, client.key = ctx, member, key
	return client.value, client.err == nil, client.err
}

func (client *nodeClientSpy) Delete(ctx context.Context, member cluster.Member, key string) error {
	client.calls++
	client.ctx, client.member, client.key = ctx, member, key
	return client.err
}

func TestLocalServicePropagatesStorageErrors(t *testing.T) {
	_, store, _ := newOptions(t)
	record := storage.Record{Key: "key", Version: storage.Version{Counter: math.MaxUint64, NodeID: "remote"}}
	if _, err := store.Apply(record); err != nil {
		t.Fatal(err)
	}
	service := routing.NewLocalService(store)
	if err := service.Put(t.Context(), "key", "value"); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("Put error = %v", err)
	}
	if err := service.Delete(t.Context(), "key"); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("Delete error = %v", err)
	}
}

func TestRouterLocalLifecycle(t *testing.T) {
	options, store, client := newOptions(t)
	router := mustRouter(t, options)
	key := keyForOwner(t, options.Ring, "a")
	if err := router.Put(t.Context(), key, ""); err != nil {
		t.Fatal(err)
	}
	if value, found, err := router.Get(t.Context(), key); err != nil || !found || value != "" {
		t.Fatalf("Get = (%q, %v, %v), want empty stored value", value, found, err)
	}
	if _, found := store.Get(key); !found {
		t.Fatal("local store was not written")
	}
	if err := router.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if _, found, err := router.Get(t.Context(), key); err != nil || found {
		t.Fatalf("Get after delete = (%v, %v)", found, err)
	}
	if client.calls != 0 {
		t.Fatalf("local operations made %d peer calls", client.calls)
	}
}

func TestRouterRemoteLifecycle(t *testing.T) {
	options, store, client := newOptions(t)
	router := mustRouter(t, options)
	key := keyForOwner(t, options.Ring, "b")
	if err := router.Put(t.Context(), key, "value"); err != nil {
		t.Fatal(err)
	}
	if value, found, err := router.Get(t.Context(), key); err != nil || !found || value != "value" {
		t.Fatalf("Get = (%q, %v, %v)", value, found, err)
	}
	if err := router.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	member, found := options.Membership.Lookup("b")
	if !found {
		t.Fatal("missing test member")
	}
	if client.calls != 3 || client.member != member || client.key != key || client.ctx != t.Context() {
		t.Fatalf("unexpected peer calls: %+v", client)
	}
	if _, found := store.Get(key); found {
		t.Fatal("remote write also modified local store")
	}
}

func TestRouterDoesNotFallBackOnPeerFailure(t *testing.T) {
	for _, failure := range []error{routing.ErrUnavailable, context.DeadlineExceeded, routing.ErrInvalidResponse} {
		t.Run(failure.Error(), func(t *testing.T) {
			options, store, client := newOptions(t)
			client.err = failure
			router := mustRouter(t, options)
			key := keyForOwner(t, options.Ring, "b")
			if err := store.Put(key, "stale local value"); err != nil {
				t.Fatal(err)
			}
			if err := router.Put(t.Context(), key, "new"); !errors.Is(err, failure) {
				t.Fatalf("Put error = %v", err)
			}
			if _, found, err := router.Get(t.Context(), key); found || !errors.Is(err, failure) {
				t.Fatalf("Get = (%v, %v)", found, err)
			}
			if err := router.Delete(t.Context(), key); !errors.Is(err, failure) {
				t.Fatalf("Delete error = %v", err)
			}
			if value, found := store.Get(key); !found || value != "stale local value" {
				t.Fatal("failed operation changed local storage")
			}
			if client.calls != 3 {
				t.Fatalf("peer calls = %d, want 3 without retries", client.calls)
			}
		})
	}
}

func TestCanceledOperationsDoNotAccessStorageOrPeers(t *testing.T) {
	options, store, client := newOptions(t)
	services := []routingService{routing.NewLocalService(store), mustRouter(t, options)}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, service := range services {
		for _, owner := range []string{"a", "b"} {
			key := keyForOwner(t, options.Ring, owner)
			if err := store.Put(key, "original"); err != nil {
				t.Fatal(err)
			}
			if err := service.Put(ctx, key, "new"); !errors.Is(err, context.Canceled) {
				t.Fatalf("Put error = %v", err)
			}
			if _, _, err := service.Get(ctx, key); !errors.Is(err, context.Canceled) {
				t.Fatalf("Get error = %v", err)
			}
			if err := service.Delete(ctx, key); !errors.Is(err, context.Canceled) {
				t.Fatalf("Delete error = %v", err)
			}
			if value, found := store.Get(key); !found || value != "original" {
				t.Fatal("canceled operation changed storage")
			}
		}
	}
	if client.calls != 0 {
		t.Fatal("canceled operation contacted peer")
	}
}

type routingService interface {
	Put(context.Context, string, string) error
	Get(context.Context, string) (string, bool, error)
	Delete(context.Context, string) error
}

func TestNewRouterRejectsInvalidDependencies(t *testing.T) {
	cases := map[string]func(*routing.Options){
		"no store":         func(options *routing.Options) { options.Store = nil },
		"no client":        func(options *routing.Options) { options.Client = nil },
		"no ring":          func(options *routing.Options) { options.Ring = nil },
		"no membership":    func(options *routing.Options) { options.Membership = nil },
		"unknown local ID": func(options *routing.Options) { options.LocalID = "unknown" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			options, _, _ := newOptions(t)
			change(&options)
			if _, err := routing.NewRouter(options); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
	for _, ids := range [][]string{{"a"}, {"a", "unknown"}, {"a", "b", "extra"}} {
		t.Run(fmt.Sprint(ids), func(t *testing.T) {
			options, _, _ := newOptions(t)
			ring, err := consistenthash.NewRing(ids, 16)
			if err != nil {
				t.Fatal(err)
			}
			options.Ring = ring
			if _, err := routing.NewRouter(options); err == nil {
				t.Fatal("accepted inconsistent topology")
			}
		})
	}
}

func newOptions(t *testing.T) (routing.Options, *storage.MemoryStore, *nodeClientSpy) {
	t.Helper()
	membership, err := cluster.NewMembership([]cluster.Member{{ID: "a", Address: "http://127.0.0.1:8001"}, {ID: "b", Address: "http://127.0.0.1:8002"}})
	if err != nil {
		t.Fatal(err)
	}
	ring, err := consistenthash.NewRing(membership.NodeIDs(), 16)
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewMemoryStore("a")
	if err != nil {
		t.Fatal(err)
	}
	client := &nodeClientSpy{}
	return routing.Options{LocalID: "a", Store: store, Membership: membership, Ring: ring, Client: client}, store, client
}

func mustRouter(t *testing.T, options routing.Options) *routing.Router {
	t.Helper()
	router, err := routing.NewRouter(options)
	if err != nil {
		t.Fatal(err)
	}
	return router
}

func keyForOwner(t *testing.T, ring *consistenthash.Ring, owner string) string {
	t.Helper()
	for index := 0; index < 1000; index++ {
		key := fmt.Sprintf("key:%d", index)
		owners, err := ring.GetNodes(key, 1)
		if err != nil {
			t.Fatal(err)
		}
		if owners[0] == owner {
			return key
		}
	}
	t.Fatalf("no key found for owner %s", owner)
	return ""
}
