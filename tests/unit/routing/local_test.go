package routing_test

import (
	"context"
	"errors"
	"kvstore/internal/routing"
	"kvstore/internal/storage"
	"math"
	"testing"
)

func TestLocalServicePropagatesStorageErrors(t *testing.T) {
	store, err := storage.NewMemoryStore("local")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(storage.Record{Key: "key", Version: storage.Version{Counter: math.MaxUint64, NodeID: "remote"}}); err != nil {
		t.Fatal(err)
	}
	service := routing.NewLocalService(store)
	if err := service.Put(t.Context(), "key", "value"); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("Put: %v", err)
	}
	if err := service.Delete(t.Context(), "key"); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("Delete: %v", err)
	}
}

func TestCanceledLocalOperationsPreserveStorage(t *testing.T) {
	store, err := storage.NewMemoryStore("local")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", "original"); err != nil {
		t.Fatal(err)
	}
	service := routing.NewLocalService(store)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := service.Put(ctx, "key", "new"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put: %v", err)
	}
	if _, _, err := service.Get(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get: %v", err)
	}
	if err := service.Delete(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete: %v", err)
	}
	if value, found := store.Get("key"); !found || value != "original" {
		t.Fatal("canceled operation changed storage")
	}
}
