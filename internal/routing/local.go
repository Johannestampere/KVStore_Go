// Package routing directs key-value operations to their assigned owner.
package routing

import "context"

// Store provides concurrent node-local storage.
type Store interface {
	Put(key, value string) error
	Get(key string) (string, bool)
	Delete(key string) error
}

// LocalService adapts local storage to cancellable application operations.
type LocalService struct {
	store Store
}

// NewLocalService wraps a non-nil store.
func NewLocalService(store Store) *LocalService {
	return &LocalService{store: store}
}

// Put stores a value unless the request is already canceled.
func (service *LocalService) Put(ctx context.Context, key, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return service.store.Put(key, value)
}

// Get reads a value unless the request is already canceled.
func (service *LocalService) Get(ctx context.Context, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	value, found := service.store.Get(key)
	return value, found, nil
}

// Delete removes a key unless the request is already canceled.
func (service *LocalService) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return service.store.Delete(key)
}
