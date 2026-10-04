// Package replication coordinates versioned operations across a fixed replica set.
package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/consistenthash"
	"kvstore/internal/storage"
)

// ErrUnavailable indicates an unreachable replica.
var ErrUnavailable = errors.New("replica unavailable")

// ErrInvalidResponse indicates a violation of the replica protocol.
var ErrInvalidResponse = errors.New("invalid replica response")

// Store provides atomic record updates and unique local versions.
type Store interface {
	GetRecord(string) (storage.Record, bool)
	Apply(storage.Record) (bool, error)
	NextVersion(storage.Version) (storage.Version, error)
}

// ReplicaClient accesses node-local records and must honor context cancellation.
type ReplicaClient interface {
	GetRecord(context.Context, cluster.Member, string) (storage.Record, bool, error)
	Apply(context.Context, cluster.Member, storage.Record) error
}

// Options supplies immutable topology and coordinator dependencies.
type Options struct {
	LocalID           string
	Store             Store
	Membership        *cluster.Membership
	Ring              *consistenthash.Ring
	Client            ReplicaClient
	ReplicationFactor int
	Timeout           time.Duration
}

// Coordinator requires every selected replica for reads and writes.
type Coordinator struct {
	localID           string
	store             Store
	membership        *cluster.Membership
	ring              *consistenthash.Ring
	client            ReplicaClient
	replicationFactor int
	timeout           time.Duration
}

// NewCoordinator validates placement and dependencies before serving requests.
func NewCoordinator(options Options) (*Coordinator, error) {
	if options.Store == nil || options.Membership == nil || options.Ring == nil || options.Client == nil {
		return nil, errors.New("replication requires storage, membership, a ring, and a client")
	}
	if options.Timeout <= 0 {
		return nil, errors.New("replication timeout must be positive")
	}
	if _, found := options.Membership.Lookup(options.LocalID); !found {
		return nil, fmt.Errorf("local node %q is not a member", options.LocalID)
	}
	ids := options.Membership.NodeIDs()
	if options.ReplicationFactor < 1 || options.ReplicationFactor > len(ids) {
		return nil, errors.New("replication factor must be between one and the member count")
	}
	owners, err := options.Ring.GetNodes("", len(ids))
	if err != nil {
		return nil, fmt.Errorf("replication topology: %w", err)
	}
	if _, err := options.Ring.GetNodes("", len(ids)+1); err == nil {
		return nil, errors.New("ring contains nodes outside membership")
	}
	for _, id := range owners {
		if _, found := options.Membership.Lookup(id); !found {
			return nil, fmt.Errorf("ring node %q is not a member", id)
		}
	}
	return &Coordinator{
		localID: options.LocalID, store: options.Store,
		membership: options.Membership, ring: options.Ring, client: options.Client,
		replicationFactor: options.ReplicationFactor, timeout: options.Timeout,
	}, nil
}

// Put assigns one version and writes the same value to every selected replica.
func (coordinator *Coordinator) Put(ctx context.Context, key, value string) error {
	return coordinator.write(ctx, storage.Record{Key: key, Value: value})
}

// Delete replicates a versioned deletion marker, including for absent keys.
func (coordinator *Coordinator) Delete(ctx context.Context, key string) error {
	return coordinator.write(ctx, storage.Record{Key: key, Deleted: true})
}

// Get returns the highest version observed after every replica responds.
func (coordinator *Coordinator) Get(ctx context.Context, key string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, coordinator.timeout)
	defer cancel()
	replicas, err := coordinator.replicas(key)
	if err != nil {
		return "", false, err
	}
	record, found, err := coordinator.read(ctx, replicas, key)
	if err != nil || !found || record.Deleted {
		return "", false, err
	}
	return record.Value, true, nil
}

func (coordinator *Coordinator) write(ctx context.Context, record storage.Record) error {
	ctx, cancel := context.WithTimeout(ctx, coordinator.timeout)
	defer cancel()
	replicas, err := coordinator.replicas(record.Key)
	if err != nil {
		return err
	}
	latest, _, err := coordinator.read(ctx, replicas, record.Key)
	if err != nil {
		return fmt.Errorf("observe replicas: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	record.Version, err = coordinator.store.NextVersion(latest.Version)
	if err != nil {
		return err
	}
	_, err = collect(ctx, replicas, func(ctx context.Context, member cluster.Member) (struct{}, error) {
		if err := ctx.Err(); err != nil {
			return struct{}{}, err
		}
		if member.ID == coordinator.localID {
			_, err := coordinator.store.Apply(record)
			return struct{}{}, err
		}
		return struct{}{}, coordinator.client.Apply(ctx, member, record)
	})
	return err
}

type replicaRecord struct {
	record storage.Record
	found  bool
}

func (coordinator *Coordinator) read(ctx context.Context, replicas []cluster.Member, key string) (storage.Record, bool, error) {
	responses, err := collect(ctx, replicas, func(ctx context.Context, member cluster.Member) (replicaRecord, error) {
		if err := ctx.Err(); err != nil {
			return replicaRecord{}, err
		}
		if member.ID == coordinator.localID {
			record, found := coordinator.store.GetRecord(key)
			return replicaRecord{record, found}, nil
		}
		record, found, err := coordinator.client.GetRecord(ctx, member, key)
		return replicaRecord{record, found}, err
	})
	if err != nil {
		return storage.Record{}, false, err
	}
	var latest replicaRecord
	seen := make(map[storage.Version]storage.Record)
	for _, response := range responses {
		if !response.found {
			continue
		}
		record := response.record
		if err := record.Validate(); err != nil || record.Key != key {
			return storage.Record{}, false, ErrInvalidResponse
		}
		if previous, found := seen[record.Version]; found && previous != record {
			return storage.Record{}, false, storage.ErrVersionConflict
		}
		seen[record.Version] = record
		if !latest.found || record.Version.Compare(latest.record.Version) > 0 {
			latest = response
		}
	}
	return latest.record, latest.found, nil
}

func (coordinator *Coordinator) replicas(key string) ([]cluster.Member, error) {
	ids, err := coordinator.ring.GetNodes(key, coordinator.replicationFactor)
	if err != nil {
		return nil, err
	}
	members := make([]cluster.Member, 0, len(ids))
	for _, id := range ids {
		member, found := coordinator.membership.Lookup(id)
		if !found {
			return nil, fmt.Errorf("replica %q is not a member", id)
		}
		members = append(members, member)
	}
	return members, nil
}

type replicaResult[T any] struct {
	index int
	value T
	err   error
}

func collect[T any](ctx context.Context, members []cluster.Member, operation func(context.Context, cluster.Member) (T, error)) ([]T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Each worker can finish even if the caller returns on cancellation.
	results := make(chan replicaResult[T], len(members))
	for index, member := range members {
		go func() {
			value, err := operation(ctx, member)
			if err != nil {
				err = fmt.Errorf("replica %s: %w", member.ID, err)
			}
			results <- replicaResult[T]{index, value, err}
		}()
	}
	values := make([]T, len(members))
	failures := make([]error, len(members))
	for range members {
		select {
		case result := <-results:
			values[result.index], failures[result.index] = result.value, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return values, errors.Join(failures...)
}
