package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"kvstore/internal/cluster"
)

// ErrQuorumUnavailable indicates that too few replicas can satisfy an operation.
var ErrQuorumUnavailable = errors.New("quorum unavailable")

// ValidateQuorums requires bounded, overlapping read and write quorums.
func ValidateQuorums(replicationFactor, readQuorum, writeQuorum int) error {
	if replicationFactor < 1 || readQuorum < 1 || readQuorum > replicationFactor || writeQuorum < 1 || writeQuorum > replicationFactor {
		return errors.New("read_quorum and write_quorum must be between one and replication_factor")
	}
	if readQuorum <= replicationFactor-writeQuorum {
		return errors.New("read_quorum + write_quorum must exceed replication_factor")
	}
	return nil
}

type replicaResult[T any] struct {
	value T
	err   error
}

func startCalls[T any](ctx context.Context, members []cluster.Member, operation func(context.Context, cluster.Member) (T, error), finished func()) <-chan replicaResult[T] {
	// Workers can finish after quorum returns without blocking on a result send.
	results := make(chan replicaResult[T], len(members))
	var workers sync.WaitGroup
	for _, member := range members {
		workers.Add(1)
		go func() {
			defer workers.Done()
			value, err := operation(ctx, member)
			if err != nil {
				err = fmt.Errorf("replica %s: %w", member.ID, err)
			}
			results <- replicaResult[T]{value, err}
		}()
	}
	go func() {
		workers.Wait()
		close(results)
		finished()
	}()
	return results
}

func awaitQuorum[T any](ctx context.Context, results <-chan replicaResult[T], total, required int) ([]T, error) {
	values := make([]T, 0, required)
	var failures []error
	for remaining := total; remaining > 0; remaining-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case result := <-results:
			if result.err == nil {
				values = append(values, result.value)
			} else {
				failures = append(failures, result.err)
			}
			if len(values) >= required {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return values, nil
			}
			if len(values)+remaining-1 < required {
				return nil, errors.Join(fmt.Errorf("%w: %d successful responses, need %d", ErrQuorumUnavailable, len(values), required), errors.Join(failures...))
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, ErrQuorumUnavailable
}
