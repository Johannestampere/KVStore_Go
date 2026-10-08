package replication

import (
	"context"
	"errors"
)

// Observer records failures concurrently; operations and phases are read or write.
type Observer interface {
	RecordReplicaFailure(operation string)
	RecordQuorumFailure(phase string)
}

func (coordinator *Coordinator) observeReplicaFailure(operation string, err error) {
	if coordinator.observer != nil && err != nil && !errors.Is(err, context.Canceled) {
		coordinator.observer.RecordReplicaFailure(operation)
	}
}

func (coordinator *Coordinator) observeQuorumFailure(phase string, err error) {
	if coordinator.observer == nil || err == nil || errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, ErrQuorumUnavailable) || errors.Is(err, context.DeadlineExceeded) {
		coordinator.observer.RecordQuorumFailure(phase)
	}
}
