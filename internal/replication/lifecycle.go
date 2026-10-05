package replication

import (
	"context"
	"fmt"
)

func (coordinator *Coordinator) begin(ctx context.Context) (context.Context, func(), error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.closing {
		return nil, nil, fmt.Errorf("%w: coordinator is shutting down", ErrUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	coordinator.pending.Add(1)
	ctx, cancel := context.WithTimeout(ctx, coordinator.timeout)
	stopShutdown := context.AfterFunc(coordinator.lifetime, cancel)
	return ctx, func() { stopShutdown(); cancel(); coordinator.pending.Done() }, nil
}

// Shutdown stops admission and drains replica calls; expiration cancels remaining work.
func (coordinator *Coordinator) Shutdown(ctx context.Context) error {
	coordinator.mu.Lock()
	if !coordinator.closing {
		coordinator.closing = true
		go func() { coordinator.pending.Wait(); close(coordinator.drained) }()
	}
	coordinator.mu.Unlock()
	select {
	case <-coordinator.drained:
		coordinator.stop()
		return nil
	case <-ctx.Done():
		coordinator.stop()
		return ctx.Err()
	}
}

func (coordinator *Coordinator) writeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// Keep the original deadline, but detach cancellation after quorum succeeds.
	deadline, _ := ctx.Deadline() // begin always establishes an operation deadline.
	writeContext, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	stopShutdown := context.AfterFunc(coordinator.lifetime, cancel)
	return writeContext, func() { stopShutdown(); cancel() }
}
