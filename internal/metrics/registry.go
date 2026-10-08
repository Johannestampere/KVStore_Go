// Package metrics records bounded, node-local operational measurements.
package metrics

import (
	"maps"
	"net/http"
	"sync"
	"time"
)

var latencyBounds = [...]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}

type httpLabels struct {
	scope  string
	method string
}

type requestLabels struct {
	httpLabels
	status int
}

type histogram struct {
	count   uint64
	sum     float64
	buckets [len(latencyBounds)]uint64
}

type snapshot struct {
	requests        map[requestLabels]uint64
	durations       map[httpLabels]histogram
	replicaFailures [3]uint64
	quorumFailures  [3]uint64
}

// Registry collects concurrent observations. Its zero value is ready to use.
// Do not copy it after use. Measurements reset when the process restarts.
type Registry struct {
	mu           sync.Mutex
	measurements snapshot
}

// ObserveHTTP records one completed handler call, normalizing unknown labels.
func (registry *Registry) ObserveHTTP(scope, method string, status int, elapsed time.Duration) {
	labels := httpLabels{scope: normalizeScope(scope), method: normalizeMethod(method)}
	if status < 100 || status > 599 {
		status = 0
	}
	seconds := max(0, elapsed.Seconds())
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.measurements.requests == nil {
		registry.measurements.requests = make(map[requestLabels]uint64)
		registry.measurements.durations = make(map[httpLabels]histogram)
	}
	registry.measurements.requests[requestLabels{labels, status}]++
	duration := registry.measurements.durations[labels]
	duration.count++
	duration.sum += seconds
	for index, bound := range latencyBounds {
		if seconds <= bound {
			duration.buckets[index]++
		}
	}
	registry.measurements.durations[labels] = duration
}

// RecordReplicaFailure counts a failed local or remote replica read or write.
func (registry *Registry) RecordReplicaFailure(operation string) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.measurements.replicaFailures[phaseIndex(operation)]++
}

// RecordQuorumFailure counts a read or write phase that failed to reach quorum.
func (registry *Registry) RecordQuorumFailure(phase string) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.measurements.quorumFailures[phaseIndex(phase)]++
}

func (registry *Registry) snapshot() snapshot {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return snapshot{
		requests:        maps.Clone(registry.measurements.requests),
		durations:       maps.Clone(registry.measurements.durations),
		replicaFailures: registry.measurements.replicaFailures,
		quorumFailures:  registry.measurements.quorumFailures,
	}
}

func normalizeScope(scope string) string {
	switch scope {
	case "public", "internal":
		return scope
	default:
		return "other"
	}
}

func normalizeMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
		return method
	default:
		return "OTHER"
	}
}

func phaseIndex(phase string) int {
	switch phase {
	case "read":
		return 0
	case "write":
		return 1
	default:
		return 2
	}
}
