package benchmark

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Report includes workload settings and separately timed preparation results.
type Report struct {
	Config       Config    `json:"config"`
	KeyPrefix    string    `json:"key_prefix"`
	StartedAt    time.Time `json:"started_at"`
	GoVersion    string    `json:"go_version"`
	Platform     string    `json:"platform"`
	Completed    bool      `json:"completed"`
	Preparation  *Result   `json:"preparation,omitempty"`
	Measurements Result    `json:"measurements"`
}

// Runner executes independent requests without application retries or redirects.
type Runner struct {
	config Config
	value  string
	body   []byte
}

// NewRunner validates settings and prepares the shared immutable PUT payload.
func NewRunner(config Config) (*Runner, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	config.Nodes = append([]string(nil), config.Nodes...)
	for index, address := range config.Nodes {
		config.Nodes[index] = strings.TrimSuffix(address, "/")
	}
	value := strings.Repeat("x", config.ValueBytes)
	body, err := json.Marshal(struct {
		Value string `json:"value"`
	}{Value: value})
	if err != nil {
		return nil, fmt.Errorf("encode benchmark value: %w", err)
	}
	return &Runner{config: config, value: value, body: body}, nil
}

// Run preloads read keys, then measures a closed-loop workload until completion or cancellation.
func (runner *Runner) Run(ctx context.Context) (Report, error) {
	report := Report{
		Config: runner.config, KeyPrefix: "bench-" + rand.Text(),
		GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = runner.config.Concurrency
	transport.MaxIdleConnsPerHost = runner.config.Concurrency
	client := &http.Client{
		Transport: transport, Timeout: runner.config.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer transport.CloseIdleConnections()
	if runner.config.ReadPercent > 0 {
		preparation := runner.execute(ctx, runner.config.ReadKeys, func(index int) Sample {
			return runner.request(ctx, client, http.MethodPut, runner.readKey(report.KeyPrefix, index), index)
		})
		report.Preparation = &preparation
		if err := ctx.Err(); err != nil {
			return report, fmt.Errorf("preload interrupted: %w", err)
		}
		if preparation.Total.Failed > 0 {
			return report, fmt.Errorf("preload failed for %d of %d keys", preparation.Total.Failed, runner.config.ReadKeys)
		}
	}
	report.StartedAt = time.Now().UTC()
	report.Measurements = runner.execute(ctx, runner.config.Requests, func(index int) Sample {
		method, key, ordinal := runner.operation(report.KeyPrefix, index)
		return runner.request(ctx, client, method, key, ordinal)
	})
	if err := ctx.Err(); err != nil {
		return report, fmt.Errorf("benchmark interrupted: %w", err)
	}
	report.Completed = true
	return report, nil
}

func (runner *Runner) operation(prefix string, index int) (string, string, int) {
	// Evenly interleave reads; each method independently rotates across entry nodes.
	readsBefore := (index/100)*runner.config.ReadPercent + (index%100)*runner.config.ReadPercent/100
	read := ((index%100)+1)*runner.config.ReadPercent/100 > (index%100)*runner.config.ReadPercent/100
	if read {
		return http.MethodGet, runner.readKey(prefix, readsBefore%runner.config.ReadKeys), readsBefore
	}
	ordinal := index - readsBefore
	return http.MethodPut, fmt.Sprintf("%s-write-%d", prefix, ordinal), ordinal
}

func (runner *Runner) readKey(prefix string, index int) string {
	return fmt.Sprintf("%s-read-%d", prefix, index)
}

func (runner *Runner) execute(ctx context.Context, count int, operation func(int) Sample) Result {
	samples := make([]Sample, count)
	var next atomic.Int64
	var workers sync.WaitGroup
	started := time.Now()
	for range min(runner.config.Concurrency, count) {
		workers.Go(func() {
			for ctx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				if ctx.Err() != nil {
					return
				}
				// Each worker owns a distinct slot; aggregation starts after Wait.
				samples[index] = operation(index)
			}
		})
	}
	workers.Wait()
	elapsed := time.Since(started)
	attempted := samples[:0]
	for _, sample := range samples {
		if sample.Method != "" {
			attempted = append(attempted, sample)
		}
	}
	return Summarize(attempted, elapsed)
}
