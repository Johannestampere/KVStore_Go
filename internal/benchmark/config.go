// Package benchmark measures HTTP workloads with a fixed number of clients.
package benchmark

import (
	"fmt"
	"net/url"
	"time"
)

// Config describes a deterministic workload. Description records deployment details.
type Config struct {
	Nodes       []string      `json:"nodes"`
	Requests    int           `json:"requests"`
	Concurrency int           `json:"concurrency"`
	ReadPercent int           `json:"read_percent"`
	ReadKeys    int           `json:"read_keys"`
	ValueBytes  int           `json:"value_bytes"`
	Timeout     time.Duration `json:"request_timeout_ns"`
	Description string        `json:"description"`
}

// Validate rejects settings that cannot produce a valid workload.
func (config Config) Validate() error {
	if len(config.Nodes) == 0 {
		return fmt.Errorf("at least one node URL is required")
	}
	for _, address := range config.Nodes {
		parsed, err := url.Parse(address)
		if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("invalid node URL %q: require an absolute HTTP(S) URL", address)
		}
		if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return fmt.Errorf("invalid node URL %q: credentials, paths, queries, and fragments are unsupported", address)
		}
	}
	if config.Requests < 1 || config.Concurrency < 1 {
		return fmt.Errorf("requests and concurrency must be positive")
	}
	if config.ReadPercent < 0 || config.ReadPercent > 100 {
		return fmt.Errorf("read percentage must be between 0 and 100")
	}
	if config.ReadKeys < 0 || (config.ReadPercent > 0 && config.ReadKeys == 0) {
		return fmt.Errorf("read keys must be positive for workloads containing reads")
	}
	if config.ValueBytes < 0 || config.ValueBytes > (1<<20)-12 {
		return fmt.Errorf("value bytes must be between 0 and %d to fit the JSON request limit", (1<<20)-12)
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("request timeout must be positive")
	}
	return nil
}
