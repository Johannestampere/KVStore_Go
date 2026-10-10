package benchmark_test

import (
	"testing"
	"time"

	"kvstore/internal/benchmark"
)

func validConfig() benchmark.Config {
	return benchmark.Config{
		Nodes: []string{"http://127.0.0.1:8001"}, Requests: 100, Concurrency: 4,
		ReadPercent: 50, ReadKeys: 10, ValueBytes: 256, Timeout: time.Second,
	}
}

func TestConfigRejectsInvalidWorkloads(t *testing.T) {
	cases := map[string]func(*benchmark.Config){
		"no nodes":           func(c *benchmark.Config) { c.Nodes = nil },
		"relative URL":       func(c *benchmark.Config) { c.Nodes = []string{"localhost:8001"} },
		"unsupported scheme": func(c *benchmark.Config) { c.Nodes = []string{"ftp://localhost"} },
		"credentials":        func(c *benchmark.Config) { c.Nodes = []string{"http://user:secret@localhost"} },
		"path":               func(c *benchmark.Config) { c.Nodes = []string{"http://localhost/kv"} },
		"query":              func(c *benchmark.Config) { c.Nodes = []string{"http://localhost?x=1"} },
		"fragment":           func(c *benchmark.Config) { c.Nodes = []string{"http://localhost/#x"} },
		"zero requests":      func(c *benchmark.Config) { c.Requests = 0 },
		"zero concurrency":   func(c *benchmark.Config) { c.Concurrency = 0 },
		"negative reads":     func(c *benchmark.Config) { c.ReadPercent = -1 },
		"excess reads":       func(c *benchmark.Config) { c.ReadPercent = 101 },
		"missing read keys":  func(c *benchmark.Config) { c.ReadKeys = 0 },
		"negative keys":      func(c *benchmark.Config) { c.ReadKeys = -1 },
		"negative value":     func(c *benchmark.Config) { c.ValueBytes = -1 },
		"oversized value":    func(c *benchmark.Config) { c.ValueBytes = (1 << 20) - 11 },
		"no deadline":        func(c *benchmark.Config) { c.Timeout = 0 },
	}
	for name, modify := range cases {
		t.Run(name, func(t *testing.T) {
			config := validConfig()
			modify(&config)
			if _, err := benchmark.NewRunner(config); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestConfigAllowsEmptyValuesAndWriteOnlyWorkloads(t *testing.T) {
	config := validConfig()
	config.Nodes = []string{"http://localhost:8001/", "https://localhost:8002"}
	config.ReadPercent, config.ReadKeys, config.ValueBytes = 0, 0, 0
	if _, err := benchmark.NewRunner(config); err != nil {
		t.Fatal(err)
	}
}
