package benchmark_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kvstore/internal/benchmark"
)

func TestBenchmarkCLIReportsResultsAndExitStatus(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "bench")
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/bench")
	build.Dir = "../../.."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build benchmark: %v\n%s", err, output)
	}
	for _, status := range []int{http.StatusNoContent, http.StatusServiceUnavailable} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		command := exec.CommandContext(ctx, binary, "-nodes", server.URL, "-requests", "3", "-read-percent", "0", "-format", "json")
		var output, diagnostics bytes.Buffer
		command.Stdout, command.Stderr = &output, &diagnostics
		err := command.Run()
		cancel()
		if (err == nil) != (status == http.StatusNoContent) {
			t.Fatalf("unexpected exit for status %d: %v, %s", status, err, diagnostics.String())
		}
		var report benchmark.Report
		if err := json.Unmarshal(output.Bytes(), &report); err != nil {
			t.Fatalf("stdout is not a JSON report: %v\n%s", err, output.String())
		}
		if !report.Completed || report.Measurements.Total.Attempted != 3 || report.Preparation != nil {
			t.Fatalf("unexpected CLI report: %+v", report)
		}
	}
	for _, arguments := range [][]string{{"-h"}, {"-format", "csv"}, {"-requests", "0"}, {"unexpected"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		command := exec.CommandContext(ctx, binary, arguments...)
		output, err := command.CombinedOutput()
		cancel()
		if arguments[0] == "-h" {
			if err != nil || !strings.Contains(string(output), "read-percent") {
				t.Fatalf("help failed: %v\n%s", err, output)
			}
		} else if err == nil {
			t.Fatalf("invalid arguments accepted: %v", arguments)
		}
	}
}
