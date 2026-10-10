package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kvstore/internal/benchmark"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, output, diagnostics io.Writer) error {
	config, format, err := parseOptions(arguments, diagnostics)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	runner, err := benchmark.NewRunner(config)
	if err != nil {
		return err
	}
	report, runError := runner.Run(ctx)
	if err := writeReport(output, format, report); err != nil {
		return errors.Join(runError, fmt.Errorf("write benchmark report: %w", err))
	}
	if runError != nil {
		return runError
	}
	if report.Measurements.Total.Failed > 0 {
		return fmt.Errorf("%d benchmark requests failed", report.Measurements.Total.Failed)
	}
	return nil
}

func parseOptions(arguments []string, diagnostics io.Writer) (benchmark.Config, string, error) {
	var config benchmark.Config
	flags := flag.NewFlagSet("bench", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	nodes := flags.String("nodes", "http://127.0.0.1:8001", "Comma-separated entry node URLs")
	format := flags.String("format", "text", "Report format: text or json")
	flags.IntVar(&config.Requests, "requests", 10000, "Number of measured requests")
	flags.IntVar(&config.Concurrency, "concurrency", 10, "Maximum concurrent requests")
	flags.IntVar(&config.ReadPercent, "read-percent", 50, "Percentage of GETs; remaining requests are PUTs")
	flags.IntVar(&config.ReadKeys, "keys", 1000, "Number of keys preloaded for reads")
	flags.IntVar(&config.ValueBytes, "value-bytes", 256, "ASCII value size in bytes")
	flags.DurationVar(&config.Timeout, "timeout", 5*time.Second, "Per-request deadline, including response body")
	flags.StringVar(&config.Description, "description", "", "Deployment, hardware, and experiment notes")
	if err := flags.Parse(arguments); err != nil {
		return config, "", err
	}
	if flags.NArg() != 0 {
		return config, "", fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *format != "text" && *format != "json" {
		return config, "", fmt.Errorf("format must be text or json")
	}
	config.Nodes = strings.Split(*nodes, ",")
	for index := range config.Nodes {
		config.Nodes[index] = strings.TrimSpace(config.Nodes[index])
	}
	return config, *format, nil
}

func writeReport(output io.Writer, format string, report benchmark.Report) error {
	if format == "json" {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	var text strings.Builder
	fmt.Fprintf(&text, "Closed-loop benchmark: %s\n", report.KeyPrefix)
	fmt.Fprintf(&text, "Nodes: %s\n", strings.Join(report.Config.Nodes, ", "))
	fmt.Fprintf(&text, "Requests: %d; concurrency: %d; reads: %d%%; value: %d bytes; timeout: %s\n",
		report.Config.Requests, report.Config.Concurrency, report.Config.ReadPercent, report.Config.ValueBytes, report.Config.Timeout)
	fmt.Fprintf(&text, "Runtime: %s %s\nDescription: %s\n", report.GoVersion, report.Platform, report.Config.Description)
	if report.Preparation != nil {
		fmt.Fprintf(&text, "Preload: %d succeeded, %d failed (excluded from measurements)\n",
			report.Preparation.Total.Succeeded, report.Preparation.Total.Failed)
	}
	fmt.Fprintf(&text, "Completed: %t; measured wall time: %.3fs\n", report.Completed, report.Measurements.ElapsedSeconds)
	writeSummary(&text, "TOTAL", report.Measurements.Total)
	for _, method := range []string{"GET", "PUT"} {
		if summary, exists := report.Measurements.Methods[method]; exists {
			writeSummary(&text, method, summary)
		}
	}
	_, err := io.WriteString(output, text.String())
	return err
}

func writeSummary(output *strings.Builder, name string, summary benchmark.Summary) {
	fmt.Fprintf(output, "%s: %d attempted, %d succeeded, %d failed (%.2f%%); %.2f successful ops/s\n",
		name, summary.Attempted, summary.Succeeded, summary.Failed, summary.ErrorRate*100, summary.SuccessfulPerSecond)
	for _, distribution := range []struct {
		name   string
		values *benchmark.Percentiles
	}{{"all attempts", summary.LatencyMS}, {"successful", summary.SuccessfulLatencyMS}} {
		if distribution.values != nil {
			fmt.Fprintf(output, "  %s latency ms: p50=%.3f p95=%.3f p99=%.3f\n", distribution.name,
				distribution.values.P50, distribution.values.P95, distribution.values.P99)
		}
	}
	fmt.Fprintf(output, "  statuses=%v failures=%v\n", summary.StatusCodes, summary.Failures)
}
