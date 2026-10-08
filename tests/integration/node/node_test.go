package node_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"kvstore/internal/storage"
)

type nodeProcess struct {
	command  *exec.Cmd
	exited   chan error
	address  string
	logPath  string
	finished bool
}

func TestNodeRecoversAfterGracefulShutdownAndCrash(t *testing.T) {
	binary := buildNode(t)
	for _, clustered := range []bool{false, true} {
		t.Run(fmt.Sprintf("clustered_%t", clustered), func(t *testing.T) {
			directory := t.TempDir()
			arguments := []string{"-addr", "127.0.0.1:0", "-data-dir", filepath.Join(directory, "data")}
			if clustered {
				configPath := filepath.Join(directory, "cluster.json")
				configuration := `{"members":[{"id":"node-a","address":"http://127.0.0.1:1"}],"virtual_nodes":8,"replication_factor":1}`
				if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
					t.Fatal(err)
				}
				arguments = append(arguments, "-config", configPath, "-id", "node-a")
			}
			node := startNode(t, binary, arguments)
			assertNodeHealth(t, node)
			requestNode(t, node, http.MethodPut, "/kv/kept", `{"value":"original"}`, http.StatusNoContent)
			requestNode(t, node, http.MethodPut, "/kv/deleted", `{"value":"remove me"}`, http.StatusNoContent)
			requestNode(t, node, http.MethodDelete, "/kv/deleted", "", http.StatusNoContent)
			var tombstone storage.Record
			if clustered {
				body := requestNode(t, node, http.MethodGet, "/internal/records/deleted", "", http.StatusOK)
				if err := json.Unmarshal(body, &tombstone); err != nil {
					t.Fatal(err)
				}
				if !tombstone.Deleted {
					t.Fatal("missing deletion marker")
				}
			}
			lockContext, cancelLock := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelLock()
			contender := exec.CommandContext(lockContext, binary, arguments...)
			if output, err := contender.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("lock storage log")) {
				t.Fatalf("second process was not rejected: %v, %s", err, output)
			}
			stopNode(t, node, false)
			node = startNode(t, binary, arguments)
			assertNodeHealth(t, node)
			assertNodeValue(t, node, "kept", "original")
			requestNode(t, node, http.MethodGet, "/kv/deleted", "", http.StatusNotFound)
			requestNode(t, node, http.MethodPut, "/kv/after-restart", `{"value":"survives crash"}`, http.StatusNoContent)
			if clustered {
				var record storage.Record
				body := requestNode(t, node, http.MethodGet, "/internal/records/after-restart", "", http.StatusOK)
				if err := json.Unmarshal(body, &record); err != nil {
					t.Fatal(err)
				}
				if record.Version.Counter <= tombstone.Version.Counter {
					t.Fatal("version counter reused after restart")
				}
			}
			stopNode(t, node, true)
			node = startNode(t, binary, arguments)
			assertNodeValue(t, node, "kept", "original")
			assertNodeValue(t, node, "after-restart", "survives crash")
			requestNode(t, node, http.MethodGet, "/kv/deleted", "", http.StatusNotFound)
			stopNode(t, node, false)
		})
	}
}

func TestNodeHealthDoesNotRequireQuorum(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "cluster.json")
	configuration := `{
		"members": [
			{"id":"node-a","address":"http://127.0.0.1:1"},
			{"id":"node-b","address":"http://127.0.0.1:1"},
			{"id":"node-c","address":"http://127.0.0.1:2"}
		],
		"virtual_nodes":8,"replication_factor":3,"read_quorum":2,"write_quorum":2
	}`
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	node := startNode(t, buildNode(t), []string{
		"-addr", "127.0.0.1:0", "-config", configPath, "-id", "node-a", "-peer-timeout", "100ms",
	})
	requestNode(t, node, http.MethodGet, "/kv/missing", "", http.StatusServiceUnavailable)
	waitMetrics(t, node,
		`kvstore_http_requests_total{scope="public",method="GET",status="503"} 1`,
		`kvstore_quorum_failures_total{phase="read"} 1`,
	)
	assertNodeHealth(t, node)
	requestNode(t, node, http.MethodGet, "/health/extra", "", http.StatusNotFound)
	requestNode(t, node, http.MethodPost, "/health", "", http.StatusMethodNotAllowed)
	stopNode(t, node, false)
}

func buildNode(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "node")
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/node")
	build.Dir = "../../.."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build node: %v\n%s", err, output)
	}
	return binary
}

func assertNodeHealth(t *testing.T, node *nodeProcess) {
	t.Helper()
	body := requestNode(t, node, http.MethodGet, "/health", "", http.StatusOK)
	if string(body) != `{"status":"ok"}` {
		t.Fatalf("health response: %s", body)
	}
	if body := requestNode(t, node, http.MethodHead, "/health", "", http.StatusOK); len(body) != 0 {
		t.Fatal("HEAD /health returned a body")
	}
}

func startNode(t *testing.T, binary string, arguments []string) *nodeProcess {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "node.log")
	output, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, arguments...)
	command.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0")
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		if closeError := output.Close(); closeError != nil {
			t.Error(closeError)
		}
		t.Fatal(err)
	}
	node := &nodeProcess{command: command, exited: make(chan error, 1), logPath: logPath}
	go func() { node.exited <- command.Wait() }()
	t.Cleanup(func() {
		if !node.finished {
			if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Error(err)
			}
			<-node.exited
		}
		if err := output.Close(); err != nil {
			t.Error(err)
		}
	})
	addressPattern := regexp.MustCompile(`address=(127\.0\.0\.1:\d+)`)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		if match := addressPattern.FindSubmatch(contents); len(match) == 2 {
			node.address = "http://" + string(match[1])
			return node
		}
		select {
		case err := <-node.exited:
			node.finished = true
			t.Fatalf("node exited before listening: %v\n%s", err, contents)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("node did not start listening")
	return nil
}

func stopNode(t *testing.T, node *nodeProcess, crash bool) {
	t.Helper()
	var err error
	if crash {
		err = node.command.Process.Kill()
	} else {
		err = node.command.Process.Signal(os.Interrupt)
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-node.exited:
		node.finished = true
		if !crash && err != nil {
			contents, readError := os.ReadFile(node.logPath)
			t.Fatalf("node shutdown: %v, log read: %v\n%s", err, readError, contents)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("node did not stop")
	}
}

func requestNode(t *testing.T, node *nodeProcess, method, path, body string, expected int) []byte {
	t.Helper()
	request, err := http.NewRequest(method, node.address+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(response.Body)
	if closeError := response.Body.Close(); closeError != nil {
		t.Error(closeError)
	}
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expected {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, response.StatusCode, expected, contents)
	}
	return contents
}

func assertNodeValue(t *testing.T, node *nodeProcess, key, expected string) {
	t.Helper()
	body := requestNode(t, node, http.MethodGet, "/kv/"+key, "", http.StatusOK)
	var response struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Value != expected {
		t.Fatalf("Get(%q)=%q, want %q", key, response.Value, expected)
	}
}
