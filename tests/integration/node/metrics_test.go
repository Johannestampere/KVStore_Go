package node_test

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNodeMetricsSeparateTrafficAndResetAfterRestart(t *testing.T) {
	binary := buildNode(t)
	for _, clustered := range []bool{false, true} {
		t.Run(fmt.Sprintf("clustered_%t", clustered), func(t *testing.T) {
			directory := t.TempDir()
			arguments := []string{"-addr", "127.0.0.1:0", "-data-dir", filepath.Join(directory, "data")}
			if clustered {
				path := filepath.Join(directory, "cluster.json")
				configuration := `{"members":[{"id":"node-a","address":"http://127.0.0.1:1"}],"virtual_nodes":8,"replication_factor":1}`
				if err := os.WriteFile(path, []byte(configuration), 0o600); err != nil {
					t.Fatal(err)
				}
				arguments = append(arguments, "-config", path, "-id", "node-a")
			}
			node := startNode(t, binary, arguments)
			requestNode(t, node, "GET", "/kv/private-key", "", 404)
			requestNode(t, node, "PUT", "/kv/private-key", `{"value":"private-value"}`, 204)
			requestNode(t, node, "GET", "/kv/private-key", "", 200)
			requestNode(t, node, "PUT", "/kv/invalid", `{}`, 400)
			if clustered {
				requestNode(t, node, "GET", "/internal/records/private-key", "", 200)
			}
			requestNode(t, node, "DELETE", "/kv/private-key", "", 204)
			expected := []string{
				`kvstore_http_requests_total{scope="public",method="GET",status="404"} 1`,
				`kvstore_http_requests_total{scope="public",method="GET",status="200"} 1`,
				`kvstore_http_requests_total{scope="public",method="PUT",status="204"} 1`,
				`kvstore_http_requests_total{scope="public",method="PUT",status="400"} 1`,
				`kvstore_http_requests_total{scope="public",method="DELETE",status="204"} 1`,
				`kvstore_http_request_duration_seconds_count{scope="public",method="GET"} 2`,
			}
			if clustered {
				expected = append(expected, `kvstore_http_requests_total{scope="internal",method="GET",status="200"} 1`)
			}
			snapshot := waitMetrics(t, node, expected...)
			if strings.Contains(snapshot, "private-key") || strings.Contains(snapshot, "private-value") {
				t.Fatal("metrics contain request data")
			}
			if !clustered && strings.Contains(snapshot, `scope="internal"`) {
				t.Fatal("standalone node recorded replica traffic")
			}
			assertNodeHealth(t, node)
			requestNode(t, node, "POST", "/metrics", "", http.StatusMethodNotAllowed)
			if body := requestNode(t, node, "HEAD", "/metrics", "", 200); len(body) != 0 {
				t.Fatal("HEAD metrics has body")
			}
			if again := string(requestNode(t, node, "GET", "/metrics", "", 200)); again != snapshot {
				t.Fatal("probes or scrapes changed metrics")
			}
			stopNode(t, node, false)
			node = startNode(t, binary, arguments)
			if snapshot := string(requestNode(t, node, "GET", "/metrics", "", 200)); strings.Contains(snapshot, "kvstore_http_requests_total{") {
				t.Fatal("request counts persisted across restart")
			}
			stopNode(t, node, false)
		})
	}
}

func waitMetrics(t *testing.T, node *nodeProcess, expected ...string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var snapshot string
	for time.Now().Before(deadline) {
		snapshot = string(requestNode(t, node, "GET", "/metrics", "", 200))
		matches := true
		for _, sample := range expected {
			if !strings.Contains("\n"+snapshot, "\n"+sample+"\n") {
				matches = false
				break
			}
		}
		if matches {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("metrics did not contain %v\n%s", expected, snapshot)
	return ""
}
