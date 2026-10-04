package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kvstore/internal/api"
	"kvstore/internal/storage"
)

type replicaStoreSpy struct {
	record  storage.Record
	found   bool
	applies int
	err     error
}

func (store *replicaStoreSpy) GetRecord(string) (storage.Record, bool) {
	return store.record, store.found
}
func (store *replicaStoreSpy) Apply(record storage.Record) (bool, error) {
	store.applies++
	if store.err != nil {
		return false, store.err
	}
	store.record, store.found = record, true
	return true, nil
}

func TestReplicaHandlerAppliesUnchangedRecord(t *testing.T) {
	store := &replicaStoreSpy{}
	record := storage.Record{Key: "key", Deleted: true, Version: storage.Version{Counter: 900, NodeID: "coordinator"}}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/internal/records/key", strings.NewReader(string(payload)))
	request.Header.Set("Content-Type", "application/json")
	response := performRequest(api.NewReplicaHandler(store), request)
	assertStatus(t, response, http.StatusNoContent)
	if store.applies != 1 || store.record != record {
		t.Fatalf("record changed: %+v", store.record)
	}
	response = performRequest(api.NewReplicaHandler(store), httptest.NewRequest(http.MethodGet, "/internal/records/key", nil))
	assertStatus(t, response, http.StatusOK)
	var result storage.Record
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result != record {
		t.Fatal("GET hid or changed tombstone")
	}
}

func TestReplicaHandlerRejectsInvalidRecordsBeforeStorage(t *testing.T) {
	const valid = `{"key":"key","value":"value","version":{"counter":1,"node_id":"a"},"deleted":false}`
	cases := []struct {
		name, body, contentType string
		status                  int
	}{
		{"content type", valid, "text/plain", 415},
		{"wrong key", strings.Replace(valid, `"key":"key"`, `"key":"other"`, 1), "application/json", 400},
		{"missing version", `{"key":"key","value":"value","deleted":false}`, "application/json", 400},
		{"null version", `{"key":"key","value":"value","version":null,"deleted":false}`, "application/json", 400},
		{"missing value", `{"key":"key","version":{"counter":1,"node_id":"a"},"deleted":false}`, "application/json", 400},
		{"missing deletion flag", `{"key":"key","value":"value","version":{"counter":1,"node_id":"a"}}`, "application/json", 400},
		{"null deletion flag", strings.Replace(valid, "false", "null", 1), "application/json", 400},
		{"invalid tombstone", strings.Replace(valid, "false", "true", 1), "application/json", 400},
		{"zero version", strings.Replace(valid, `"counter":1`, `"counter":0`, 1), "application/json", 400},
		{"unknown field", strings.TrimSuffix(valid, "}") + `,"unknown":1}`, "application/json", 400},
		{"trailing JSON", valid + `{}`, "application/json", 400},
		{"malformed", `{`, "application/json", 400},
		{"value limit", strings.Replace(valid, `"value":"value"`, `"value":"`+strings.Repeat("x", (1<<20)+1)+`"`, 1), "application/json", 400},
		{"body limit", valid + strings.Repeat(" ", 8<<20), "application/json", 413},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			store := &replicaStoreSpy{}
			request := httptest.NewRequest(http.MethodPut, "/internal/records/key", strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", testCase.contentType)
			response := performRequest(api.NewReplicaHandler(store), request)
			assertStatus(t, response, testCase.status)
			if store.applies != 0 {
				t.Fatal("invalid request changed storage")
			}
		})
	}
}

func TestReplicaHandlerMissingAndConflictingRecords(t *testing.T) {
	store := &replicaStoreSpy{}
	response := performRequest(api.NewReplicaHandler(store), httptest.NewRequest(http.MethodGet, "/internal/records/missing", nil))
	assertStatus(t, response, http.StatusNotFound)
	for _, failure := range []error{storage.ErrStaleRecord, storage.ErrVersionConflict} {
		store.err = failure
		request := httptest.NewRequest(http.MethodPut, "/internal/records/key", strings.NewReader(`{"key":"key","value":"","version":{"counter":1,"node_id":"a"},"deleted":true}`))
		request.Header.Set("Content-Type", "application/json")
		response := performRequest(api.NewReplicaHandler(store), request)
		assertStatus(t, response, http.StatusConflict)
	}
}
