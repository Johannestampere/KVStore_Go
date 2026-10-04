package replication

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"kvstore/internal/storage"
)

// DecodeRecord reads exactly one complete record from the peer protocol.
func DecodeRecord(reader io.Reader) (storage.Record, error) {
	var payload struct {
		Key     *string          `json:"key"`
		Value   *string          `json:"value"`
		Version *storage.Version `json:"version"`
		Deleted *bool            `json:"deleted"`
	}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return storage.Record{}, fmt.Errorf("decode record: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return storage.Record{}, fmt.Errorf("read end of record: %w", err)
		}
		return storage.Record{}, errors.New("expected one record")
	}
	if payload.Key == nil || payload.Value == nil || payload.Version == nil || payload.Deleted == nil {
		return storage.Record{}, errors.New("record requires key, value, version, and deleted fields")
	}
	record := storage.Record{Key: *payload.Key, Value: *payload.Value, Version: *payload.Version, Deleted: *payload.Deleted}
	if len(record.Value) > 1<<20 {
		return storage.Record{}, errors.New("record value exceeds 1 MiB")
	}
	if err := record.Validate(); err != nil {
		return storage.Record{}, err
	}
	return record, nil
}
