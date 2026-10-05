package storage_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"kvstore/internal/storage"
)

func openStore(t *testing.T, directory string) *storage.PersistentStore {
	t.Helper()
	store, err := storage.OpenPersistentStore(directory, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func closeStore(t *testing.T, store *storage.PersistentStore) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogRestoresValuesTombstonesAndUnstoredReservations(t *testing.T) {
	directory := t.TempDir()
	store := openStore(t, directory)
	values := map[string]string{"": "", "unicode/日本": "hello 🌍", "binary\xff": "\x00\xff\xfe"}
	for key, value := range values {
		if err := store.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Put("deleted", "old"); err != nil {
		t.Fatal(err)
	}
	old, _ := store.GetRecord("deleted")
	if err := store.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	remote := storage.Record{Key: "remote", Value: "other", Version: storage.Version{Counter: 50, NodeID: "node-b"}}
	if _, err := store.Apply(remote); err != nil {
		t.Fatal(err)
	}
	reserved, err := store.NextVersion(storage.Version{Counter: 1000})
	if err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)

	recovered := openStore(t, directory)
	for key, expected := range values {
		if value, found := recovered.Get(key); !found || value != expected {
			t.Fatalf("Get(%q)=%q,%t", key, value, found)
		}
	}
	if record, found := recovered.GetRecord("remote"); !found || record != remote {
		t.Fatalf("remote record: %+v, %t", record, found)
	}
	if _, found := recovered.Get("deleted"); found {
		t.Fatal("deleted value resurrected")
	}
	if _, err := recovered.Apply(old); !errors.Is(err, storage.ErrStaleRecord) {
		t.Fatalf("stale resurrection: %v", err)
	}
	next, err := recovered.NextVersion(storage.Version{})
	if err != nil || next.Counter != reserved.Counter+1 {
		t.Fatalf("reservation: %+v, %v", next, err)
	}
	if err := recovered.Put("after", "restart"); err != nil {
		t.Fatal(err)
	}
	closeStore(t, recovered)
	again := openStore(t, directory)
	if value, found := again.Get("after"); !found || value != "restart" {
		t.Fatalf("post-recovery append: %q,%t", value, found)
	}
}

func TestLogExclusivityAndIdentity(t *testing.T) {
	directory := t.TempDir()
	first := openStore(t, directory)
	if second, err := storage.OpenPersistentStore(directory, "node-a"); err == nil {
		second.Close()
		t.Fatal("two writers acquired the log")
	}
	closeStore(t, first)
	if wrong, err := storage.OpenPersistentStore(directory, "node-b"); err == nil {
		wrong.Close()
		t.Fatal("opened another node's log")
	}
	openStore(t, directory)
	if _, err := storage.OpenPersistentStore("", "node-a"); err == nil {
		t.Fatal("accepted empty data directory")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenPersistentStore(file, "node-a"); err == nil {
		t.Fatal("accepted file as directory")
	}
}

func completedLog(t *testing.T) ([]byte, int) {
	t.Helper()
	directory := t.TempDir()
	store := openStore(t, directory)
	if err := store.Put("kept", "first"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, "store.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("tail", "last"); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)
	contents, err := os.ReadFile(filepath.Join(directory, "store.log"))
	if err != nil {
		t.Fatal(err)
	}
	return contents, int(info.Size())
}

func TestLogDiscardsIncompleteFinalFrameAndAppendsAfterRecovery(t *testing.T) {
	contents, boundary := completedLog(t)
	for _, remaining := range []int{1, 7, 11, 12, 13, (len(contents) - boundary) / 2, len(contents) - boundary - 1} {
		t.Run(strconv.Itoa(remaining), func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "store.log")
			if err := os.WriteFile(path, contents[:boundary+remaining], 0o600); err != nil {
				t.Fatal(err)
			}
			store := openStore(t, directory)
			if value, found := store.Get("kept"); !found || value != "first" {
				t.Fatal("lost complete entry")
			}
			if _, found := store.Get("tail"); found {
				t.Fatal("replayed incomplete entry")
			}
			info, err := os.Stat(path)
			if err != nil || info.Size() != int64(boundary) {
				t.Fatalf("tail not removed: %v, %v", info, err)
			}
			if err := store.Put("replacement", "value"); err != nil {
				t.Fatal(err)
			}
			closeStore(t, store)
			recovered := openStore(t, directory)
			if value, found := recovered.Get("replacement"); !found || value != "value" {
				t.Fatal("append after recovery lost")
			}
		})
	}
}

func TestLogRejectsCompleteCorruptionWithoutTruncating(t *testing.T) {
	contents, last := completedLog(t)
	firstEntry := 12 + int(binary.BigEndian.Uint32(contents[:4]))
	for _, position := range []int{0, 8, firstEntry, firstEntry + 12, last, last + 12, len(contents) - 1} {
		t.Run(strconv.Itoa(position), func(t *testing.T) {
			corrupt := bytes.Clone(contents)
			corrupt[position] ^= 0x80
			directory := t.TempDir()
			path := filepath.Join(directory, "store.log")
			if err := os.WriteFile(path, corrupt, 0o600); err != nil {
				t.Fatal(err)
			}
			if store, err := storage.OpenPersistentStore(directory, "node-a"); err == nil {
				store.Close()
				t.Fatal("accepted corrupt log")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, corrupt) {
				t.Fatalf("corrupt log changed: %v", err)
			}
		})
	}
}

func TestLogRecoversInterruptedInitialization(t *testing.T) {
	contents, _ := completedLog(t)
	headerLength := 12 + int(binary.BigEndian.Uint32(contents[:4]))
	for _, size := range []int{0, 1, 11, 12, headerLength - 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "store.log"), contents[:size], 0o600); err != nil {
				t.Fatal(err)
			}
			store := openStore(t, directory)
			if err := store.Put("key", "value"); err != nil {
				t.Fatal(err)
			}
			closeStore(t, store)
			if value, found := openStore(t, directory).Get("key"); !found || value != "value" {
				t.Fatal("initialization recovery failed")
			}
		})
	}
}

func TestLogRejectsUndecodableCompleteFrame(t *testing.T) {
	contents, boundary := completedLog(t)
	// A valid checksum must not make an undecodable frame look like clean EOF.
	frame := make([]byte, 13)
	binary.BigEndian.PutUint32(frame[:4], 1)
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(frame[12:]))
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	contents = append(contents[:boundary], frame...)
	directory := t.TempDir()
	path := filepath.Join(directory, "store.log")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if store, err := storage.OpenPersistentStore(directory, "node-a"); err == nil {
		closeStore(t, store)
		t.Fatal("treated a malformed complete entry as end of log")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, contents) {
		t.Fatalf("malformed log changed: %v", err)
	}
}

func TestLogConcurrentWritesSurviveReopening(t *testing.T) {
	directory := t.TempDir()
	store := openStore(t, directory)
	var workers sync.WaitGroup
	for index := range 25 {
		workers.Go(func() {
			if err := store.Put(strconv.Itoa(index), "value"); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	closeStore(t, store)
	recovered := openStore(t, directory)
	for index := range 25 {
		if value, found := recovered.Get(strconv.Itoa(index)); !found || value != "value" {
			t.Errorf("missing key %d", index)
		}
	}
}
