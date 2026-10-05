package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	logFormat       = 1
	frameHeaderSize = 12
	maxFrameSize    = 16 << 20
)

var errIncompleteFrame = errors.New("incomplete final log frame")

type logFrame struct {
	Format uint32
	NodeID string
	Entry  *LogEntry
}

type fileLog struct {
	file   *os.File
	nodeID string
}

func openFileLog(directory, nodeID string) (*fileLog, error) {
	if directory == "" {
		return nil, errors.New("data directory must not be empty")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(directory, "store.log"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	journal := &fileLog{file: file, nodeID: nodeID}
	if err := journal.lock(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	for _, path := range []string{directory, filepath.Dir(filepath.Clean(directory))} {
		if err := syncDirectory(path); err != nil {
			return nil, errors.Join(err, journal.Close())
		}
	}
	return journal, nil
}

func (journal *fileLog) lock() error {
	info, err := journal.file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("storage log must be a regular file")
	}
	if err := syscall.Flock(int(journal.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lock storage log (another node may be using it): %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (journal *fileLog) Replay(visit func(LogEntry) error) error {
	if _, err := journal.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header, err := journal.readNext()
	if err == io.EOF {
		return journal.appendFrame(logFrame{Format: logFormat, NodeID: journal.nodeID})
	}
	if err != nil {
		return err
	}
	if header.Format != logFormat || header.NodeID != journal.nodeID || header.Entry != nil {
		return errors.New("log format or node identity does not match")
	}
	for {
		frame, err := journal.readNext()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if frame.Format != 0 || frame.NodeID != "" || frame.Entry == nil {
			return errors.New("unexpected log metadata in record stream")
		}
		if err := visit(*frame.Entry); err != nil {
			return fmt.Errorf("invalid log entry: %w", err)
		}
	}
}

func (journal *fileLog) readNext() (logFrame, error) {
	offset, err := journal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return logFrame{}, err
	}
	frame, err := readFrame(journal.file)
	if errors.Is(err, errIncompleteFrame) {
		if err := journal.discardTail(offset); err != nil {
			return logFrame{}, fmt.Errorf("discard incomplete log tail: %w", err)
		}
		return logFrame{}, io.EOF
	}
	if err != nil && err != io.EOF {
		return logFrame{}, fmt.Errorf("read log at byte %d: %w", offset, err)
	}
	return frame, err
}

func (journal *fileLog) discardTail(offset int64) error {
	if err := journal.file.Truncate(offset); err != nil {
		return err
	}
	if err := journal.file.Sync(); err != nil {
		return err
	}
	_, err := journal.file.Seek(offset, io.SeekStart)
	return err
}

func readFrame(reader io.Reader) (logFrame, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return logFrame{}, errIncompleteFrame
		}
		return logFrame{}, err
	}
	// Protect the length too, so a corrupt length cannot masquerade as a torn tail.
	if crc32.ChecksumIEEE(header[:8]) != binary.BigEndian.Uint32(header[8:]) {
		return logFrame{}, errors.New("log header checksum mismatch")
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length == 0 || length > maxFrameSize {
		return logFrame{}, errors.New("invalid log frame length")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return logFrame{}, errIncompleteFrame
		}
		return logFrame{}, err
	}
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:8]) {
		return logFrame{}, errors.New("log payload checksum mismatch")
	}
	decoder := gob.NewDecoder(bytes.NewReader(payload))
	var frame logFrame
	if err := decoder.Decode(&frame); err != nil {
		return logFrame{}, fmt.Errorf("decode log frame: %w", err)
	}
	var extra logFrame
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return logFrame{}, errors.New("unexpected data after log frame")
	}
	return frame, nil
}

func (journal *fileLog) Append(entry LogEntry) error {
	return journal.appendFrame(logFrame{Entry: &entry})
}

func (journal *fileLog) appendFrame(frame logFrame) error {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(frame); err != nil {
		return fmt.Errorf("encode log frame: %w", err)
	}
	if payload.Len() > maxFrameSize {
		return errors.New("log frame exceeds 16 MiB")
	}
	frameBytes := make([]byte, frameHeaderSize+payload.Len())
	binary.BigEndian.PutUint32(frameBytes[:4], uint32(payload.Len()))
	binary.BigEndian.PutUint32(frameBytes[4:8], crc32.ChecksumIEEE(payload.Bytes()))
	binary.BigEndian.PutUint32(frameBytes[8:12], crc32.ChecksumIEEE(frameBytes[:8]))
	copy(frameBytes[frameHeaderSize:], payload.Bytes())
	if written, err := journal.file.Write(frameBytes); err != nil {
		return fmt.Errorf("append log: %w", err)
	} else if written != len(frameBytes) {
		return io.ErrShortWrite
	}
	if err := journal.file.Sync(); err != nil {
		return fmt.Errorf("sync log: %w", err)
	}
	return nil
}

func (journal *fileLog) Close() error {
	// Closing the descriptor also releases its advisory lock, including on crashes.
	return journal.file.Close()
}
