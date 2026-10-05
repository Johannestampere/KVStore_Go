package storage

// LogEntry preserves a counter reservation and, optionally, a record update.
type LogEntry struct {
	Counter uint64
	Record  *Record
}

// Journal is an exclusively owned, ordered log bound to one node identity.
// Append must synchronize an entry before returning success. An error may
// leave an entry on disk; callers must stop appending until recovery.
type Journal interface {
	Replay(visit func(LogEntry) error) error
	Append(LogEntry) error
	Close() error
}
