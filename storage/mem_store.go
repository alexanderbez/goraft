package storage

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/alexanderbez/goraft/types"
)

// Assert that RaftStore implements the RaftDB interface at compile time.
var _ RaftDB = (*MemStore)(nil)

type (
	// MemStore implements an in-memory simple Raft stable storage and persistence
	// layer. It implements the RaftDB interface.
	MemStore struct {
		mtx sync.RWMutex

		lastLogIndex uint64 // log with the highest index

		logsBucket map[uint64]types.Log // persistence of append-only logs
		mainBucket map[string][]byte    // persistence of non-log data (e.g. cluster config and node metadata)
	}
)

// ----------------------------------------------------------------------------
// MemStore

func NewMemStore() *MemStore {
	return &MemStore{
		logsBucket: make(map[uint64]types.Log),
		mainBucket: make(map[string][]byte),
	}
}

// SetLogEncoder sets the store's log encoder type.
func (ms *MemStore) SetLogEncoder(_ types.LogEncoder) {}

// SetLogDecoder sets the store's log decoder type.
func (ms *MemStore) SetLogDecoder(_ types.LogDecoder) {}

// Get returns a value for a given key. If the key does not exist, an error is
// returned.
func (ms *MemStore) Get(key []byte) ([]byte, error) {
	ms.mtx.RLock()
	defer ms.mtx.RUnlock()

	value, ok := ms.mainBucket[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

// Set inserts a value for a given key. An error is returned upon failure. An
// error is returned upon failure.
func (ms *MemStore) Set(key, value []byte) error {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	ms.mainBucket[string(key)] = value
	return nil
}

// GetUint64 returns an unsigned 64-bit value for a given key. If the key does
// not exist, an error is returned.
func (ms *MemStore) GetUint64(key []byte) (uint64, error) {
	ms.mtx.RLock()
	defer ms.mtx.RUnlock()

	value, ok := ms.mainBucket[string(key)]
	if !ok {
		return 0, ErrKeyNotFound
	}

	return binary.LittleEndian.Uint64(value), nil
}

// SetUint64 inserts an unsigned 64-bit value for a given key. An error is
// returned upon failure.
func (ms *MemStore) SetUint64(key []byte, value uint64) error {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	ms.mainBucket[string(key)] = uint64ToBytes(value)
	return nil
}

// SetLogs inserts a batch of logs where each log should contain an incremented
// index. An error is returned if any log cannot be serialized or if insertion
// fails.
//
// Contract: No log should exist for that given index.
func (ms *MemStore) SetLogs(logs []types.Log) error {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	for _, log := range logs {
		if log.Index > ms.lastLogIndex {
			ms.lastLogIndex = log.Index
		}

		ms.logsBucket[log.Index] = log
	}

	return nil
}

// SetLog inserts a log by index. An error is returned if the log cannot be
// serialized or if insertion fails.
//
// Contract: No log should exist for that given index.
func (ms *MemStore) SetLog(log types.Log) error {
	return ms.SetLogs([]types.Log{log})
}

// GetLog returns a log for a given index. If no log exists for such an index
// or if it cannot be deserialized, an error is returned.
func (ms *MemStore) GetLog(index uint64) (types.Log, error) {
	ms.mtx.RLock()
	defer ms.mtx.RUnlock()

	log, ok := ms.logsBucket[index]
	if !ok {
		return log, ErrKeyNotFound
	}

	return log, nil
}

// GetLastLogIndex returns the last stored log index. If no logs have been
// stored, then zero is returned.
func (ms *MemStore) GetLastLogIndex() uint64 {
	ms.mtx.RLock()
	defer ms.mtx.RUnlock()

	return ms.lastLogIndex
}

// LogsIter iterates over a given range of logs by an index range. For each
// index, a cb function that accepts the given log is called. If the callback
// executed for any given log returns an error, the iteration exits along with
// the error being returned to the caller.
func (ms *MemStore) LogsIter(start, end uint64, cb func(log types.Log) error) error {
	ms.mtx.RLock()
	defer ms.mtx.RUnlock()

	if start > end {
		return fmt.Errorf("invalid range query: %d > %d", start, end)
	}

	for i := start; i <= end; i++ {
		log, ok := ms.logsBucket[i]
		if ok {
			if err := cb(log); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close performs a no-op on a MemStore.
func (ms *MemStore) Close() error { return nil }
