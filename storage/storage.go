package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/alexanderbez/goraft/types"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// Assert that RaftStore implements the RaftDB interface at compile time.
var _ RaftDB = (*RaftStore)(nil)

// ErrKeyNotFound defines an error returned when a given key does not exist.
var ErrKeyNotFound = errors.New("key does not exist")

type (
	// DB defines an embedded key/value store database interface.
	DB interface {
		Get(key []byte) ([]byte, error)
		Set(key, value []byte) error
		GetUint64(key []byte) (uint64, error)
		SetUint64(key []byte, value uint64) error
		Close() error
	}

	// RaftDB defines a Raft stable storage interface that any persistance layer
	// used in Raft consensus must implement.
	RaftDB interface {
		DB

		SetLogEncoder(enc types.LogEncoder)
		SetLogDecoder(dec types.LogDecoder)
		SetLogs(logs []types.Log) error
		SetLog(log types.Log) error
		GetLog(index uint64) (types.Log, error)
		GetLastLogIndex() uint64
		LogsIter(start, end uint64, cb func(log types.Log) error) error
		// RemoveLogs(lastIndex uint64)
	}

	// RaftStore implements the default Raft stable storage and persistance layer.
	// It uses BoltDB as the underlying embedded database and implements the
	// RaftDB interface.
	//
	// Contract: RaftStore should never persist nil values.
	RaftStore struct {
		db *bolt.DB

		logEncoder types.LogEncoder
		logDecoder types.LogDecoder

		logsBucket []byte // persistance of append-only logs
		mainBucket []byte // persistance of non-log data (e.g. cluster config and node metadata)
	}

	// RaftStoreOpts defines a type alias for BoltDB connection and configuration
	// options.
	RaftStoreOpts bolt.Options
)

// ----------------------------------------------------------------------------
// RaftStore

// NewRaftStore returns a reference to a new RaftStore with a given set of
// options. It will handle creating a BoltDB connection with the provided
// options. In addition, during write-mode, log and configuration buckets will
// be created if they do not already exist. An error will be returned if any
// required options are missing, the BoltDB connection cannot be established, or
// if bucket creation fails.
func NewRaftStore(dbPath string, opts RaftStoreOpts) (*RaftStore, error) {
	if len(dbPath) == 0 {
		return nil, errors.New("database path cannot be empty")
	}

	db, err := bolt.Open(dbPath, 0600, opts.toBoltOptions())
	if err != nil {
		return nil, err
	}

	rs := &RaftStore{
		db:         db,
		logEncoder: types.JSONLogEncoder{},
		logDecoder: types.JSONLogDecoder{},
		logsBucket: []byte("logs"),
		mainBucket: []byte("main"),
	}

	if !opts.ReadOnly {
		if err := rs.createBuckets(); err != nil {
			return nil, err
		}
	}

	return rs, nil
}

// SetLogEncoder sets the store's log encoder type.
func (rs *RaftStore) SetLogEncoder(enc types.LogEncoder) {
	rs.logEncoder = enc
}

// SetLogDecoder sets the store's log decoder type.
func (rs *RaftStore) SetLogDecoder(dec types.LogDecoder) {
	rs.logDecoder = dec
}

// Get returns a value for a given key. If the key does not exist, an error is
// returned.
func (rs *RaftStore) Get(key []byte) ([]byte, error) {
	return rs.get(rs.mainBucket, key)
}

// Set inserts a value for a given key. An error is returned upon failure.
func (rs *RaftStore) Set(key, value []byte) error {
	return rs.set(rs.mainBucket, key, value)
}

// GetUint64 returns an unsigned 64-bit value for a given key. If the key does
// not exist, an error is returned.
func (rs *RaftStore) GetUint64(key []byte) (uint64, error) {
	rawValue, err := rs.get(rs.mainBucket, key)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(rawValue), nil
}

// SetUint64 inserts an unsigned 64-bit value for a given key. An error is
// returned upon failure.
func (rs *RaftStore) SetUint64(key []byte, value uint64) error {
	return rs.set(rs.mainBucket, key, uint64ToBytes(value))
}

// SetLogs inserts a batch of logs where each log should contain an incremented
// index. An error is returned if any log cannot be serialized or if insertion
// fails.
//
// Contract: No log should exist for that given index.
func (rs *RaftStore) SetLogs(logs []types.Log) error {
	return rs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(rs.logsBucket)

		for _, log := range logs {
			rawLog, err := rs.logEncoder.Encode(log)
			if err != nil {
				return errors.Wrap(err, "failed to encode log")
			}

			if err := b.Put(uint64ToBytes(log.Index), rawLog); err != nil {
				return err
			}
		}

		return nil
	})
}

// SetLog inserts a log by index. An error is returned if the log cannot be
// serialized or if insertion fails.
//
// Contract: No log should exist for that given index.
func (rs *RaftStore) SetLog(log types.Log) error {
	rawLog, err := rs.logEncoder.Encode(log)
	if err != nil {
		return errors.Wrap(err, "failed to encode log")
	}

	return rs.set(rs.logsBucket, uint64ToBytes(log.Index), rawLog)
}

// GetLog returns a log for a given index. If no logs exists for such an index
// or if it cannot be deserialized, an error is returned.
func (rs *RaftStore) GetLog(index uint64) (types.Log, error) {
	var log types.Log

	rawLog, err := rs.get(rs.logsBucket, uint64ToBytes(index))
	if err != nil {

	}

	if err := rs.logDecoder.Decode(rawLog, &log); err != nil {
		return log, errors.Wrap(err, "failed to decode log")
	}

	return log, nil
}

// GetLastLogIndex returns the last stored log index. If no logs have been
// stored, then zero is returned.
func (rs *RaftStore) GetLastLogIndex() uint64 {
	rawValue := make([]byte, 8)

	rs.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(rs.logsBucket).Cursor()

		key, _ := cursor.Last()
		if key != nil {
			copy(rawValue, key)
		}

		return nil
	})

	return binary.LittleEndian.Uint64(rawValue)
}

// LogsIter iterates over a given range of logs by an index range. For each
// index, a cb function that accepts the given log is called. If the callback
// executed for any given log returns an error, the iteration exits along with
// the error being returned to the caller.
func (rs *RaftStore) LogsIter(start, end uint64, cb func(log types.Log) error) error {
	if start > end {
		return fmt.Errorf("invalid range query: %d > %d", start, end)
	}

	return rs.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(rs.logsBucket).Cursor()

		min := uint64ToBytes(start)
		max := uint64ToBytes(end)

		for k, v := cursor.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = cursor.Next() {
			var log types.Log
			if err := rs.logDecoder.Decode(v, &log); err != nil {
				return errors.Wrap(err, "failed to decode log")
			}

			if err := cb(log); err != nil {
				return err
			}
		}

		return nil
	})
}

// Close releases all database resources. It will block waiting for any open
// transactions to finish before closing the database and returning.
func (rs *RaftStore) Close() error {
	return rs.db.Close()
}

func (rs *RaftStore) get(bucket, key []byte) (value []byte, err error) {
	err = rs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		v := b.Get(key)

		if v != nil {
			// copy value as it's only valid while the transaction is open
			value = make([]byte, len(v))
			copy(value, v)
			return nil
		}

		return ErrKeyNotFound
	})

	return value, err
}

func (rs *RaftStore) set(bucket, key, value []byte) error {
	return rs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.Put(key, value)
	})
}

func (rs *RaftStore) createBuckets() error {
	for _, bucket := range [][]byte{rs.logsBucket, rs.mainBucket} {
		err := rs.db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to create bucket: %s", bucket))
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// ----------------------------------------------------------------------------
// Options

func (opts RaftStoreOpts) toBoltOptions() *bolt.Options {
	return &bolt.Options{
		Timeout:         opts.Timeout,
		NoGrowSync:      opts.NoGrowSync,
		ReadOnly:        opts.ReadOnly,
		MmapFlags:       opts.MmapFlags,
		InitialMmapSize: opts.InitialMmapSize,
	}
}

// ----------------------------------------------------------------------------
// Auxiliary

func uint64ToBytes(i uint64) []byte {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, i)
	return raw
}
