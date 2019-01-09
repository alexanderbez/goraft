package storage_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/alexanderbez/goraft/storage"
	"github.com/alexanderbez/goraft/types"
	"github.com/stretchr/testify/require"
)

func newTestRaftStore(t *testing.T) (*storage.RaftStore, string) {
	tmpfile, err := ioutil.TempFile(os.TempDir(), "test_raft_db")
	require.NoError(t, err)

	opts := storage.RaftStoreOpts{Timeout: 1 * time.Second}
	raftStore, err := storage.NewRaftStore(tmpfile.Name(), opts)
	require.NoError(t, err)

	return raftStore, tmpfile.Name()
}

func TestRaftStore(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	err := raftStore.Close()
	require.NoError(t, err)
}

func TestRaftStoreReadOnly(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	raftStore.Close()

	opts := storage.RaftStoreOpts{Timeout: 1 * time.Second, ReadOnly: true}
	raftStore, err := storage.NewRaftStore(tmpfile, opts)
	require.NoError(t, err)

	err = raftStore.Set([]byte("nodeID"), []byte("node0"))
	require.Error(t, err)
}

func TestRaftStoreGetSet(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	key, value := []byte("nodeID"), []byte("node0")

	// require non-existant key returns nil
	res, err := raftStore.Get(key)
	require.Error(t, err)
	require.Equal(t, storage.ErrKeyNotFound, err)
	require.Nil(t, res)

	err = raftStore.Set(key, value)
	require.NoError(t, err)

	// require existant key returns correct value
	res, err = raftStore.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, res)
}

func TestRaftStoreGetSetUint64(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	key, value := []byte("commitID"), uint64(71)

	// require non-existant key returns nil
	res, err := raftStore.GetUint64(key)
	require.Error(t, err)
	require.Equal(t, storage.ErrKeyNotFound, err)
	require.Equal(t, uint64(0), res)

	err = raftStore.SetUint64(key, value)
	require.NoError(t, err)

	// require existant key returns correct value
	res, err = raftStore.GetUint64(key)
	require.NoError(t, err)
	require.Equal(t, value, res)
}

func TestRaftStoreGetSetLog(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	log := types.NewLog(1, 1, []byte("command"))

	// require non-existant key returns nil
	res, err := raftStore.GetLog(log.Index)
	require.Error(t, err)
	require.Equal(t, storage.ErrKeyNotFound, err)
	require.Equal(t, types.Log{}, res)

	err = raftStore.SetLog(log)
	require.NoError(t, err)

	// require existant key returns correct log
	res, err = raftStore.GetLog(log.Index)
	require.NoError(t, err)
	require.Equal(t, log, res)
}

func TestRaftStoreLogsIter(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	for _, log := range logs {
		err := raftStore.SetLog(log)
		require.NoError(t, err)
	}

	// create an iterator and retrieve all log indices
	var indices []uint64
	err := raftStore.LogsIter(
		logs[0].Index,
		logs[len(logs)-1].Index,
		func(log types.Log) error {
			indices = append(indices, log.Index)
			return nil
		},
	)
	require.NoError(t, err)

	// require all log indices are correct
	for i, index := range indices {
		require.Equal(t, index, logs[i].Index)
	}
}

func TestRaftStoreSetLogs(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	err := raftStore.SetLogs(logs)
	require.NoError(t, err)

	// require all logs exist that were inserted in batch
	for i, log := range logs {
		res, err := raftStore.GetLog(uint64(i))
		require.NoError(t, err)
		require.Equal(t, log, res)
	}
}

func TestRaftStoreGetLastLogIndex(t *testing.T) {
	raftStore, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftStore)

	llIndex := raftStore.GetLastLogIndex()
	require.Equal(t, uint64(0), llIndex)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	for _, log := range logs {
		err := raftStore.SetLog(log)
		require.NoError(t, err)
	}

	llIndex = raftStore.GetLastLogIndex()
	require.Equal(t, logs[len(logs)-1].Index, llIndex)
}
