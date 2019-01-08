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
	raftDB, err := storage.NewRaftStore(tmpfile.Name(), opts)
	require.NoError(t, err)

	return raftDB, tmpfile.Name()
}

func TestRaftStore(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	err := raftDB.Close()
	require.NoError(t, err)
}

func TestRaftStoreReadOnly(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	raftDB.Close()

	opts := storage.RaftStoreOpts{Timeout: 1 * time.Second, ReadOnly: true}
	raftDB, err := storage.NewRaftStore(tmpfile, opts)
	require.NoError(t, err)

	err = raftDB.Set([]byte("nodeID"), []byte("node0"))
	require.Error(t, err)
}

func TestRaftStoreGetSet(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	key, value := []byte("nodeID"), []byte("node0")

	// require non-existant key returns nil
	res, err := raftDB.Get(key)
	require.Error(t, err)
	require.Nil(t, res)

	err = raftDB.Set(key, value)
	require.NoError(t, err)

	// require existant key returns correct value
	res, err = raftDB.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, res)
}

func TestRaftStoreGetSetUint64(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	key, value := []byte("commitID"), uint64(71)

	// require non-existant key returns nil
	res, err := raftDB.GetUint64(key)
	require.Error(t, err)
	require.Equal(t, uint64(0), res)

	err = raftDB.SetUint64(key, value)
	require.NoError(t, err)

	// require existant key returns correct value
	res, err = raftDB.GetUint64(key)
	require.NoError(t, err)
	require.Equal(t, value, res)
}

func TestRaftStoreGetSetLog(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	log := types.NewLog(1, 1, []byte("command"))

	res, err := raftDB.GetLog(log.Index)
	require.Error(t, err)
	require.Equal(t, types.Log{}, res)

	err = raftDB.SetLog(log)
	require.NoError(t, err)

	res, err = raftDB.GetLog(log.Index)
	require.NoError(t, err)
	require.Equal(t, log, res)
}

func TestRaftStoreLogsIter(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	for _, log := range logs {
		err := raftDB.SetLog(log)
		require.NoError(t, err)
	}

	var indices []uint64
	err := raftDB.LogsIter(
		logs[0].Index,
		logs[len(logs)-1].Index,
		func(log types.Log) error {
			indices = append(indices, log.Index)
			return nil
		},
	)
	require.NoError(t, err)

	for i, index := range indices {
		require.Equal(t, index, logs[i].Index)
	}
}

func TestRaftStoreSetLogs(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	err := raftDB.SetLogs(logs)
	require.NoError(t, err)

	for i, log := range logs {
		res, err := raftDB.GetLog(uint64(i))
		require.NoError(t, err)
		require.Equal(t, log, res)
	}
}

func TestRaftStoreGetLastLogIndex(t *testing.T) {
	raftDB, tmpfile := newTestRaftStore(t)

	defer os.Remove(tmpfile)
	require.NotNil(t, raftDB)

	llIndex := raftDB.GetLastLogIndex()
	require.Equal(t, uint64(0), llIndex)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	for _, log := range logs {
		err := raftDB.SetLog(log)
		require.NoError(t, err)
	}

	llIndex = raftDB.GetLastLogIndex()
	require.Equal(t, logs[len(logs)-1].Index, llIndex)
}
