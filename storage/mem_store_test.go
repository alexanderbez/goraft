package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alexanderbez/goraft/storage"
	"github.com/alexanderbez/goraft/types"
)

func TestMemStore(t *testing.T) {
	memStore := storage.NewMemStore()
	require.NotNil(t, memStore)

	err := memStore.Close()
	require.NoError(t, err)
}

func TestMemStoreGetSet(t *testing.T) {
	memStore := storage.NewMemStore()
	require.NotNil(t, memStore)

	key, value := []byte("nodeID"), []byte("node0")

	// require non-existent key returns nil
	res, err := memStore.Get(key)
	require.Error(t, err)
	require.Nil(t, res)

	err = memStore.Set(key, value)
	require.NoError(t, err)

	// require existent key returns correct value
	res, err = memStore.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, res)
}

func TestMemStoreGetSetUint64(t *testing.T) {
	memStore := storage.NewMemStore()
	require.NotNil(t, memStore)

	key, value := []byte("commitID"), uint64(71)

	// require non-existent key returns nil
	res, err := memStore.GetUint64(key)
	require.Error(t, err)
	require.Equal(t, uint64(0), res)

	err = memStore.SetUint64(key, value)
	require.NoError(t, err)

	// require existent key returns correct value
	res, err = memStore.GetUint64(key)
	require.NoError(t, err)
	require.Equal(t, value, res)
}

func TestMemStoreSetLogs(t *testing.T) {
	memStore := storage.NewMemStore()
	require.NotNil(t, memStore)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	err := memStore.SetLogs(logs)
	require.NoError(t, err)

	// require all logs exist that were inserted in batch
	for i, log := range logs {
		res, err := memStore.GetLog(uint64(i))
		require.NoError(t, err)
		require.Equal(t, log, res)
	}
}

func TestMemStoreGetSetLog(t *testing.T) {
	memStore := storage.NewMemStore()
	require.NotNil(t, memStore)

	log := types.NewLog(1, 1, []byte("command"))

	// require non-existent key returns nil
	res, err := memStore.GetLog(log.Index)
	require.Error(t, err)
	require.Equal(t, storage.ErrKeyNotFound, err)
	require.Equal(t, types.Log{}, res)

	err = memStore.SetLog(log)
	require.NoError(t, err)

	// require existent key returns correct log
	res, err = memStore.GetLog(log.Index)
	require.NoError(t, err)
	require.Equal(t, log, res)
}

func TestMemStoreGetLastLogIndex(t *testing.T) {
	memStore := storage.NewMemStore()
	require.NotNil(t, memStore)

	llIndex := memStore.GetLastLogIndex()
	require.Equal(t, uint64(0), llIndex)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	for _, log := range logs {
		err := memStore.SetLog(log)
		require.NoError(t, err)
	}

	llIndex = memStore.GetLastLogIndex()
	require.Equal(t, logs[len(logs)-1].Index, llIndex)
}

func TestMemStoreLogsIter(t *testing.T) {
	memStore := storage.NewMemStore()
	require.NotNil(t, memStore)

	logs := make([]types.Log, 20)
	for i := range logs {
		logs[i] = types.NewLog(uint64(i), 1, nil)
	}

	for _, log := range logs {
		err := memStore.SetLog(log)
		require.NoError(t, err)
	}

	// create an iterator and retrieve all log indices
	var indices []uint64
	err := memStore.LogsIter(
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
