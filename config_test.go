package goraft_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/BurntSushi/toml"

	"github.com/alexanderbez/goraft"
	"github.com/stretchr/testify/require"
)

func newTestConfig() goraft.Config {
	return goraft.Config{
		NodeID:            "node0",
		ListenAddr:        "node0.test",
		CompactionEnabled: true,
		DataDir:           "/tmp/node0/data",
		Peers: []goraft.Peer{
			{NodeID: "node1", Address: "node1.test"},
		},
	}
}

func TestConfigLoad(t *testing.T) {
	testCfg := newTestConfig()

	// encode the test config
	var buff bytes.Buffer
	enc := toml.NewEncoder(&buff)
	err := enc.Encode(testCfg)
	require.NoError(t, err)

	// create a tempfile and write the test config to file
	tmpfile, err := ioutil.TempFile(os.TempDir(), "test_raft_config")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write(buff.Bytes())
	require.NoError(t, err)

	// validate that we can load the test config from file
	resCfg, err := goraft.LoadConfigFromFile(tmpfile.Name())
	require.NoError(t, err)
	require.Equal(t, testCfg, resCfg)
}

func TestConfigValidation(t *testing.T) {
	testCfg := newTestConfig()
	err := testCfg.Validate()
	require.NoError(t, err)

	testCfg = newTestConfig()
	testCfg.ListenAddr = "http://bad_addr"
	err = testCfg.Validate()
	require.Error(t, err, "expected to fail due to invalid host")

	testCfg = newTestConfig()
	testCfg.NodeID = ""
	err = testCfg.Validate()
	require.Error(t, err, "expected to fail due to invalid node ID")

	testCfg = newTestConfig()
	testCfg.DataDir = ""
	err = testCfg.Validate()
	require.Error(t, err, "expected to fail due to invalid datadir")

	testCfg = newTestConfig()
	testCfg.Peers[0].NodeID = ""
	err = testCfg.Validate()
	require.Error(t, err, "expected to fail due to invalid peer node ID")

	testCfg = newTestConfig()
	testCfg.Peers[0].Address = "http://bad_addr"
	err = testCfg.Validate()
	require.Error(t, err, "expected to fail due to invalid peer node address")
}
