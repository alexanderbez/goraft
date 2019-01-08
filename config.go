package goraft

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"
)

type (
	// Config defines a node's configuration needed for Raft consensus
	// bootstrapping and cluster management.
	Config struct {
		NodeID            string `toml:"node_id"`
		ListenAddr        string `toml:"listen_addr"`
		DataDir           string `toml:"data_dir"`
		CompactionEnabled bool   `toml:"compaction_enabled"`
		Peers             []Peer `toml:"peers"`
	}

	// Peer defines a node's peer in a cluster configuration.
	Peer struct {
		NodeID  string `toml:"node_id"`
		Address string `toml:"address"`
	}
)

// LoadConfigFromFile attempts to load a configuration from file. It returns an
// error upon failure of reading or parsing the file.
func LoadConfigFromFile(filepath string) (cfg Config, err error) {
	rawConfig, err := ioutil.ReadFile(filepath)
	err = toml.Unmarshal(rawConfig, &cfg)

	return
}

// Validate performs basic validation of a node's configuration.
func (cfg Config) Validate() error {
	return validation.ValidateStruct(
		&cfg,
		validation.Field(&cfg.NodeID, validation.Required),
		validation.Field(&cfg.ListenAddr, validation.Required, is.Host),
		validation.Field(&cfg.DataDir, validation.Required),
		validation.Field(&cfg.CompactionEnabled, validation.Required),
		validation.Field(&cfg.Peers),
	)
}

// Validate performs basic validation of a peer configuration.
func (p Peer) Validate() error {
	return validation.ValidateStruct(
		&p,
		validation.Field(&p.NodeID, validation.Required),
		validation.Field(&p.Address, validation.Required, is.Host),
	)
}
