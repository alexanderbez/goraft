package types

import "encoding/json"

// Assert serialization types at compile time.
var (
	_ LogEncoder = JSONLogEncoder{}
	_ LogDecoder = JSONLogDecoder{}
)

type (
	// Log represents a command to execute in a state machine. Each log contains
	// a monotonically increasing index and a term for when the log entry was
	// received by a leader.
	//
	// Contract: If two logs contain a log entry with the same index and term,
	// then the logs are identical in all entries up through the given index.
	Log struct {
		Index   uint64 `json:"index"`
		Term    uint64 `json:"term"`
		Command []byte `json:"command"`
	}

	// LogEncoder describes an interface that is able to encode a log into an
	// arbitrary series of bytes.
	LogEncoder interface {
		Encode(Log) ([]byte, error)
	}

	// LogDecoder describes an interface that is able to decode an arbitrary
	// series of bytes into a log.
	LogDecoder interface {
		Decode([]byte, *Log) error
	}

	// JSONLogEncoder provides log JSON serialization. It implements the
	// LogEncoder interface.
	JSONLogEncoder struct{}

	// JSONLogDecoder provides log JSON deserialization. It implements the
	// LogDecoder interface.
	JSONLogDecoder struct{}
)

func NewLog(index, term uint64, command []byte) Log {
	return Log{index, term, command}
}

// Encode returns a JSON encoded log. It returns an error upon failure.
func (e JSONLogEncoder) Encode(log Log) ([]byte, error) {
	return json.Marshal(log)
}

// Decode returns a JSON decoded log. It returns an error upon failure.
func (d JSONLogDecoder) Decode(data []byte, log *Log) error {
	return json.Unmarshal(data, log)
}
