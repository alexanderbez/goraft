package types

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
)

func NewLog(index, term uint64, command []byte) Log {
	return Log{index, term, command}
}
