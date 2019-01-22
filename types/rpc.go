package types

type (
	// RequestVoteRPC defines an RPC method used to handle requesting and gathering
	// Raft votes.
	RequestVoteRPC struct {
		Term         uint64 `json:"term"`           // the candidate's term
		CandidateID  string `json:"candidate_id"`   // the candidate requesting a vote
		LastLogIndex uint64 `json:"last_log_index"` // the index of the candidate's last log entry
		LastLogTerm  uint64 `json:"last_log_term"`  // the term of the candidate's last log entry
	}

	// AppendEntriesRPC defines an RPC method used to handle a leader attempting
	// to replicate log entries. It is also used as a heartbeat mechanism.
	AppendEntriesRPC struct {
		Term         uint64 `json:"term"`           // the leader's term
		LeaderID     string `json:"leader_id"`      // the leader's ID (this allows followers to redirect clients)
		PrevLogIndex uint64 `json:"prev_log_index"` // the index of the log entry immediately preceding new logs
		PrevLogTerm  uint64 `json:"prev_log_term"`  // the term of PrevLogIndex
		Logs         []Log  `json:"logs"`           // the log entries to store
		CommitIndex  uint64 `json:"commit_index"`   // the leader's commit index
	}
)
