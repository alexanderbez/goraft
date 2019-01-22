package goraft

// StateMachine defines an interface that any finite state machine used in Raft
// must implement. The state machine can be completely agnostic to the mechanics
// of Raft. It consumes commands from Raft log entries and returns any interface
// value resulting in success or a potential error.
type StateMachine interface {
	Apply(command []byte) (interface{}, error)
}
