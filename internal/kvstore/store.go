// Package kvstore is the replicated state machine: a plain in-memory map
// that only ever changes by applying a command that has already been
// committed through Raft consensus. It never makes its own decisions about
// ordering or durability -- that is entirely Raft's job. This separation is
// what makes the state machine itself trivial to reason about and test.
package kvstore

import (
	"encoding/json"
	"sync"
)

type OpType string

const (
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
	// OpNoop is proposed as a "read barrier": waiting for it to commit and
	// apply guarantees every write ordered before it in the log has also
	// been applied, which is what makes a subsequent local read
	// linearizable instead of possibly-stale.
	OpNoop OpType = "noop"
)

// Command is the unit of work that gets proposed to Raft and, once
// committed, applied to every replica's state machine in the same order.
type Command struct {
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCommand(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}

// Store is the actual key-value map. Every method is safe for concurrent
// use since reads can be served while a concurrent Apply is in flight.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply executes an already-committed command. It must be deterministic:
// given the same sequence of commands, every replica must end up in the
// same state, which is the entire point of replicating the log rather than
// the state.
func (s *Store) Apply(cmd Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Snapshot returns a point-in-time copy of the full state, used both for
// Raft log compaction (so the log doesn't grow forever) and for bootstrapping
// a node that's catching up from a snapshot instead of replaying history.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

func (s *Store) RestoreSnapshot(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = m
	return nil
}

// AllEntries is for the Merkle anti-entropy layer and for tests -- it is
// never on the hot path for client reads/writes.
func (s *Store) AllEntries() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
