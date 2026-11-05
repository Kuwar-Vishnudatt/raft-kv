// Package raftnode wires the bare etcd/raft consensus algorithm (which does
// no I/O of its own) up to real storage, a real network transport, and the
// kvstore state machine. Raft tells you *what* to persist and *who* to send
// messages to; everything in this package is the "how."
package raftnode

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// HTTPTransport ships raft messages between peers over plain HTTP. Sends are
// fire-and-forget: Raft's own protocol is designed to tolerate message loss
// and retransmit on the next tick, so a failed send here just becomes a
// ReportUnreachable call rather than something this layer needs to retry
// itself.
type HTTPTransport struct {
	selfID uint64
	peers  map[uint64]string // nodeID -> "host:port"
	node   raft.Node
	client *http.Client

	mu      sync.RWMutex
	blocked map[uint64]bool // nodeIDs currently "partitioned" from us, for fault injection
}

func NewHTTPTransport(selfID uint64, peers map[uint64]string, node raft.Node) *HTTPTransport {
	return &HTTPTransport{
		selfID:  selfID,
		peers:   peers,
		node:    node,
		client:  &http.Client{Timeout: 2 * time.Second},
		blocked: make(map[uint64]bool),
	}
}

// Partition simulates a network partition isolating `peerID` from this node:
// messages in both directions are dropped. This is intentionally done at the
// transport layer rather than via OS-level firewall rules (tc/iptables) so
// the same fault-injection code runs identically in tests, CI, and on any
// developer's machine without root privileges or platform-specific tooling
// -- Raft's safety properties don't care *why* a message didn't arrive.
func (t *HTTPTransport) Partition(peerID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[peerID] = true
}

func (t *HTTPTransport) Heal(peerID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, peerID)
}

func (t *HTTPTransport) isBlocked(peerID uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.blocked[peerID]
}

func (t *HTTPTransport) Send(msgs []*raftpb.Message) {
	for _, m := range msgs {
		if t.isBlocked(m.GetTo()) {
			continue
		}
		go t.sendOne(m)
	}
}

func (t *HTTPTransport) sendOne(m *raftpb.Message) {
	addr, ok := t.peers[m.GetTo()]
	if !ok {
		return
	}
	data, err := proto.Marshal(m)
	if err != nil {
		return
	}
	resp, err := t.client.Post("http://"+addr+"/raft/message", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.node.ReportUnreachable(m.GetTo())
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

// HandleMessage is the HTTP handler peers POST raft messages to.
func (t *HTTPTransport) HandleMessage(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var m raftpb.Message
	if err := proto.Unmarshal(data, &m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if t.isBlocked(m.GetFrom()) {
		// We're "partitioned" from the sender too -- drop it, same as a
		// real network partition would from either side.
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := t.node.Step(context.Background(), &m); err != nil && err != raft.ErrStepPeerNotFound {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
