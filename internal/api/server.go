// Package api exposes the client-facing HTTP interface: a plain GET/PUT/DELETE
// key-value API on top of the replicated log, plus a couple of introspection
// endpoints used by the fault-injection test harness and the anti-entropy
// checker.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/Kuwar-Vishnudatt/raft-kv/internal/antientropy"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/kvstore"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/raftnode"
)

type Server struct {
	node    *raftnode.RaftNode
	store   *kvstore.Store
	checker *antientropy.Checker
	client  *http.Client
	writeTO time.Duration
}

func NewServer(node *raftnode.RaftNode, store *kvstore.Store, checker *antientropy.Checker) *Server {
	return &Server{
		node:    node,
		store:   store,
		checker: checker,
		client:  &http.Client{Timeout: 3 * time.Second},
		writeTO: 2 * time.Second,
	}
}

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/message", s.node.Transport().HandleMessage)
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/internal/state", s.handleInternalState)
	mux.HandleFunc("/internal/anti-entropy", s.handleAntiEntropy)
	mux.HandleFunc("/internal/partition", s.handlePartition)
	mux.HandleFunc("/internal/heal", s.handleHeal)
	return mux
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handleWrite(w, r, key, kvstore.OpPut)
	case http.MethodDelete:
		s.handleWrite(w, r, key, kvstore.OpDelete)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if !s.node.IsLeader() {
		if s.forwardToLeader(w, r) {
			return
		}
	}

	ctx, cancel := contextWithTimeout(r, s.writeTO)
	defer cancel()
	value, ok, err := s.node.LinearizableRead(ctx, key)
	if err != nil {
		writeAmbiguousOrError(w, err)
		return
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Write([]byte(value))
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, key string, op kvstore.OpType) {
	if !s.node.IsLeader() {
		if s.forwardToLeader(w, r) {
			return
		}
	}

	var value string
	if op == kvstore.OpPut {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		value = string(body)
	}

	ctx, cancel := contextWithTimeout(r, s.writeTO)
	defer cancel()
	err := s.node.Propose(ctx, kvstore.Command{Op: op, Key: key, Value: value})
	if err != nil {
		writeAmbiguousOrError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// writeAmbiguousOrError distinguishes "we know this failed" from "we don't
// know if this succeeded" -- a timeout waiting for a proposal to commit is
// the latter, and callers need to be told that explicitly rather than
// treating a 500 as "definitely didn't happen" and blindly retrying a
// non-idempotent write.
func writeAmbiguousOrError(w http.ResponseWriter, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		http.Error(w, "ambiguous: request timed out waiting for consensus; it may still commit", http.StatusGatewayTimeout)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// forwardToLeader transparently proxies the request so clients don't need
// their own leader-tracking/retry logic; it returns false (falls through
// to let the caller try locally) only if no leader is currently known,
// e.g. mid-election.
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request) bool {
	addr, ok := s.node.LeaderAddr()
	if !ok {
		return false
	}
	proxyReq, err := http.NewRequest(r.Method, "http://"+addr+r.URL.Path, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return true
	}
	resp, err := s.client.Do(proxyReq)
	if err != nil {
		http.Error(w, "leader unreachable: "+err.Error(), http.StatusBadGateway)
		return true
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.node.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":         status.ID,
		"leader":     status.Lead,
		"term":       status.Term,
		"raft_state": status.RaftState.String(),
		"applied":    status.Applied,
	})
}

func (s *Server) handleInternalState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.store.AllEntries())
}

func (s *Server) handleAntiEntropy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.checker.Results())
}

// --- fault-injection endpoints, used only by the test harness ---

func (s *Server) handlePartition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID uint64 `json:"peer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.node.Transport().Partition(req.PeerID)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHeal(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID uint64 `json:"peer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.node.Transport().Heal(req.PeerID)
	w.WriteHeader(http.StatusOK)
}

func contextWithTimeout(r *http.Request, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), timeout)
}
