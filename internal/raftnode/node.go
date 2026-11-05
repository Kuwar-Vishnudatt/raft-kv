package raftnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/Kuwar-Vishnudatt/raft-kv/internal/kvstore"
)

// Config is deliberately static: every node is started knowing the full,
// fixed peer set up front. Raft supports dynamic membership via joint
// consensus (ConfChange), but that is a substantial additional protocol in
// its own right; this project scopes it out explicitly rather than
// half-implement it. See docs/ARCHITECTURE.md.
type Config struct {
	ID            uint64
	Peers         map[uint64]string // nodeID -> "host:port", includes self
	DataDir       string
	SnapshotEvery uint64 // applied-entry count between snapshot+compactions
}

// envelope tags every proposed command with a locally-unique ID so the apply
// loop can signal the specific goroutine that proposed it once (and if) it
// actually commits -- Raft itself has no concept of "the caller waiting on
// this," only a replicated log.
type envelope struct {
	ID  string          `json:"id"`
	Cmd kvstore.Command `json:"cmd"`
}

type RaftNode struct {
	cfg       Config
	node      raft.Node
	storage   *DiskStorage
	transport *HTTPTransport
	store     *kvstore.Store

	confState    *raftpb.ConfState
	appliedIndex uint64
	snapshotIdx  uint64
	proposalSeq  uint64

	pendingMu sync.Mutex
	pending   map[string]chan error

	leaderMu sync.RWMutex
	leaderID uint64

	stopc chan struct{}
	wg    sync.WaitGroup
}

func NewRaftNode(cfg Config, store *kvstore.Store) (*RaftNode, error) {
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 1000
	}

	storage, existed, err := OpenDiskStorage(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open disk storage: %w", err)
	}

	raftCfg := &raft.Config{
		ID:                        cfg.ID,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   storage,
		MaxSizePerMsg:             4096,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		PreVote:                   true, // avoids needless elections when a partitioned node rejoins, see ARCHITECTURE.md
	}

	var node raft.Node
	if existed {
		node = raft.RestartNode(raftCfg)
	} else {
		var peers []raft.Peer
		for id := range cfg.Peers {
			peers = append(peers, raft.Peer{ID: id})
		}
		node = raft.StartNode(raftCfg, peers)
	}

	rn := &RaftNode{
		cfg:     cfg,
		node:    node,
		storage: storage,
		store:   store,
		pending: make(map[string]chan error),
		stopc:   make(chan struct{}),
	}
	rn.transport = NewHTTPTransport(cfg.ID, cfg.Peers, node)
	return rn, nil
}

func (rn *RaftNode) Transport() *HTTPTransport { return rn.transport }

// Run drives the Raft event loop and must be started in its own goroutine.
// It owns the Raft node for its entire lifetime -- nothing else may call
// node.Step/Tick/Advance directly.
func (rn *RaftNode) Run() {
	rn.wg.Add(1)
	defer rn.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		// time.Ticker only ever buffers a single pending tick: if this loop
		// is kept continuously busy (Ready() has a backlog under sustained
		// high proposal throughput), the main select below can keep
		// resolving to the Ready case and silently drop elapsed ticks
		// instead of queuing them. That desyncs Raft's internal election/
		// heartbeat timers (measured in ticks) from wall-clock time, which
		// is exactly backwards from what you want under load -- failure
		// detection should stay prompt precisely when the cluster is busy.
		// Draining any already-elapsed tick first, every iteration, bounds
		// the delay to roughly one loop iteration instead of leaving it to
		// chance which case a Go select picks among several ready ones.
		select {
		case <-ticker.C:
			rn.node.Tick()
		default:
		}

		select {
		case <-ticker.C:
			rn.node.Tick()

		case rd := <-rn.node.Ready():
			if err := rn.storage.Append(rd.Entries); err != nil {
				log.Fatalf("node %d: failed to persist entries: %v", rn.cfg.ID, err)
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := rn.storage.SetHardState(rd.HardState); err != nil {
					log.Fatalf("node %d: failed to persist hardstate: %v", rn.cfg.ID, err)
				}
			}
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rn.store.RestoreSnapshot(rd.Snapshot.GetData()); err != nil {
					log.Fatalf("node %d: failed to restore snapshot: %v", rn.cfg.ID, err)
				}
				if cs := rd.Snapshot.GetMetadata().GetConfState(); cs != nil {
					rn.confState = cs
				}
				rn.appliedIndex = rd.Snapshot.GetMetadata().GetIndex()
				rn.snapshotIdx = rd.Snapshot.GetMetadata().GetIndex()
			}

			rn.transport.Send(rd.Messages)

			for _, entry := range rd.CommittedEntries {
				rn.applyEntry(entry)
			}

			rn.maybeSnapshot()
			rn.updateLeader()

			rn.node.Advance()

		case <-rn.stopc:
			rn.node.Stop()
			rn.storage.Close()
			return
		}
	}
}

func (rn *RaftNode) applyEntry(entry *raftpb.Entry) {
	rn.appliedIndex = entry.GetIndex()

	if entry.GetType() == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := proto.Unmarshal(entry.GetData(), &cc); err != nil {
			log.Printf("node %d: bad conf change: %v", rn.cfg.ID, err)
			return
		}
		rn.confState = rn.node.ApplyConfChange(&cc)
		return
	}

	if len(entry.GetData()) == 0 {
		return // raft emits an empty entry on each new leader's election; nothing to apply
	}

	var env envelope
	if err := json.Unmarshal(entry.GetData(), &env); err != nil {
		log.Printf("node %d: failed to decode committed entry: %v", rn.cfg.ID, err)
		return
	}
	if env.Cmd.Op != "" && env.Cmd.Op != kvstore.OpNoop {
		rn.store.Apply(env.Cmd)
	}
	rn.signalPending(env.ID, nil)
}

func (rn *RaftNode) maybeSnapshot() {
	if rn.appliedIndex-rn.snapshotIdx < rn.cfg.SnapshotEvery {
		return
	}
	data, err := rn.store.Snapshot()
	if err != nil {
		log.Printf("node %d: snapshot failed: %v", rn.cfg.ID, err)
		return
	}
	if _, err := rn.storage.CreateSnapshot(rn.appliedIndex, rn.confState, data); err != nil {
		log.Printf("node %d: create snapshot failed: %v", rn.cfg.ID, err)
		return
	}
	compactIndex := uint64(1)
	if rn.appliedIndex > 1 {
		compactIndex = rn.appliedIndex - 1
	}
	if err := rn.storage.Compact(compactIndex); err != nil && !errors.Is(err, raft.ErrCompacted) {
		log.Printf("node %d: compact failed: %v", rn.cfg.ID, err)
		return
	}
	rn.snapshotIdx = rn.appliedIndex
}

func (rn *RaftNode) updateLeader() {
	status := rn.node.Status()
	rn.leaderMu.Lock()
	rn.leaderID = status.Lead
	rn.leaderMu.Unlock()
}

func (rn *RaftNode) signalPending(id string, err error) {
	rn.pendingMu.Lock()
	waitc, ok := rn.pending[id]
	if ok {
		delete(rn.pending, id)
	}
	rn.pendingMu.Unlock()
	if ok {
		select {
		case waitc <- err:
		default:
		}
	}
}

// Propose commits a write command through the log and blocks until it is
// applied (or ctx expires). A timeout here means an AMBIGUOUS outcome, not a
// confirmed failure -- the command may commit anyway shortly after,
// particularly if it was the leader that stopped responding. Callers should
// treat it the same way they'd treat a timeout on a payment API: safe to
// retry only if the operation is idempotent (see docs/ARCHITECTURE.md).
func (rn *RaftNode) Propose(ctx context.Context, cmd kvstore.Command) error {
	id := fmt.Sprintf("%d-%d", rn.cfg.ID, atomic.AddUint64(&rn.proposalSeq, 1))
	data, err := json.Marshal(envelope{ID: id, Cmd: cmd})
	if err != nil {
		return err
	}

	waitc := make(chan error, 1)
	rn.pendingMu.Lock()
	rn.pending[id] = waitc
	rn.pendingMu.Unlock()
	defer func() {
		rn.pendingMu.Lock()
		delete(rn.pending, id)
		rn.pendingMu.Unlock()
	}()

	if err := rn.node.Propose(ctx, data); err != nil {
		return err
	}

	select {
	case err := <-waitc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LinearizableRead guarantees the returned value reflects every write
// committed before this call started, by routing the read through the log
// as a no-op "barrier" before touching local state. This trades latency (a
// full consensus round trip per read) for the simplest-to-prove-correct
// implementation; docs/ARCHITECTURE.md discusses the ReadIndex optimization
// as a documented follow-on that avoids the log write.
func (rn *RaftNode) LinearizableRead(ctx context.Context, key string) (string, bool, error) {
	if err := rn.Propose(ctx, kvstore.Command{Op: kvstore.OpNoop}); err != nil {
		return "", false, err
	}
	v, ok := rn.store.Get(key)
	return v, ok, nil
}

func (rn *RaftNode) IsLeader() bool {
	return rn.node.Status().RaftState == raft.StateLeader
}

func (rn *RaftNode) LeaderAddr() (string, bool) {
	rn.leaderMu.RLock()
	id := rn.leaderID
	rn.leaderMu.RUnlock()
	if id == 0 {
		return "", false
	}
	addr, ok := rn.cfg.Peers[id]
	return addr, ok
}

func (rn *RaftNode) Status() raft.Status { return rn.node.Status() }

func (rn *RaftNode) Stop() {
	close(rn.stopc)
	rn.wg.Wait()
}
