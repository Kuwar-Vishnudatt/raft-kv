// Package integration spins up real raft-kv clusters -- real HTTP listeners
// on localhost, real goroutine schedulers, real disk-backed WALs in a temp
// dir -- and drives them the same way a deployed cluster would be driven:
// over HTTP, with real process-equivalent kill/restart and real simulated
// network partitions. Nothing here talks to the internal Go types directly
// except to set them up; every assertion goes through the same HTTP API an
// external client would use.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/Kuwar-Vishnudatt/raft-kv/internal/antientropy"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/api"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/kvstore"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/raftnode"
)

type nodeHandle struct {
	id     uint64
	addr   string
	dir    string
	node   *raftnode.RaftNode
	server *http.Server
}

// Cluster's nodes map is mutated by Kill/Restart (called from the test's
// main goroutine, typically) while it's read by every HTTP helper -- which
// in the linearizability test are called concurrently from several
// simulated-client goroutines. mu protects every access to nodes/peers so
// "kill the leader mid-traffic" is itself race-free, not just the system
// under test.
type Cluster struct {
	t     *testing.T
	peers map[uint64]string

	mu    sync.RWMutex
	nodes map[uint64]*nodeHandle
	dirs  map[uint64]string
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// StartCluster brings up n nodes, all pointed at each other, and blocks
// until a leader is elected.
func StartCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	peers := make(map[uint64]string, n)
	dirs := make(map[uint64]string, n)
	for i := 1; i <= n; i++ {
		id := uint64(i)
		peers[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
		dirs[id] = t.TempDir()
	}

	c := &Cluster{t: t, peers: peers, nodes: make(map[uint64]*nodeHandle), dirs: dirs}
	for id := range peers {
		c.startNode(id)
	}
	c.awaitLeader(10 * time.Second)
	return c
}

func (c *Cluster) startNode(id uint64) {
	c.t.Helper()
	store := kvstore.NewStore()
	node, err := raftnode.NewRaftNode(raftnode.Config{
		ID:            id,
		Peers:         c.peers,
		DataDir:       c.dirs[id],
		SnapshotEvery: 1000,
	}, store)
	if err != nil {
		c.t.Fatalf("node %d: failed to start: %v", id, err)
	}
	go node.Run()

	otherPeers := make(map[uint64]string, len(c.peers)-1)
	for pid, addr := range c.peers {
		if pid != id {
			otherPeers[pid] = addr
		}
	}
	checker := antientropy.NewChecker(store, otherPeers, 2*time.Second)
	stopc := make(chan struct{})
	go checker.Run(stopc)

	srv := api.NewServer(node, store, checker)
	httpServer := &http.Server{Addr: c.peers[id], Handler: srv.Routes()}
	ln, err := net.Listen("tcp", c.peers[id])
	if err != nil {
		c.t.Fatalf("node %d: failed to listen on %s: %v", id, c.peers[id], err)
	}
	go httpServer.Serve(ln)

	c.mu.Lock()
	c.nodes[id] = &nodeHandle{id: id, addr: c.peers[id], dir: c.dirs[id], node: node, server: httpServer}
	c.mu.Unlock()
}

// Kill simulates a true process crash: the Raft loop stops and the listener
// closes, discarding any in-memory state. Only what was fsynced to disk
// survives, same as a real `kill -9`.
func (c *Cluster) Kill(id uint64) {
	c.t.Helper()
	c.mu.Lock()
	h, ok := c.nodes[id]
	if ok {
		delete(c.nodes, id)
	}
	c.mu.Unlock()
	if !ok {
		c.t.Fatalf("no such node %d", id)
	}
	h.server.Close()
	h.node.Stop()
}

// Restart brings a killed node back using the same data directory, so it
// must recover via its disk-backed WAL/snapshot rather than starting fresh.
func (c *Cluster) Restart(id uint64) {
	c.t.Helper()
	c.mu.RLock()
	_, alive := c.nodes[id]
	c.mu.RUnlock()
	if alive {
		c.t.Fatalf("node %d is still running, Kill it first", id)
	}
	c.startNode(id)
}

func (c *Cluster) nodeOrNil(id uint64) *nodeHandle {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

func (c *Cluster) Partition(a, b uint64) {
	if ha := c.nodeOrNil(a); ha != nil {
		ha.node.Transport().Partition(b)
	}
	if hb := c.nodeOrNil(b); hb != nil {
		hb.node.Transport().Partition(a)
	}
}

func (c *Cluster) Heal(a, b uint64) {
	if ha := c.nodeOrNil(a); ha != nil {
		ha.node.Transport().Heal(b)
	}
	if hb := c.nodeOrNil(b); hb != nil {
		hb.node.Transport().Heal(a)
	}
}

// PartitionMinority isolates a single node from the other n-1, the classic
// "minority partition" fault: the isolated node must not be able to commit
// anything on its own, while the remaining majority keeps working.
func (c *Cluster) PartitionMinority(id uint64) {
	for other := range c.peers {
		if other != id {
			c.Partition(id, other)
		}
	}
}

func (c *Cluster) HealAll() {
	for a := range c.peers {
		for b := range c.peers {
			if a != b {
				c.Heal(a, b)
			}
		}
	}
}

func (c *Cluster) liveIDs() []uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]uint64, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (c *Cluster) awaitLeader(timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range c.liveIDs() {
			if leader := c.statusLeader(id); leader != 0 {
				return leader
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %s", timeout)
	return 0
}

func (c *Cluster) statusLeader(id uint64) uint64 {
	h := c.nodeOrNil(id)
	if h == nil {
		return 0
	}
	resp, err := http.Get("http://" + h.addr + "/status")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var status struct {
		Leader uint64 `json:"leader"`
	}
	json.NewDecoder(resp.Body).Decode(&status)
	return status.Leader
}

// statusLeaderAny asks an arbitrary live node who it thinks the leader is,
// for callers that don't need to wait/retry (unlike awaitLeader).
func (c *Cluster) statusLeaderAny() uint64 {
	for _, id := range c.liveIDs() {
		if leader := c.statusLeader(id); leader != 0 {
			return leader
		}
	}
	return 0
}

// AwaitLeader blocks until some live node reports a leader (used after
// fault injection, where re-election takes a few hundred ms).
func (c *Cluster) AwaitLeader(timeout time.Duration) uint64 {
	return c.awaitLeader(timeout)
}

// AwaitLeaderChange blocks until a *currently live* node reports itself (or
// is reported) as leader, and that leader is not oldLeader. A plain
// AwaitLeader right after killing the leader is not enough: surviving
// followers hold stale knowledge of the dead leader until their own
// election timeout fires, so a naive "leader is non-zero" check can return
// immediately with the leader that was just killed.
func (c *Cluster) AwaitLeaderChange(oldLeader uint64, timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range c.liveIDs() {
			leader := c.statusLeader(id)
			if leader != 0 && leader != oldLeader {
				if h := c.nodeOrNil(leader); h != nil {
					return leader
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no new leader (other than %d) elected within %s", oldLeader, timeout)
	return 0
}

// AnyNodeAddr returns the address of an arbitrary live node, for tests that
// don't care which one they talk to (requests get forwarded to the leader
// automatically). It fails the test if no node is live.
func (c *Cluster) AnyNodeAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, h := range c.nodes {
		return h.addr
	}
	c.t.Fatal("no live nodes")
	return ""
}

// AnyLiveNodeAddrForTest is like AnyNodeAddr but returns "" instead of
// failing the test when nothing is live -- used by background client
// goroutines that must tolerate a brief window with no reachable node
// during fault injection rather than aborting the whole test.
func (c *Cluster) AnyLiveNodeAddrForTest() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.nodes) == 0 {
		return ""
	}
	ids := make([]uint64, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	return c.nodes[ids[rand.Intn(len(ids))]].addr
}

func (c *Cluster) Addr(id uint64) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	h, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("node %d is not live", id)
	}
	return h.addr
}

func (c *Cluster) Put(addr, key, value string) (int, error) {
	req, err := http.NewRequest(http.MethodPut, "http://"+addr+"/kv/"+key, bytes.NewBufferString(value))
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (c *Cluster) Get(addr, key string) (string, int, error) {
	resp, err := http.Get("http://" + addr + "/kv/" + key)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), resp.StatusCode, nil
}

func (c *Cluster) Shutdown() {
	c.mu.Lock()
	handles := make([]*nodeHandle, 0, len(c.nodes))
	for id, h := range c.nodes {
		handles = append(handles, h)
		delete(c.nodes, id)
	}
	c.mu.Unlock()

	for _, h := range handles {
		h.server.Close()
		h.node.Stop()
	}
}

func init() {
	// Quiet raft's own internal logger noise in test output; failures are
	// asserted via HTTP responses and cluster state, not log scraping.
	raft.SetLogger(&raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)})
}
