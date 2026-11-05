// Package antientropy periodically compares this node's Merkle root against
// its peers' and records any divergence for an operator/dashboard to see.
// See the package comment on internal/merkle for why this detects rather
// than auto-repairs divergence.
package antientropy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Kuwar-Vishnudatt/raft-kv/internal/kvstore"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/merkle"
)

type PeerResult struct {
	CheckedAt     time.Time `json:"checked_at"`
	Match         bool      `json:"match"`
	DivergentKeys []string  `json:"divergent_keys,omitempty"`
	Error         string    `json:"error,omitempty"`
}

type Checker struct {
	store    *kvstore.Store
	peers    map[uint64]string // nodeID -> HTTP address
	client   *http.Client
	interval time.Duration

	mu      sync.RWMutex
	results map[uint64]PeerResult
}

func NewChecker(store *kvstore.Store, peers map[uint64]string, interval time.Duration) *Checker {
	return &Checker{
		store:    store,
		peers:    peers,
		client:   &http.Client{Timeout: 3 * time.Second},
		interval: interval,
		results:  make(map[uint64]PeerResult),
	}
}

func (c *Checker) Run(stopc <-chan struct{}) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.checkOnce()
		case <-stopc:
			return
		}
	}
}

func (c *Checker) checkOnce() {
	mine := c.store.AllEntries()
	myRoot := merkle.Build(mine).RootHash()

	for peerID, addr := range c.peers {
		theirs, theirRoot, err := c.fetchPeerSnapshot(addr)
		result := PeerResult{CheckedAt: time.Now()}
		if err != nil {
			result.Error = err.Error()
			c.setResult(peerID, result)
			continue
		}
		if string(myRoot) == string(theirRoot) {
			result.Match = true
		} else {
			result.Match = false
			result.DivergentKeys = merkle.DivergentKeys(mine, theirs)
		}
		c.setResult(peerID, result)
	}
}

func (c *Checker) fetchPeerSnapshot(addr string) (map[string]string, []byte, error) {
	resp, err := c.client.Get("http://" + addr + "/internal/state")
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("peer returned status %d", resp.StatusCode)
	}
	var data map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, nil, err
	}
	return data, merkle.Build(data).RootHash(), nil
}

func (c *Checker) setResult(peerID uint64, r PeerResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results[peerID] = r
}

func (c *Checker) Results() map[uint64]PeerResult {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[uint64]PeerResult, len(c.results))
	for k, v := range c.results {
		out[k] = v
	}
	return out
}
