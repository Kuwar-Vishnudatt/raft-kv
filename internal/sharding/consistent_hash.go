// Package sharding maps keys to shards via consistent hashing, so this
// single-Raft-group store can be run as multiple independent replicated
// groups (shards) for horizontal scale, the same pattern production
// systems like CockroachDB/TiKV use (one Raft group per range), without
// each shard needing to know anything about the others.
package sharding

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// Ring is a consistent-hash ring over shard IDs, using virtual nodes so that
// shards each own a roughly even share of the key space instead of a plain
// mod-N hash (which would reshuffle almost every key whenever the shard
// count changes).
type Ring struct {
	mu          sync.RWMutex
	virtualNode int
	sortedHash  []uint32
	hashToShard map[uint32]string
}

func NewRing(virtualNodesPerShard int) *Ring {
	if virtualNodesPerShard <= 0 {
		virtualNodesPerShard = 150
	}
	return &Ring{
		virtualNode: virtualNodesPerShard,
		hashToShard: make(map[uint32]string),
	}
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (r *Ring) AddShard(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.virtualNode; i++ {
		h := hashString(shardID + "#" + strconv.Itoa(i))
		r.hashToShard[h] = shardID
		r.sortedHash = append(r.sortedHash, h)
	}
	sort.Slice(r.sortedHash, func(i, j int) bool { return r.sortedHash[i] < r.sortedHash[j] })
}

func (r *Ring) RemoveShard(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.sortedHash[:0]
	for _, h := range r.sortedHash {
		if r.hashToShard[h] == shardID {
			delete(r.hashToShard, h)
			continue
		}
		kept = append(kept, h)
	}
	r.sortedHash = kept
}

// ShardFor returns which shard owns a key: walk clockwise from the key's
// hash to the first virtual node, exactly as in Karger et al.'s original
// consistent hashing scheme.
func (r *Ring) ShardFor(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHash) == 0 {
		return "", false
	}
	h := hashString(key)
	idx := sort.Search(len(r.sortedHash), func(i int) bool { return r.sortedHash[i] >= h })
	if idx == len(r.sortedHash) {
		idx = 0 // wrap around the ring
	}
	return r.hashToShard[r.sortedHash[idx]], true
}

func (r *Ring) Shards() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]bool)
	var out []string
	for _, s := range r.hashToShard {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
