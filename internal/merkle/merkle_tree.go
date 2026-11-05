// Package merkle builds a Merkle tree over a replica's key-value state and
// compares trees across replicas to detect divergence.
//
// Note what this is -- and deliberately is not -- for: in a correctly
// functioning Raft cluster, every replica's state machine is guaranteed
// identical by the consensus protocol itself once a command is applied, so
// two replicas should never actually diverge. This package exists as a
// defense-in-depth *integrity checker*: it catches the cases Raft's own
// guarantees don't cover -- a bit flip on disk, a bug in Apply(), an
// operator manually editing a data file -- and raises an alarm. It does not
// attempt automatic repair the way a Dynamo-style eventually-consistent
// system's anti-entropy would, because in a strongly consistent system,
// silently "fixing" a detected divergence would be hiding a real bug rather
// than tolerating an expected one.
package merkle

import (
	"crypto/sha256"
	"sort"
)

type Tree struct {
	leaves []leaf
	nodes  [][]byte // level 0 = leaf hashes, last level = [root]
}

type leaf struct {
	key  string
	hash [32]byte
}

func hashKV(key, value string) [32]byte {
	return sha256.Sum256([]byte(key + "\x00" + value))
}

// Build constructs a tree from a snapshot of key-value pairs. Keys are
// sorted first so that two replicas holding the same data always build
// byte-identical trees regardless of map iteration order.
func Build(data map[string]string) *Tree {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	t := &Tree{}
	level := make([][]byte, 0, len(keys))
	for _, k := range keys {
		h := hashKV(k, data[k])
		t.leaves = append(t.leaves, leaf{key: k, hash: h})
		level = append(level, h[:])
	}

	if len(level) == 0 {
		t.nodes = [][]byte{emptyHash()}
		return t
	}

	t.nodes = level
	for len(level) > 1 {
		level = reduceLevel(level)
	}
	t.nodes = append(t.nodes, level...)
	return t
}

func emptyHash() []byte {
	h := sha256.Sum256(nil)
	return h[:]
}

func reduceLevel(level [][]byte) [][]byte {
	var next [][]byte
	for i := 0; i < len(level); i += 2 {
		if i+1 == len(level) {
			next = append(next, level[i]) // odd node out, carries up unchanged
			continue
		}
		h := sha256.Sum256(append(append([]byte{}, level[i]...), level[i+1]...))
		next = append(next, h[:])
	}
	return next
}

// RootHash is the single value two replicas compare to know, in one
// comparison, whether their entire state machines match.
func (t *Tree) RootHash() []byte {
	if len(t.nodes) == 0 {
		return emptyHash()
	}
	return t.nodes[len(t.nodes)-1]
}

// DivergentKeys compares two trees built from the same key space and
// returns every key whose value differs (or exists in only one of them).
// This is a straightforward leaf-level diff rather than a tree-walk descent
// -- correct and simple to verify, at the cost of being O(n) instead of
// O(log n) to localize a single divergent key; for a defense-in-depth check
// that runs periodically in the background rather than on any hot path,
// that trade is the right one for this project's scope.
func DivergentKeys(a, b map[string]string) []string {
	var diffs []string
	seen := make(map[string]bool, len(a)+len(b))
	for k, va := range a {
		seen[k] = true
		if vb, ok := b[k]; !ok || va != vb {
			diffs = append(diffs, k)
		}
	}
	for k := range b {
		if !seen[k] {
			diffs = append(diffs, k)
		}
	}
	sort.Strings(diffs)
	return diffs
}
