package sharding

import (
	"fmt"
	"testing"
)

func TestShardForIsStable(t *testing.T) {
	r := NewRing(100)
	r.AddShard("shard-0")
	r.AddShard("shard-1")
	r.AddShard("shard-2")

	first, _ := r.ShardFor("user:42")
	for i := 0; i < 100; i++ {
		got, ok := r.ShardFor("user:42")
		if !ok || got != first {
			t.Fatalf("ShardFor should be deterministic, got %q want %q", got, first)
		}
	}
}

func TestDistributionIsReasonablyEven(t *testing.T) {
	r := NewRing(150)
	r.AddShard("shard-0")
	r.AddShard("shard-1")
	r.AddShard("shard-2")

	counts := map[string]int{}
	const n = 30000
	for i := 0; i < n; i++ {
		shard, _ := r.ShardFor(fmt.Sprintf("key-%d", i))
		counts[shard]++
	}

	// With 3 shards an even split is n/3 each; allow 25% slack either way --
	// virtual nodes smooth the distribution but it's still randomized hashing,
	// not a perfect partition.
	want := n / 3
	slack := want / 4
	for shard, c := range counts {
		if c < want-slack || c > want+slack {
			t.Errorf("shard %s got %d keys, want ~%d (+/- %d)", shard, c, want, slack)
		}
	}
}

func TestRemovingShardOnlyRemapsItsOwnKeys(t *testing.T) {
	r := NewRing(150)
	r.AddShard("shard-0")
	r.AddShard("shard-1")
	r.AddShard("shard-2")

	const n = 5000
	before := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		shard, _ := r.ShardFor(key)
		before[key] = shard
	}

	r.RemoveShard("shard-1")

	moved := 0
	for key, oldShard := range before {
		newShard, _ := r.ShardFor(key)
		if oldShard == "shard-1" {
			continue // these keys had to move somewhere
		}
		if newShard != oldShard {
			moved++
		}
	}

	// The whole point of consistent hashing over mod-N hashing: removing one
	// of three shards should not reshuffle keys that belonged to the other two.
	if moved != 0 {
		t.Fatalf("expected 0 keys not owned by the removed shard to move, got %d", moved)
	}
}

func TestShardForEmptyRing(t *testing.T) {
	r := NewRing(100)
	if _, ok := r.ShardFor("anything"); ok {
		t.Fatal("expected no shard for an empty ring")
	}
}
