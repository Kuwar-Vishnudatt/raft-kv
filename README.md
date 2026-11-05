# raft-kv

A sharded, linearizable distributed key-value store: Raft consensus per
shard (via etcd's extracted `raft` library), consistent hashing across
shards, a disk-backed WAL with checksummed replay, Merkle-tree anti-entropy
as a defense-in-depth integrity checker, and a correctness test suite that
actually injects network partitions and process kills and verifies the
resulting history is linearizable with
[Porcupine](https://github.com/anishathalye/porcupine) rather than
eyeballing it.

## Why this exists

A plain CRUD app proves nothing anymore -- AI tools can scaffold one in two
minutes. What's scarce is systems judgment: correctness under concurrency,
behavior under real failure (node death, network partition), and proof --
not a claim -- that it's correct. This project exists to demonstrate exactly
that, end to end, with the tests to back it up.

## Architecture

```
                    ┌────────────────────┐
  client ────────▶  │  raftkv-gateway     │  (stateless; consistent-hash routing)
                    └──────────┬──────────┘
                               │ picks a shard by key
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐    ┌──────────┐      ┌──────────┐
        │  shard 0 │    │  shard 1 │ ...  │  shard N │   each shard = an
        │ (3 nodes,│    │ (3 nodes,│      │          │   independent Raft
        │  1 Raft  │    │  1 Raft  │      │          │   group (the same
        │  group)  │    │  group)  │      │          │   pattern CockroachDB/
        └──────────┘    └──────────┘      └──────────┘   TiKV use per-range
```

Within a shard, each node runs the full Raft event loop: leader election,
log replication, snapshotting, and a disk-backed WAL so a killed-and-
restarted node recovers from disk rather than rejoining empty. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design writeup,
including the three real, interesting bugs found and fixed while building
the linearizability test itself (not the easy part of this project).

## Quickstart

Requires Go 1.21+.

```bash
make build                 # builds bin/raftkv-node and bin/raftkv-gateway

make cluster                # starts a single 3-node shard on :9001-9003
# in another terminal:
curl -X PUT -d "hello" localhost:9001/kv/greeting
curl localhost:9001/kv/greeting   # -> hello
curl localhost:9002/kv/greeting   # -> hello (forwarded/replicated, same answer from any node)
```

Or run multiple shards behind the gateway -- see
[scripts/run-cluster.sh](scripts/run-cluster.sh) for a 2-shard, 6-node,
1-gateway example you can copy and extend.

## Tests

```bash
make test        # unit tests: metrics-free logic, no network, <2s
make test-race   # same, with -race
make test-all    # + integration tests: real clusters, real faults, ~30s
```

What's actually being tested, not just "does it compile":

- **`TestLeaderFailoverPreservesData` / `TestKilledNodeRecoversStateFromDiskOnRestart`**:
  kill the leader (or any node) mid-cluster, verify a new leader is elected,
  no committed data is lost, and a truly restarted node (fresh process,
  reloading from its WAL+snapshot on disk) catches back up.
- **`TestMinorityPartitionCannotCommit` / `TestMajorityPartitionContinuesServing` /
  `TestHealedPartitionReconverges`**: the actual CP-vs-AP tradeoff, made
  concrete -- an isolated minority must refuse writes, the majority must
  keep serving, and a healed partition must reconverge.
- **`TestLinearizabilityUnderFaultInjection`**: the crown jewel. Several
  concurrent clients hammer a key while the leader is killed out from under
  them mid-flight, and the full concurrent history (including the
  leadership transition) is checked against Porcupine's linearizability
  checker -- which exhaustively searches for a valid sequential ordering,
  not a spot check.

## What's deliberately out of scope (v1)

- **Dynamic membership** (adding/removing nodes from a live shard via Raft's
  joint-consensus ConfChange protocol). Membership is static, set at
  cluster bootstrap. Worth doing properly or not at all; half-implementing
  it was judged worse than documenting it as a clean v2 extension.
- **ReadIndex-optimized reads.** Reads currently go through the log as a
  no-op "barrier" to guarantee linearizability -- simple and easy to verify
  correct, at the cost of a consensus round-trip per read. etcd's actual
  ReadIndex protocol avoids this; it's a reasonable next optimization once
  the simple version is proven correct (which is what this project
  prioritizes).
- **Dynamic shard rebalancing.** Shards are fixed at gateway startup.
  Consistent hashing means adding a shard only remaps the keys that
  actually belong to it (see `internal/sharding`'s tests), but there's no
  automated data-migration step to accompany a rebalance yet.

## Project layout

```
cmd/raftkv-node/      single-replica binary
cmd/raftkv-gateway/    stateless consistent-hash router across shards
internal/
  kvstore/             the replicated state machine (a plain map + Apply)
  raftnode/            wires bare etcd/raft to disk storage, HTTP transport, the state machine
  sharding/            consistent-hash ring
  merkle/              Merkle tree + anti-entropy divergence detection
  antientropy/         periodic cross-replica integrity checker
  api/                 client-facing HTTP: GET/PUT/DELETE, leader-forwarding, status
test/integration/      real multi-node clusters, real fault injection, Porcupine
docs/ARCHITECTURE.md   full design writeup + the bugs found building this
```
