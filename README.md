# raft-kv

**A sharded, linearizable distributed key-value store, built from the consensus protocol up — with a real correctness test to prove it, not just claim it.**

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## What is this?

raft-kv is a key-value store (think: a very small, distributed version of Redis — `PUT key value`, `GET key`, `DELETE key`) that stores its data on multiple machines at once for durability, and splits its data across multiple independent clusters ("shards") for scale. It's written in Go.

The headline property is **linearizability**: every client, no matter which replica it talks to, sees a single, globally-agreed-upon order of operations, as if there were only one copy of the data and one clock. That guarantee survives the things that actually break distributed systems in production: a leader node dying mid-write, a network partition splitting the cluster in two, a node crashing and restarting with a cold disk. Most of this project's effort went into *proving* that guarantee holds under exactly those conditions, using [Porcupine](https://github.com/anishathalye/porcupine), a real automated linearizability checker — not into the comparatively mechanical work of wiring up the consensus library.

Consensus (agreeing on the order of operations across replicas, even when some of them are down or unreachable) is handled by `go.etcd.io/raft/v3`, the same Raft implementation etcd and several production databases use. Everything around it — the disk-backed write-ahead log, the HTTP transport, the state machine, the sharding layer, the anti-entropy integrity checker, and the fault-injection test harness — is this project's own code.

## Why this exists

A plain CRUD app proves nothing anymore — AI coding tools can scaffold a REST API over a database in minutes. That has quietly devalued the skill most "full-stack" portfolios demonstrate. What it hasn't commoditized is **systems judgment**: knowing which consistency model a problem actually needs, correctly reasoning about what happens when a leader dies in the middle of a write, and — critically — being able to *prove* a system is correct under failure instead of just asserting it in a README.

That last part is the differentiator this project is built around. It would have been easy to build the happy path (single node, no faults, manual `curl` testing) and call it done. Instead, this project:

- Runs real multi-node clusters in its test suite, not mocks.
- Actually kills leaders, partitions networks, and restarts processes mid-test — using fault injection built into the transport layer, not OS-level `tc`/`iptables` tricks.
- Checks the resulting history against Porcupine, which exhaustively searches for *any* valid sequential ordering of a concurrent history and fails loudly if none exists — the actual mathematical definition of linearizability, not a spot-check.
- Found three real bugs doing this (two in the system, one in the test itself) and fixed all three with root-cause understanding, not by papering over symptoms. Their full stories are below — that's the most honest signal of engineering maturity this README has to offer.

If you're evaluating this as a hiring signal: the interesting part isn't that it has Raft in it. It's that it was tested in a way that actually would have caught a real correctness bug, and that when the testing surfaced something confusing, the response was "is the bug in my system or in my test?" rather than declaring victory.

## How it works

This section walks through what actually happens, step by step, when the system is used — the goal is that you can trace a request through the whole system in your head after reading it once.

### The pieces in play

- **Clients** talk HTTP to either a single node directly, or to the **gateway** (`cmd/raftkv-gateway`) if the deployment is sharded.
- A **shard** is a complete, independent Raft cluster — typically 3 nodes (`cmd/raftkv-node`), each running the full Raft protocol (leader election + log replication) plus this project's own disk storage, HTTP transport, and state machine.
- A **gateway** is a stateless process that sits in front of multiple shards and routes each request to the shard that owns the key, using **consistent hashing** (a hashing scheme where each shard owns a contiguous arc of a hash ring, so adding or removing a shard only reshuffles the keys on either side of that arc, instead of remapping almost every key the way a plain `hash(key) % N` would).

### Writing a key

1. A client sends `PUT /kv/mykey` with a body, either straight to a node or via the gateway.
2. **If it went through the gateway**: the gateway hashes `mykey`, walks the consistent-hash ring to find which shard owns it (`internal/sharding`), and forwards the raw HTTP request to any node in that shard — it doesn't need to know or care which node in the shard is the leader.
3. **Whichever node receives it** checks whether it's currently the Raft leader for its shard. If not, it transparently proxies the request to the node it believes *is* the leader (`forwardToLeader` in `internal/api/server.go`) and returns whatever that node answers — the client never needs its own leader-tracking logic.
4. **The leader** wraps the write as a `Command` (`internal/kvstore`) inside an envelope carrying a unique proposal ID, and calls `raft.Node.Propose()`. This hands the command to etcd/raft's internal state machine, which is now responsible for replicating it to a majority of the shard's nodes before it can be considered committed.
5. **Replication happens inside the Raft event loop** (`internal/raftnode/node.go`): the leader's loop produces a `Ready()` batch containing the new log entry and outbound messages to followers. The entry is appended to a **checksummed write-ahead log (WAL)** on disk (`internal/raftnode/diskstorage.go`) before anything else happens — durability comes first. The messages go out over a plain HTTP transport (`internal/raftnode/transport.go`) to the other nodes in the shard.
6. **Followers** persist the entry to their own disk-backed WAL and acknowledge it. Once the leader sees acknowledgments from a majority of the shard (itself included), Raft considers the entry **committed** — this is the point at which the write is permanent and cannot be un-done by any sequence of future failures, as long as a majority of nodes survive.
7. Every node (leader and followers alike) then **applies** the committed entry to its local state machine — in this case, a plain `map[string]string` (`internal/kvstore/store.go`) — in the exact same order. Because every replica applies the same commands in the same order, they all end up in the same state; this is the core idea behind Raft and consensus-replicated systems in general ("state machine replication").
8. The leader's original HTTP handler, which has been blocked waiting since step 4, is woken up once its specific proposal ID is applied, and returns `200 OK` to the client (or to the gateway, which relays it back).

If the request times out while waiting for commit (for example, the leader died mid-flight), the server deliberately returns a distinct "ambiguous" response (`writeAmbiguousOrError`) rather than a plain failure — the write might have actually committed a moment later. Callers are expected to treat that the way they'd treat a timeout on a payment API: safe to retry only if the write is idempotent. This distinction — "definitely did not happen" vs. "genuinely unknown" — turns out to matter a great deal; it's the subject of the most interesting bug below.

### Reading a key

A naive read would just check the local map and return whatever's there — but on a follower that's lagging slightly behind the leader, that could return stale data, breaking linearizability. To avoid that, every read is itself **routed through the Raft log** as a no-op "barrier" command (`LinearizableRead` in `internal/raftnode/node.go`): the node proposes a no-op, waits for it to commit and apply, and only then reads the local map. Because the no-op can only apply after every write that was already ordered ahead of it in the log has also applied, the subsequent read is guaranteed to reflect every write that any client could have observed as committed — which is exactly what linearizability requires.

This is the simplest possible way to implement a linearizable read, and it's easy to convince yourself it's correct — the cost is that every read pays a full consensus round-trip, the same as a write. (etcd's real `ReadIndex` protocol avoids that cost; see "what this doesn't do yet" below for why it's not implemented here.)

### Recovering from a crash

Each node's Raft storage (`internal/raftnode/diskstorage.go`) mirrors every log entry to a checksummed WAL file on disk, plus periodic snapshots of the full key-value state (`internal/raftnode/node.go`'s `maybeSnapshot`, which also compacts the log so it doesn't grow forever). If a node's process is killed and restarted, it reopens its data directory, replays its WAL and snapshot from disk, and rejoins the cluster already caught up — rather than starting empty and having to replay the *entire* cluster history from its peers over the network.

### Anti-entropy: a safety net, not a consistency mechanism

It's worth being precise about what the **Merkle-tree anti-entropy** checker (`internal/merkle`, `internal/antientropy`) is and isn't for. In a correctly functioning Raft cluster, two replicas that have applied the same log index are *guaranteed* to hold identical state by the consensus protocol itself — this is not a Dynamo-style eventually-consistent system where background repair is how replicas ever become consistent in the first place.

Here, each node periodically builds a Merkle tree (a tree of hashes where every parent hash is derived from its children, so comparing two trees' root hashes tells you in one comparison whether the underlying data is identical, without transferring or diffing the data itself) over its own key-value state, and compares root hashes with its peers (`internal/antientropy/checker.go`). If the roots match, nothing happens. If they don't, it walks down to find exactly which keys diverge and records that for an operator to see. It deliberately does **not** attempt to auto-repair the divergence: in a system whose whole premise is strong consistency, a detected divergence is a bug (a bit flip on disk, a non-deterministic bug in `Apply()`, someone hand-editing a data file) that needs fixing, not a routine condition to quietly paper over.

### Putting it together: a sharded deployment

A verified example from this project: **2 shards, 3 nodes each (6 nodes total), plus 1 gateway** — a key's journey looks like client → gateway (hashes the key, picks shard 1) → any node in shard 1 → forwarded to shard 1's current leader → proposed → replicated to a majority of shard 1's 3 nodes → applied → acknowledged back up the chain. Shard 0 is a completely separate Raft group that neither knows nor cares about any of this. This is the same per-range-Raft-group pattern CockroachDB and TiKV use in production, at a larger scale and with automatic rebalancing this project doesn't attempt (see below).

## Architecture at a glance

```
cmd/raftkv-node/        the single-replica binary — run 3+ of these per shard
cmd/raftkv-gateway/     stateless consistent-hash router sitting in front of multiple shards

internal/
  kvstore/              the replicated state machine: a plain map + Apply() — has no idea Raft exists
  raftnode/              wires bare etcd/raft to disk storage, HTTP transport, and the state machine:
                          - node.go: the Tick()/Ready()/Advance() event loop, proposal tracking
                          - diskstorage.go: checksummed WAL + snapshots, raft.Storage implementation
                          - transport.go: peer-to-peer HTTP, with Partition()/Heal() fault injection
                          - walfile.go: the on-disk log file format
  sharding/              consistent-hash ring with virtual nodes (internal/sharding/consistent_hash.go)
  merkle/                Merkle tree construction + divergent-key diffing
  antientropy/           periodic cross-replica root-hash comparison using the merkle package
  api/                   client-facing HTTP: GET/PUT/DELETE, leader-forwarding, /status, test-only
                          fault-injection endpoints (/internal/partition, /internal/heal)

test/integration/       real multi-node clusters, real fault injection, Porcupine-checked linearizability
docs/ARCHITECTURE.md    deeper design rationale and the full bug writeups, if you want more detail
scripts/run-cluster.sh  one-command local 3-node cluster for manual poking
```

## Real bugs found while building this

Wiring up the Raft library was the predictable part — etcd/raft handles the hard consensus math, this project just has to drive its event loop and give it storage and a network. Building a *trustworthy* test of the result was not predictable, and it surfaced three real bugs: two in the system, and one — arguably the most important lesson of the whole project — in the test itself.

### Bug 1: tick starvation under load

**What broke.** Raft's internal clock (election timeouts, heartbeats) advances by the node calling `Tick()` on a regular interval — in this project, every 100ms via a `time.Ticker`. The event loop's main `select` statement chose between draining a tick (`<-ticker.C`) and processing replication work (`<-rn.node.Ready()`). `time.Ticker` only ever buffers a single pending tick internally — if the channel isn't drained in time, the next tick simply overwrites it and the elapsed time is lost, not queued.

**How it was noticed.** Under sustained high write throughput, `Ready()` has a continuous backlog of replication work to process, so Go's `select` can keep resolving to that branch and never get around to draining pending ticks. The result: Raft's internal notion of elapsed time silently falls behind wall-clock time, specifically *under load* — which is exactly the condition where failure detection (election timeouts noticing a dead leader) needs to stay prompt, not get slower.

**The fix.** A non-blocking drain of any already-pending tick was added at the very top of every loop iteration, before the main `select` even runs, so an elapsed tick can never lose a priority contest against a backlog of ready work. This bounds the worst-case delay to roughly one loop iteration instead of leaving it to chance which case Go's `select` happens to pick among several simultaneously-ready channels.

**Why it mattered.** This is the kind of bug that's invisible in a light-load demo and only shows up once you're actually generating the kind of sustained write throughput a real correctness test needs — which is itself a small lesson: testing a distributed system properly tends to surface different bugs than testing it lightly does.

### Bug 2: a self-inflicted overload that looked like a system failure

**What broke.** This wasn't a bug in the system at all — it was a bug in how the test measured the system, which is worth telling honestly because misreading your own benchmark is a very common real-world mistake. An early version of the linearizability test ran its simulated clients in a tight, unthrottled loop with no pacing between requests.

**How it was noticed.** During normal operation, each request took a bounded but real amount of time (dominated by the WAL's `fsync` on every write). During the roughly one-second gap while a new leader was being elected (after the test deliberately killed the old one), requests instead failed *immediately* — a fast connection-refused against the now-dead leader, with no fsync or consensus round-trip involved. An unthrottled client loop can therefore cycle through orders of magnitude more attempts during that one bad second than it does during any healthy second, because failing fast is cheap. The result was a recorded history where the brief, correctly-bounded outage dominated 90%+ of all recorded operations — making a working system look, in the raw numbers, like it was failing almost all the time.

**The fix.** The test's synthetic clients were paced to a fixed, realistic interval between requests (`opsInterval` in `test/integration/linearizability_test.go`) instead of firing as fast as possible. This happens to pay for itself twice over: besides fixing the skewed numbers, it also keeps the total operation count low enough for Porcupine's checker — which is exponential in the number of concurrent operations — to actually return a verdict instead of timing out.

**Why it mattered.** A load generator with no pacing doesn't measure the system under a realistic load; it measures how fast the system can fail, and conflates the two. The lesson generalizes well past this project: before trusting a scary-looking test result, check whether the thing you're measuring is the system, or your own measurement apparatus.

### Bug 3: misclassifying an ambiguous failure as a definite one (the important one)

**What broke.** The linearizability test needs to decide, for every failed request, whether to include it in the history Porcupine checks. The earlier version of this logic treated *any* non-200 response as proof the write had never been proposed anywhere, and dropped it from the checked history entirely on that assumption.

That assumption is wrong, and wrong in a subtle way. A **connection-refused** error (the OS never even completed a TCP handshake) really does prove nothing was delivered — that one's safe to exclude. But a connection **reset mid-request**, or any other transport-level error, proves nothing of the kind: the request may have been fully received, proposed, and even *committed* on the server side before the failure became visible to the client. Concretely, this project's fault injection kills the leader at a random moment — including, sometimes, the exact instant between the leader committing a write and finishing the flush of its HTTP response. From the client's point of view that looks identical to a request that failed outright; on the server side, the write already happened.

**How it was noticed.** With the old (wrong) exclusion logic, a `get()` in the test reliably came back with a value that had no matching `put()` anywhere in the checked history — because the `put()` that actually set it had been incorrectly pruned out as "never proposed." Porcupine, working correctly, flagged this as a linearizability violation: a read returning a value no write in the history ever produced is indistinguishable, from the checker's point of view, from the system fabricating data out of nowhere. Taken at face value, this looked like a genuine correctness bug in the Raft wiring.

**The fix.** The classification logic (`provablyNeverProposed` in `test/integration/linearizability_test.go`) was rewritten to be precise instead of blanket: only a dial-level "connection refused" (or a proxy's 502 whose underlying cause is also connection-refused) is treated as provably-never-committed and excluded outright. Every other failure mode is instead treated as genuinely **ambiguous** — given a bounded grace-window extension on its recorded return time — and left in the history for Porcupine itself to reason about, rather than having the test silently pre-decide the system's behavior on the client's behalf.

**Why it mattered.** This is the single most valuable lesson of the project. The system was correct the entire time; the bug was in the test's bookkeeping about what counts as proof. A linearizability test is itself nontrivial distributed-systems code, with its own failure modes — and treating its first "found a violation!" result as ground truth, without first asking whether the bug might be in the harness rather than the system, is exactly the mistake that's worth not making. Distinguishing "definitely didn't happen" from "I genuinely don't know" turns out to be load-bearing in two completely different places in this codebase: the production API (`writeAmbiguousOrError`) and the test that verifies it (`provablyNeverProposed`) — which is itself a sign the distinction is a real property of the problem, not an accident of one piece of code.

## Try it yourself

Requires Go 1.21+.

```bash
make build        # builds bin/raftkv-node and bin/raftkv-gateway
make cluster       # starts a single 3-node shard on :9001-9003, foreground (Ctrl-C to stop)

# in another terminal:
curl -X PUT -d "hello" localhost:9001/kv/greeting
curl localhost:9001/kv/greeting   # -> hello
curl localhost:9002/kv/greeting   # -> hello, served by a different node, same answer
```

To try sharded routing: start a second 3-node shard on different ports (copy `scripts/run-cluster.sh`, change the ports and `--data-dir`), then point the gateway at one node address per shard:

```bash
./bin/raftkv-gateway --shards=shard0=localhost:9001,shard1=localhost:9011
curl -X PUT -d "v1" localhost:8080/kv/somekey   # gateway picks a shard by consistent hashing and forwards
```

Running the tests:

```bash
make test        # unit tests: pure logic, no network, <2s
make test-race   # same, with Go's race detector
make test-all    # + integration tests: real multi-node clusters, real fault injection, ~30s
```

What's actually exercised, not just "does it compile":

- **`TestLeaderFailoverPreservesData` / `TestKilledNodeRecoversStateFromDiskOnRestart`** — kill the leader (or any node) mid-cluster, confirm a new leader is elected with no committed data lost, and confirm a truly restarted process (fresh binary, reloading WAL + snapshot from disk) catches back up correctly.
- **`TestMinorityPartitionCannotCommit` / `TestMajorityPartitionContinuesServing` / `TestHealedPartitionReconverges`** — the CP-vs-AP tradeoff made concrete: an isolated minority of nodes must refuse to accept writes (it can't reach consensus), the surviving majority must keep serving normally, and a healed partition must reconverge to one consistent state.
- **`TestLinearizabilityUnderFaultInjection`** — the one described at length above: several concurrent clients hammering a key while the leader is killed mid-flight, checked end-to-end against Porcupine's linearizability checker.

## What this deliberately doesn't do (yet)

- **Dynamic cluster membership.** Raft supports adding/removing nodes from a live shard via its joint-consensus protocol (`ConfChangeV2`) — a substantial additional protocol layered on top of the base algorithm. This project scopes it out entirely: membership is static, fixed at cluster bootstrap (`ApplyConfChange` is wired up so a conf-change log entry wouldn't crash the apply loop, but nothing in this codebase ever proposes one). A clean, explicit "not supported" was judged better than a half-correct implementation of a genuinely subtle protocol.
- **ReadIndex-optimized reads.** Reads currently pay a full consensus round-trip by going through the log as a no-op barrier (see "How it works" above). etcd's own `ReadIndex` protocol gets the same linearizability guarantee without that round-trip, by having the leader confirm its leadership via a heartbeat round and then serving straight from local state. It's a reasonable next optimization — deliberately not implemented yet, on the principle of getting the simple, obviously-correct version working and proven first.
- **Dynamic shard rebalancing.** Shards are fixed at gateway startup. Consistent hashing means *adding* a shard would only remap the minority of keys that land on its new slice of the ring (see `internal/sharding/consistent_hash_test.go`), but there's no automated data-migration step built to actually move those keys to the new shard yet — today, adding a shard to the ring without migrating data would just mean some keys briefly point at an empty shard.

## Project layout

```
cmd/raftkv-node/        single-replica binary
cmd/raftkv-gateway/     stateless consistent-hash router across shards
internal/
  kvstore/              the replicated state machine (a plain map + Apply)
  raftnode/             wires bare etcd/raft to disk storage, HTTP transport, the state machine
  sharding/             consistent-hash ring
  merkle/               Merkle tree + anti-entropy divergence detection
  antientropy/          periodic cross-replica integrity checker
  api/                  client-facing HTTP: GET/PUT/DELETE, leader-forwarding, status
test/integration/       real multi-node clusters, real fault injection, Porcupine
docs/ARCHITECTURE.md    full design writeup + the bugs found building this
```

---

[MIT licensed](LICENSE). Repo: [github.com/Kuwar-Vishnudatt/raft-kv](https://github.com/Kuwar-Vishnudatt/raft-kv)
