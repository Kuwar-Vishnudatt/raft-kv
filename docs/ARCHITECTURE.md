# Architecture

## Consensus: bare etcd/raft, everything else hand-built

`go.etcd.io/raft/v3` is used for the consensus algorithm itself (leader
election, log replication, safety) -- re-implementing Raft from scratch is
the single easiest way to introduce a subtle, hard-to-find safety bug in a
project whose entire point is correctness. What etcd's library deliberately
does *not* provide is any I/O: no storage, no network transport, no state
machine. Those are this project's own code:

- **`internal/raftnode/diskstorage.go`**: a `raft.Storage` implementation
  that mirrors every write to a checksummed WAL file and a snapshot file on
  disk, replaying them on startup. `raft.MemoryStorage` serves all reads
  (it already gets the index arithmetic right); disk is only ever written
  to, never read from except at startup.
- **`internal/raftnode/transport.go`**: a plain HTTP transport between
  peers. `Send` is fire-and-forget per message (Raft's protocol already
  tolerates and retransmits on message loss), and it exposes
  `Partition`/`Heal` methods that drop messages to/from a given peer --
  fault injection at the transport layer rather than via OS-level
  `tc`/`iptables` rules, so the exact same test code runs on any developer's
  machine or CI without root.
- **`internal/raftnode/node.go`**: the event loop (`Run`) that ties it
  together -- the standard `Tick()` / `Ready()` / `Advance()` pattern, plus
  proposal tracking (so an HTTP handler can block until its specific write
  is actually committed and applied, not just accepted) and periodic
  snapshotting.
- **`internal/kvstore/store.go`**: the state machine. A plain
  `map[string]string` behind a mutex. It has no idea Raft exists; it just
  applies commands in the order it's given them, which is the entire point
  of state-machine replication.

## Reads are linearizable via a log barrier, not ReadIndex

A read proposes a no-op (`kvstore.OpNoop`) through the log and waits for it
to be applied before touching local state. Once that no-op has applied,
every write ordered before it in the log has also applied, so the
subsequent local read reflects a fully up-to-date, linearizable view. This
is the simplest possible implementation that is *obviously* correct, at the
cost of paying a full consensus round-trip per read.

etcd's own `ReadIndex` protocol avoids that round-trip (the leader confirms
it's still leader via a heartbeat round and then serves straight from local
state once applied-index catches up to the confirmed commit-index), and
would be the natural next optimization. It's not implemented here
deliberately: get the simple, provably-correct version working and tested
first, optimize second. The linearizability test (below) depends on nothing
about the read path being clever, which is exactly the point.

## Static membership, by design

Raft supports adding/removing voters from a live cluster via joint
consensus (`ConfChangeV2`) -- a real, separate protocol layered on top of
the base algorithm. This project scopes it out entirely rather than
half-implement it: every node is started knowing the complete, fixed peer
set. `ApplyConfChange` is wired up (so a `ConfChange` entry in the log
wouldn't crash the apply loop) but nothing in this codebase ever proposes
one. A clean, explicit "not supported" beats a partially-correct
implementation of a genuinely subtle protocol.

## Sharding: consistent hashing across independent Raft groups

Each shard is a complete, independent Raft cluster -- the same code as a
single-shard deployment, just N of them. `internal/sharding`'s consistent-
hash ring (with virtual nodes, per `consistent_hash_test.go`) decides which
shard owns a key, and `cmd/raftkv-gateway` is a stateless HTTP proxy that
looks up the right shard and forwards -- the shard's own nodes already know
how to find their leader, so the gateway doesn't need to. This is the same
architecture CockroachDB and TiKV use at a larger scale (one Raft group per
key-range), just without their automatic range-splitting and rebalancing.

## Merkle-tree anti-entropy: a safety net, not a consistency mechanism

`internal/merkle` builds a Merkle tree over a replica's key-value state and
`internal/antientropy` periodically compares root hashes with peers. This
is worth being precise about: in a correctly functioning Raft cluster,
replicas are *guaranteed* identical once they've applied the same log
index, by the consensus protocol itself. This is not a Dynamo-style system
where anti-entropy *is* how consistency gets reached. Here, it exists purely
as a defense-in-depth integrity checker -- catching a bit flip on disk, a
bug in `Apply()`, or an operator manually editing a data file, none of
which Raft's own guarantees protect against. It reports divergence; it does
not attempt to auto-repair it, because in a strongly-consistent system,
detected divergence is itself a bug to fix, not a routine condition to
paper over.

## The real work: testing this honestly

Writing the Raft wiring was the predictable part. Getting a *trustworthy*
linearizability test was not, and surfaced three genuine bugs -- two in the
system, one in the test itself, the last one arguably the most important
lesson of the whole project.

### Bug 1: tick starvation under sustained load

The main event loop's `select` chose between `<-ticker.C` (drive Raft's
internal election/heartbeat clock) and `<-rn.node.Ready()` (process
replication work). `time.Ticker` only ever buffers one pending tick -- if
`Ready()` has a continuous backlog (sustained high write throughput), a
busy loop can let elapsed ticks get silently dropped instead of queued,
desyncing Raft's internal clock from wall-clock time. Under load is exactly
when failure detection should stay prompt, not get slower. Fixed by
non-blockingly draining any pending tick at the top of every loop iteration
before deciding what to do next, bounding the delay to about one iteration
instead of leaving it to chance which case Go's `select` resolves.

### Bug 2: an unconditional forward created a self-inflicted overload signature

Not a correctness bug, but a sharp lesson in cause and effect: an early
version of the linearizability test ran clients in a tight, unthrottled
loop. During the ~1-second leader-election gap, requests failed *fast*
(immediate connection-refused against the dead leader); during normal
operation, requests succeeded *slowly* (bounded by the WAL's fsync on every
write). A loop with no pacing can therefore attempt orders of magnitude more
operations during the bad second than during the healthy ones, making a
brief, correct, bounded outage look like 90%+ of all recorded traffic. That
skew is a property of the load generator, not the system under test --
fixed by pacing the test's synthetic clients to a realistic rate, which
also happens to be requirement for getting Porcupine's checker (exponential
in concurrent-operation count) to return a verdict instead of timing out.

### Bug 3: not every connection error means "never committed" -- the important one

The linearizability test initially classified *any* non-200 response (a
Go HTTP client error, or any non-`200` status) as proof the write never
reached a `Propose()` call anywhere, and excluded it from the checked
history outright. This is wrong, and it is wrong in a way that's easy to
miss: a **"connection refused"** error does prove nothing was ever
delivered (the OS never completed a TCP handshake). But a connection
**reset mid-request**, or any other transport error, proves nothing of the
kind -- the request may have been fully received, proposed, and even
*committed* before the response made it back. Concretely: the test's leader
kill can land in the exact instant between the leader committing a write
and flushing the HTTP response, producing precisely that signature on the
client side.

Excluding such an operation from the checked history is unsound: if the
write really did commit and a later read observes its value, Porcupine
correctly reports a linearizability violation -- except the violation is in
the test's bookkeeping, not the system. This happened in practice: a
`get()` reliably observed a value with zero matching `put()` anywhere in
the (wrongly pruned) history, which is indistinguishable from "the system
hallucinated a value" unless you know to suspect the test. The fix was
precise rather than blanket: only a dial-level `connection refused` (and a
502 whose embedded cause is also `connection refused`) is treated as
provably-never-proposed; every other failure mode is treated as genuinely
ambiguous, given a bounded grace-window extension on its `Return` time, and
left for Porcupine to reason about rather than the test silently deciding
on the system's behalf. See `provablyNeverProposed` and
`buildCheckedHistory` in `test/integration/linearizability_test.go` for the
full reasoning in context.

The broader point: a linearizability *test* is itself nontrivial systems
code, with its own failure modes, and treating its first "found a bug!"
result as ground truth without checking whether the bug is in the system or
in the harness is exactly the mistake worth not making.
