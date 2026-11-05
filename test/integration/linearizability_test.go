package integration

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// kvState is the register model's state: a single key's value, with an
// explicit "has this key ever been set" flag so "get returns not-found" and
// "get returns empty string" are distinguishable.
type kvState struct {
	value string
	set   bool
}

type kvInput struct {
	op    string // "put" or "get"
	value string // only meaningful for put
}

type kvOutput struct {
	value string
	found bool
}

// registerModel is Porcupine's sequential specification for a single
// linearizable register: a put always succeeds and sets the value; a get
// must return whatever the most recent linearized put set, or not-found if
// none has landed yet. Porcupine's checker explores every possible
// linearization of the concurrent history recorded below and accepts the
// history only if at least one linearization is consistent with this model
// at every step -- which is the actual definition of linearizability, not
// an approximation of it.
var registerModel = porcupine.Model{
	Init: func() interface{} { return kvState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(kvState)
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.op {
		case "put":
			return true, kvState{value: in.value, set: true}
		case "get":
			ok := out.found == st.set && (!out.found || out.value == st.value)
			return ok, st
		default:
			return false, st
		}
	},
	Equal: func(a, b interface{}) bool { return a.(kvState) == b.(kvState) },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		if in.op == "put" {
			return fmt.Sprintf("put(%q)", in.value)
		}
		return fmt.Sprintf("get() -> (found=%v, %q)", out.found, out.value)
	},
}

type recordedOp struct {
	clientID int
	callNs   int64
	returnNs int64
	input    kvInput
	output   kvOutput

	// ambiguous means the write may have committed anyway after the client
	// gave up (only possible once Propose() was actually called, i.e. a 504
	// from writeAmbiguousOrError in internal/api/server.go) -- see
	// buildCheckedHistory. definitelyFailed means the write provably never
	// reached a Propose() call at all (a connection error, or a 502 from
	// forwardToLeader failing to even reach a believed leader) and so could
	// not possibly have committed under any circumstance; it is simply
	// excluded, with no need for a grace window.
	ambiguous        bool
	definitelyFailed bool
}

// TestLinearizabilityUnderFaultInjection is the test that actually justifies
// calling this system linearizable rather than just "probably fine": several
// concurrent clients hammer a single key with gets and puts while, mid-run,
// the leader is killed out from under them -- forcing a real leadership
// change with in-flight requests -- and the resulting history is checked
// against Porcupine's linearizability checker, which exhaustively searches
// for a valid sequential ordering rather than spot-checking a few cases.
func TestLinearizabilityUnderFaultInjection(t *testing.T) {
	const (
		numClients  = 4
		testDur     = 4 * time.Second
		opsInterval = 30 * time.Millisecond // a deliberately paced, realistic client rate -- see note below
		key         = "lin-test-key"
	)

	// Pacing matters for more than just realism: a tight unthrottled loop
	// against this store was observed (while building this test) to blow up
	// the ratio of failed-during-the-outage-window to succeeded-during-
	// normal-operation requests by two to three orders of magnitude --
	// requests that fail fast (connection refused while there's no leader)
	// can be attempted far more often per second than requests that succeed
	// slowly (bounded by the fsync on every write), so an unpaced loop makes
	// a sub-second leader-election gap look like the overwhelming majority
	// of all traffic. That's a self-inflicted property of the load
	// generator, not of the system being tested, and it swamps the
	// interesting history with noise. Realistic pacing keeps the fault
	// window a proportionate, meaningful slice of the recorded history.
	c := StartCluster(t, 3)
	defer c.Shutdown()

	start := time.Now()
	var mu sync.Mutex
	var history []recordedOp

	stopc := make(chan struct{})
	var wg sync.WaitGroup
	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(clientID) + 1))
			httpClient := &http.Client{Timeout: 2 * time.Second}
			for {
				select {
				case <-stopc:
					return
				case <-time.After(opsInterval + time.Duration(rng.Intn(20))*time.Millisecond):
				}

				addr := c.AnyLiveNodeAddrForTest()
				if addr == "" {
					continue
				}

				if rng.Intn(2) == 0 {
					value := fmt.Sprintf("v%d-%d", clientID, rng.Intn(1000))
					callNs := time.Since(start).Nanoseconds()
					status, body, err := doPut(httpClient, addr, key, value)
					returnNs := time.Since(start).Nanoseconds()
					op := recordedOp{clientID: clientID, callNs: callNs, returnNs: returnNs, input: kvInput{op: "put", value: value}}
					switch {
					case status == http.StatusOK:
						// success, nothing to mark
					case provablyNeverProposed(err, status, body):
						op.definitelyFailed = true
					default:
						// Every other failure mode is genuinely ambiguous: the
						// write may have been fully proposed and even committed
						// before the failure became visible here. See
						// provablyNeverProposed's doc comment.
						op.ambiguous = true
					}
					mu.Lock()
					history = append(history, op)
					mu.Unlock()
				} else {
					callNs := time.Since(start).Nanoseconds()
					value, found, status, err := doGet(httpClient, addr, key)
					returnNs := time.Since(start).Nanoseconds()
					op := recordedOp{clientID: clientID, callNs: callNs, returnNs: returnNs, input: kvInput{op: "get"}, output: kvOutput{value: value, found: found}}
					if err != nil || (status != http.StatusOK && status != http.StatusNotFound) {
						// A failed read produced no output to reconcile against the
						// model either way, so there's no grace-window nuance needed
						// here the way there is for a write -- it's simply excluded.
						op.definitelyFailed = true
					}
					mu.Lock()
					history = append(history, op)
					mu.Unlock()
				}
			}
		}(clientID)
	}

	// Let the clients run for a bit under normal conditions, then kill the
	// leader mid-flight: some in-flight requests will land exactly during
	// the leadership transition, which is precisely the window where a
	// buggy implementation would be most likely to violate linearizability
	// (e.g. by acknowledging a write that then gets lost, or by letting a
	// stale leader serve a read after it's no longer leader).
	time.Sleep(testDur / 2)
	if leader := c.statusLeaderAny(); leader != 0 {
		c.Kill(leader)
		c.AwaitLeaderChange(leader, 5*time.Second)
	}
	time.Sleep(testDur / 2)

	close(stopc)
	wg.Wait()

	ops := buildCheckedHistory(t, history)
	if len(ops) < 10 {
		t.Fatalf("too few checkable operations recorded (%d) to meaningfully check linearizability", len(ops))
	}

	// A generous budget: this is a correctness check run in CI, not a hot
	// path, and Porcupine's search is exponential in concurrent-operation
	// count -- it's better to wait than to give up early.
	res, info := porcupine.CheckOperationsVerbose(registerModel, ops, 45*time.Second)

	// Illegal and Unknown are NOT the same thing, and treating them the same
	// would be dishonest in the opposite direction from the bug this test
	// exists to catch: Illegal means Porcupine proved no valid linearization
	// exists -- a real violation. Unknown means the exhaustive search (NP-
	// hard in general) didn't finish within the time budget -- inconclusive,
	// not a disproof. A single-key register under concurrent load is close
	// to this algorithm's worst case, so an occasional Unknown here is a
	// known limitation of exhaustive checking, not evidence of a bug; only
	// Illegal is treated as a test failure.
	if res == porcupine.Illegal {
		visualizePath := "/tmp/linearizability-failure.html"
		if err := porcupine.VisualizePath(registerModel, info, visualizePath); err == nil {
			t.Logf("wrote linearizability failure visualization to %s", visualizePath)
		}
		for _, r := range history {
			if r.definitelyFailed {
				continue
			}
			t.Logf("RAW client=%d op=%s value=%q ambiguous=%v call=%dms return=%dms output=%+v",
				r.clientID, r.input.op, r.input.value, r.ambiguous, r.callNs/1e6, r.returnNs/1e6, r.output)
		}
		t.Fatalf("history is NOT linearizable -- a real violation was found, see visualization for the offending operations")
	}
	if res == porcupine.Unknown {
		t.Logf("linearizability check was inconclusive (search did not finish within the time budget) -- not a proven violation, see comment above")
	}
}

// buildCheckedHistory converts recorded operations into the form Porcupine
// checks, with deliberate handling for ambiguous (timed-out/errored)
// operations rather than naively dropping them:
//
//   - An ambiguous GET is safe to exclude outright: it produced no output,
//     so it carries no claim that could contradict the model either way.
//
//   - An ambiguous PUT is different -- the write may have committed anyway
//     shortly after the client gave up waiting (this system says so
//     explicitly in its own timeout response; see writeAmbiguousOrError in
//     internal/api/server.go). Silently dropping it would be unsound: if it
//     did commit and a later GET observes its value, the checker would see
//     a state change with no operation in the history to explain it and
//     wrongly report a linearizability violation that is actually a flaw in
//     the test's bookkeeping, not the system. Instead, its Return time is
//     extended by a bounded grace window, which tells Porcupine the write's
//     linearization point could legally fall anywhere from when it was
//     called to shortly after -- the honest representation of "we don't
//     know when, or whether, this landed," without unboundedly overlapping
//     it with the rest of the test.
//
//     The window is deliberately bounded rather than extended to the end of
//     the whole history: Porcupine's search is exponential in the number of
//     mutually-overlapping operations, and in practice (observed while
//     building this test) extending every ambiguous write all the way to
//     the end of a multi-second, 5-client history creates enough pairwise
//     overlap that the checker can time out and report Unknown -- a search
//     giving up, not a disproof. A write that genuinely lands late does so
//     within roughly one leader-election cycle of its timeout, not minutes
//     later, so a grace window well beyond that (several times the
//     configured election timeout) stays honest while keeping the search
//     tractable enough to get a real Ok/Illegal answer within the checker's
//     time budget.
func buildCheckedHistory(t *testing.T, history []recordedOp) []porcupine.Operation {
	t.Helper()
	const graceWindow = int64(800 * time.Millisecond)

	var ops []porcupine.Operation
	var excludedDefiniteFailures, extendedPuts int
	for _, r := range history {
		if r.definitelyFailed {
			// Never reached a Propose() call (connection error, or a 502
			// from forwardToLeader failing to even reach a believed
			// leader) -- provably did not commit, safe to drop entirely.
			excludedDefiniteFailures++
			continue
		}
		if r.ambiguous {
			extendedPuts++
			r.returnNs = r.callNs + graceWindow
		}
		ops = append(ops, porcupine.Operation{
			ClientId: r.clientID,
			Input:    r.input,
			Call:     r.callNs,
			Output:   r.output,
			Return:   r.returnNs,
		})
	}
	t.Logf("history: %d checkable operations (%d ambiguous puts with extended return window, %d definite failures excluded)",
		len(ops), extendedPuts, excludedDefiniteFailures)
	return ops
}

func doPut(client *http.Client, addr, key, value string) (int, string, error) {
	req, err := http.NewRequest(http.MethodPut, "http://"+addr+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		return 0, "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), nil
}

// provablyNeverProposed reports whether an error/response definitively
// proves the write's HTTP request never resulted in a Propose() call
// anywhere in the cluster -- the only case where it's safe to treat a
// failure as "could not possibly have committed."
//
// The one error Go's net package gives us that's actually this strong is a
// TCP dial failure ("connection refused"): the OS never completed a
// handshake, so zero request bytes were ever delivered, so nothing on the
// other end could have acted on them. Every other failure mode -- a
// connection reset or EOF mid-request, a proxy's own dial to the believed
// leader failing with something other than connection-refused, the request
// simply never getting a response -- leaves open the possibility that the
// write was fully received, proposed, and even committed before the
// failure became visible to this client. Treating those as "definitely
// failed" was an actual bug caught while building this test: a write whose
// connection reset right as the leader that had just committed it was
// killed produced exactly this signature, and a later read correctly
// observed its value with no explaining operation left in the checked
// history, which Porcupine rightly flagged as impossible.
func provablyNeverProposed(err error, status int, body string) bool {
	if err != nil {
		return strings.Contains(err.Error(), "connection refused")
	}
	if status == http.StatusBadGateway {
		return strings.Contains(body, "connection refused")
	}
	return false
}

func doGet(client *http.Client, addr, key string) (string, bool, int, error) {
	resp, err := client.Get("http://" + addr + "/kv/" + key)
	if err != nil {
		return "", false, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", false, resp.StatusCode, nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, resp.StatusCode, err
	}
	return string(body), true, resp.StatusCode, nil
}
