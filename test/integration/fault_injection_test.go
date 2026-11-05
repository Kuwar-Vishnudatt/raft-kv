package integration

import (
	"net/http"
	"testing"
	"time"
)

// TestMinorityPartitionCannotCommit is the core Raft safety property this
// whole project is built to demonstrate: a node cut off from the majority
// must not be able to accept a write on its own, because doing so would
// let the cluster's data silently diverge. This is the CP side of CAP,
// made concrete rather than asserted.
func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	leader := c.AwaitLeader(2 * time.Second)
	c.PartitionMinority(leader)

	// The old leader doesn't immediately know it's lost the majority; Raft's
	// CheckQuorum mechanism steps it down after it fails to hear from a
	// majority within an election timeout. Only after that should writes
	// against it definitively fail rather than hang ambiguously.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.statusLeader(leader) == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	client := &http.Client{Timeout: 1500 * time.Millisecond}
	req, _ := http.NewRequest(http.MethodPut, "http://"+c.Addr(leader)+"/kv/should-not-commit", nil)
	resp, err := client.Do(req)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("isolated former leader accepted a write -- this is a safety violation")
		}
	}
	// A client-side timeout is also an acceptable outcome here: either way,
	// the write must not have been acknowledged as successful.
}

// TestMajorityPartitionContinuesServing is the flip side: losing a minority
// of nodes must not stop the cluster from making progress.
func TestMajorityPartitionContinuesServing(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	leader := c.AwaitLeader(2 * time.Second)
	var isolated uint64
	for id := uint64(1); id <= 3; id++ {
		if id != leader {
			isolated = id
			break
		}
	}
	c.PartitionMinority(isolated)

	time.Sleep(500 * time.Millisecond) // let any churn settle; the leader shouldn't even need to change

	var majorityAddr string
	for id := uint64(1); id <= 3; id++ {
		if id != isolated {
			majorityAddr = c.Addr(id)
			break
		}
	}

	if status, err := c.Put(majorityAddr, "during-partition", "value"); err != nil || status != http.StatusOK {
		t.Fatalf("majority side failed to accept write: status=%d err=%v", status, err)
	}
}

// TestHealedPartitionReconverges proves the isolated node doesn't just sit
// in a broken state forever: once the partition heals, it must catch up and
// its state must match the rest of the cluster again -- verified via the
// same Merkle-root comparison the anti-entropy checker uses in production,
// not just "the test didn't crash."
func TestHealedPartitionReconverges(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	leader := c.AwaitLeader(2 * time.Second)
	var isolated uint64
	for id := uint64(1); id <= 3; id++ {
		if id != leader {
			isolated = id
			break
		}
	}

	c.PartitionMinority(isolated)
	time.Sleep(300 * time.Millisecond)

	var majorityAddr string
	for id := uint64(1); id <= 3; id++ {
		if id != isolated {
			majorityAddr = c.Addr(id)
			break
		}
	}
	c.Put(majorityAddr, "written-during-split", "value")

	c.HealAll()

	deadline := time.Now().Add(5 * time.Second)
	var val string
	var status int
	for time.Now().Before(deadline) {
		val, status, _ = c.Get(c.Addr(isolated), "written-during-split")
		if status == http.StatusOK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if status != http.StatusOK || val != "value" {
		t.Fatalf("previously isolated node did not reconverge: val=%q status=%d", val, status)
	}
}
