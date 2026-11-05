package integration

import (
	"net/http"
	"testing"
	"time"
)

func TestBasicPutGet(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	addr := c.AnyNodeAddr()
	if status, err := c.Put(addr, "foo", "bar"); err != nil || status != http.StatusOK {
		t.Fatalf("put failed: status=%d err=%v", status, err)
	}

	val, status, err := c.Get(addr, "foo")
	if err != nil || status != http.StatusOK {
		t.Fatalf("get failed: status=%d err=%v", status, err)
	}
	if val != "bar" {
		t.Fatalf("got %q, want %q", val, "bar")
	}
}

func TestGetMissingKeyIs404(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	_, status, err := c.Get(c.AnyNodeAddr(), "does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusNotFound {
		t.Fatalf("got status %d, want 404", status)
	}
}

func TestWriteIsReadableFromEveryNode(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	if status, _ := c.Put(c.AnyNodeAddr(), "replicated", "value"); status != http.StatusOK {
		t.Fatalf("put failed with status %d", status)
	}

	for id := uint64(1); id <= 3; id++ {
		val, status, err := c.Get(c.Addr(id), "replicated")
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		if status != http.StatusOK || val != "value" {
			t.Fatalf("node %d: got (%d, %q), want (200, %q)", id, status, val, "value")
		}
	}
}

func TestDelete(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	addr := c.AnyNodeAddr()
	c.Put(addr, "temp", "value")

	req, _ := http.NewRequest(http.MethodDelete, "http://"+addr+"/kv/temp", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete failed with status %d", resp.StatusCode)
	}

	_, status, _ := c.Get(addr, "temp")
	if status != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", status)
	}
}

func TestLeaderFailoverPreservesData(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	c.Put(c.AnyNodeAddr(), "before-failover", "still-here")
	leader := c.AwaitLeader(2 * time.Second)

	c.Kill(leader)
	c.AwaitLeaderChange(leader, 5*time.Second) // blocks until a surviving node takes over

	var survivingAddr string
	for id := uint64(1); id <= 3; id++ {
		if id != leader {
			survivingAddr = c.Addr(id)
			break
		}
	}

	val, status, err := c.Get(survivingAddr, "before-failover")
	if err != nil || status != http.StatusOK || val != "still-here" {
		t.Fatalf("data lost after failover: val=%q status=%d err=%v", val, status, err)
	}

	if status, _ := c.Put(survivingAddr, "after-failover", "new-value"); status != http.StatusOK {
		t.Fatalf("cluster did not accept writes after failover, status=%d", status)
	}
}

func TestKilledNodeRecoversStateFromDiskOnRestart(t *testing.T) {
	c := StartCluster(t, 3)
	defer c.Shutdown()

	c.Put(c.AnyNodeAddr(), "persisted", "value")
	time.Sleep(200 * time.Millisecond) // let the write fully replicate before we kill anyone

	// Kill a follower (not the leader) so the cluster keeps running normally
	// while it's down, then bring it back and prove it catches up from disk
	// rather than rejoining as if it had no history.
	leader := c.AwaitLeader(2 * time.Second)
	var follower uint64
	for id := uint64(1); id <= 3; id++ {
		if id != leader {
			follower = id
			break
		}
	}

	c.Kill(follower)
	c.Put(c.AnyNodeAddr(), "written-while-dead", "other-value")
	c.Restart(follower)

	deadline := time.Now().Add(5 * time.Second)
	var val string
	var status int
	for time.Now().Before(deadline) {
		val, status, _ = c.Get(c.Addr(follower), "written-while-dead")
		if status == http.StatusOK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if status != http.StatusOK || val != "other-value" {
		t.Fatalf("restarted node did not catch up: val=%q status=%d", val, status)
	}

	// And the data from before it ever went down should still be there too.
	val, status, _ = c.Get(c.Addr(follower), "persisted")
	if status != http.StatusOK || val != "value" {
		t.Fatalf("restarted node lost pre-crash data: val=%q status=%d", val, status)
	}
}
