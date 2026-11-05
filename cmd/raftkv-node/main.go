// Command raftkv-node runs a single replica. A cluster is just several of
// these processes started with the same --peers list and different --id.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Kuwar-Vishnudatt/raft-kv/internal/antientropy"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/api"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/kvstore"
	"github.com/Kuwar-Vishnudatt/raft-kv/internal/raftnode"
)

func main() {
	id := flag.Uint64("id", 0, "this node's raft ID (must be > 0)")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port list for every node, e.g. 1=localhost:8081,2=localhost:8082,3=localhost:8083")
	dataDir := flag.String("data-dir", "", "directory for this node's WAL/snapshot (default ./data/node-<id>)")
	snapshotEvery := flag.Uint64("snapshot-every", 1000, "applied-entry count between snapshot+log-compactions")
	antiEntropyInterval := flag.Duration("anti-entropy-interval", 10*time.Second, "how often to compare Merkle roots with peers")
	flag.Parse()

	if *id == 0 {
		log.Fatal("--id is required and must be > 0")
	}
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("--peers: %v", err)
	}
	selfAddr, ok := peers[*id]
	if !ok {
		log.Fatalf("--id %d is not present in --peers", *id)
	}
	dir := *dataDir
	if dir == "" {
		dir = fmt.Sprintf("./data/node-%d", *id)
	}

	store := kvstore.NewStore()

	node, err := raftnode.NewRaftNode(raftnode.Config{
		ID:            *id,
		Peers:         peers,
		DataDir:       dir,
		SnapshotEvery: *snapshotEvery,
	}, store)
	if err != nil {
		log.Fatalf("failed to start raft node: %v", err)
	}
	go node.Run()

	otherPeers := make(map[uint64]string, len(peers)-1)
	for pid, addr := range peers {
		if pid != *id {
			otherPeers[pid] = addr
		}
	}
	checker := antientropy.NewChecker(store, otherPeers, *antiEntropyInterval)
	stopc := make(chan struct{})
	go checker.Run(stopc)

	server := api.NewServer(node, store, checker)
	log.Printf("node %d listening on %s (data dir: %s)", *id, selfAddr, dir)
	if err := http.ListenAndServe(selfAddr, server.Routes()); err != nil {
		log.Fatal(err)
	}
}

func parsePeers(s string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("empty peer list")
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q, want id=host:port", part)
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer id %q: %w", kv[0], err)
		}
		peers[id] = kv[1]
	}
	return peers, nil
}

var _ = os.Exit // reserved for future graceful-shutdown signal handling
