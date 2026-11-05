// Command raftkv-gateway is a stateless router in front of several
// independent raft-kv clusters ("shards"). Each shard is just an ordinary
// raft-kv cluster -- it already knows how to find and forward to its own
// leader -- so the gateway's only job is: given a key, use consistent
// hashing to pick which shard owns it, and proxy the request to any node in
// that shard (that node's own forwardToLeader logic handles the rest). This
// is the same pattern CockroachDB/TiKV use at a larger scale: one
// independent Raft group per range/shard, with routing layered on top
// rather than baked into consensus itself.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Kuwar-Vishnudatt/raft-kv/internal/sharding"
)

func main() {
	listen := flag.String("listen", ":8080", "address for the gateway to listen on")
	shardsFlag := flag.String("shards", "", "comma-separated shardID=address list, one representative node address per shard, e.g. shard0=localhost:9001,shard1=localhost:9011")
	flag.Parse()

	shardAddrs, err := parseShards(*shardsFlag)
	if err != nil {
		log.Fatalf("--shards: %v", err)
	}
	if len(shardAddrs) < 1 {
		log.Fatal("--shards must name at least one shard")
	}

	ring := sharding.NewRing(150)
	for id := range shardAddrs {
		ring.AddShard(id)
	}

	gw := &gateway{ring: ring, shardAddrs: shardAddrs, client: &http.Client{Timeout: 3 * time.Second}}
	log.Printf("gateway listening on %s, routing across %d shard(s): %v", *listen, len(shardAddrs), ring.Shards())
	if err := http.ListenAndServe(*listen, gw); err != nil {
		log.Fatal(err)
	}
}

type gateway struct {
	ring       *sharding.Ring
	shardAddrs map[string]string
	client     *http.Client
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const prefix = "/kv/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	key := r.URL.Path[len(prefix):]
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	shardID, ok := g.ring.ShardFor(key)
	if !ok {
		http.Error(w, "no shards configured", http.StatusServiceUnavailable)
		return
	}
	addr := g.shardAddrs[shardID]

	proxyReq, err := http.NewRequest(r.Method, "http://"+addr+r.URL.Path, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := g.client.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("shard %s (%s) unreachable: %v", shardID, addr, err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("X-Shard-Id", shardID)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func parseShards(s string) (map[string]string, error) {
	shards := make(map[string]string)
	if strings.TrimSpace(s) == "" {
		return shards, nil
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid shard entry %q, want shardID=host:port", part)
		}
		shards[kv[0]] = kv[1]
	}
	return shards, nil
}
