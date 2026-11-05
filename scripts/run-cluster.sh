#!/usr/bin/env bash
# Starts a single 3-node shard on localhost:9001-9003 and leaves it running
# in the foreground (Ctrl-C to stop everything). Data is written under
# ./data/, which `make clean` removes.
#
# To try multi-shard routing: start a second shard on different ports (copy
# this script, change the port numbers and --data-dir), then run
# raftkv-gateway with --shards pointing at one node address per shard, e.g.:
#   ./bin/raftkv-gateway --shards=shard0=localhost:9001,shard1=localhost:9011

set -euo pipefail
cd "$(dirname "$0")/.."

PEERS="1=localhost:9001,2=localhost:9002,3=localhost:9003"
mkdir -p data

pids=()
cleanup() {
  echo
  echo "stopping cluster..."
  kill "${pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for id in 1 2 3; do
  ./bin/raftkv-node --id="$id" --peers="$PEERS" --data-dir="data/node-$id" &
  pids+=($!)
done

echo "3-node shard running on :9001, :9002, :9003 (Ctrl-C to stop)"
echo "try: curl -X PUT -d hello localhost:9001/kv/greeting && curl localhost:9002/kv/greeting"
wait
