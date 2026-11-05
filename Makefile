.PHONY: build cluster test test-race test-all fmt vet clean

build:
	go build -o bin/raftkv-node ./cmd/raftkv-node
	go build -o bin/raftkv-gateway ./cmd/raftkv-gateway

cluster: build
	./scripts/run-cluster.sh

test:
	go test ./internal/... -count=1

test-race:
	go test ./internal/... -race -count=1

test-all:
	go test ./... -count=1 -timeout 120s

fmt:
	gofmt -l .

vet:
	go vet ./...

clean:
	rm -rf bin/ data/
