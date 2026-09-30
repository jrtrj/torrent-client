# Dev-loop targets. `make verify` is the CI bar: build, vet, test, gofmt.
GO  ?= go
BIN ?= torrent-client

.PHONY: build test lint fmt fmt-check verify clean

build: ## build the client binary
	$(GO) build -o $(BIN) ./cmd/torrent-client

test: ## run the test suite
	$(GO) test ./...

lint: ## vet the tree (staticcheck is not installed here)
	$(GO) vet ./...

fmt: ## rewrite files with gofmt
	gofmt -l -w .

fmt-check: ## fail if any file needs gofmt
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

verify: build lint test fmt-check ## the CI bar
	$(GO) build ./...
	@echo "verify: build, vet, test, gofmt all green"

clean: ## drop build artifacts
	$(GO) clean
	rm -f $(BIN)
