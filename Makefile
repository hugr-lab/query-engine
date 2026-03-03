CGO_CFLAGS ?= -O1 -g
GO_BUILD_TAGS ?= duckdb_arrow
GO_TEST_FLAGS ?= -v -count=1

export CGO_CFLAGS

.PHONY: build test test-unit test-integration test-e2e \
        test-compiler test-catalog-db test-coredb

# --- Build ---

build:
	go build -tags=$(GO_BUILD_TAGS) ./...

# --- All tests ---

test: test-unit test-integration

# --- Unit tests (everything except integration-test/) ---

test-unit:
	go test -tags=$(GO_BUILD_TAGS) $(GO_TEST_FLAGS) $$(go list -tags=$(GO_BUILD_TAGS) ./... | grep -v /integration-test/)

# --- Integration tests (all under integration-test/) ---

test-integration: test-compiler test-catalog-db test-coredb

test-compiler:
	go test -tags=$(GO_BUILD_TAGS) $(GO_TEST_FLAGS) ./integration-test/compiler/...

test-catalog-db:
	go test -tags=$(GO_BUILD_TAGS) $(GO_TEST_FLAGS) ./integration-test/catalog/db/...

test-coredb:
	go test -tags=$(GO_BUILD_TAGS) $(GO_TEST_FLAGS) ./integration-test/coredb/...

# --- E2E (Docker-based, separate from `go test`) ---

test-e2e:
	cd integration-test/e2e && bash run.sh

test-e2e-duckdb:
	cd integration-test/e2e && bash run.sh --duckdb-only

test-e2e-keep:
	cd integration-test/e2e && bash run.sh --keep

test-e2e-update:
	cd integration-test/e2e && UPDATE_EXPECTED=1 bash run.sh
