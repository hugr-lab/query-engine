#!/usr/bin/env bash
# Run the /ipc/ingest integration tests against a fresh Postgres container.
#
# Usage:
#   ./run.sh         # bring up postgres, run tests, tear down
#   ./run.sh keep    # leave the container running after tests (for re-runs)
#
# The tests pick up the postgres DSN from INGEST_POSTGRES_DSN; if unset, this
# script populates it with the dockerized instance.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$HERE/docker-compose.yml"

cleanup() {
    if [[ "${1:-}" != "keep" ]]; then
        docker compose -f "$COMPOSE_FILE" down -v
    fi
}
trap 'cleanup "${1:-}"' EXIT

docker compose -f "$COMPOSE_FILE" up -d --wait

export INGEST_POSTGRES_DSN="postgres://test:test@127.0.0.1:5437/ingestdb"
export HUGR_INGEST_SCHEMAS_PATH="$HERE/testdata/schemas"

cd "$HERE/../.."
go test -tags=duckdb_arrow -count=1 -v ./integration-test/ingest/...
