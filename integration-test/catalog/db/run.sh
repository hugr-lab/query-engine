#!/bin/bash
# DB Provider integration test runner.
# Tests the db.Provider against real DuckDB (in-memory) and PostgreSQL (Docker).
#
# Usage:
#   ./run.sh              # Full run with teardown
#   ./run.sh --keep       # Keep PostgreSQL container running after tests
#   ./run.sh --duckdb     # Run only DuckDB tests (no Docker needed)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
PG_DSN="postgres://test:test@localhost:5435/dbprovider_test?sslmode=disable"

KEEP=false
DUCKDB_ONLY=false

for arg in "$@"; do
  case $arg in
    --keep) KEEP=true ;;
    --duckdb) DUCKDB_ONLY=true ;;
  esac
done

cleanup() {
  if [ "$DUCKDB_ONLY" = true ]; then
    return
  fi
  if [ "$KEEP" = false ]; then
    echo ""
    echo "Tearing down PostgreSQL..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  else
    echo ""
    echo "PostgreSQL container kept running. Tear down with:"
    echo "  docker compose -f $COMPOSE_FILE down -v"
  fi
}

trap cleanup EXIT

PROJECT_ROOT="$SCRIPT_DIR/../../../"

# 1. Run DuckDB tests (no Docker needed)
echo "Running DuckDB provider tests..."
cd "$PROJECT_ROOT"
CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow -v -count=1 -run "TestDuckDB" ./integration-test/catalog/db/
echo ""
echo "DuckDB provider tests passed."

if [ "$DUCKDB_ONLY" = true ]; then
  echo "Skipping PostgreSQL tests (--duckdb flag)."
  exit 0
fi

# 2. Start PostgreSQL
echo ""
echo "Starting PostgreSQL..."
docker compose -f "$COMPOSE_FILE" up -d --wait
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to start PostgreSQL"
  exit 2
fi

echo "PostgreSQL is ready."

# 3. Run PostgreSQL tests
echo ""
echo "Running PostgreSQL provider tests..."
DBPROVIDER_TEST_PG_DSN="$PG_DSN" CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow -v -count=1 -run "TestPostgres" ./integration-test/catalog/db/

echo ""
echo "All provider tests passed."
