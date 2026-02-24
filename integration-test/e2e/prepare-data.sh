#!/bin/bash
# Builds DuckDB data files from SQL scripts.
# Uses duckdb CLI if available, otherwise falls back to Docker.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DUCKDB_DIR="$SCRIPT_DIR/testdata/duckdb"
DB_FILE="$DUCKDB_DIR/local.duckdb"
INIT_SQL="$DUCKDB_DIR/init.sql"

# Remove existing file
rm -f "$DB_FILE" "$DB_FILE.wal"

echo "Building DuckDB test data..."

if command -v duckdb &> /dev/null; then
    duckdb "$DB_FILE" < "$INIT_SQL"
    echo "  Built $DB_FILE using duckdb CLI"
else
    echo "  duckdb CLI not found, using Docker..."
    docker run --rm \
        -v "$DUCKDB_DIR:/data" \
        datacatering/duckdb:latest \
        duckdb /data/local.duckdb < "$INIT_SQL"
    echo "  Built $DB_FILE using Docker"
fi

echo "DuckDB data preparation complete."
