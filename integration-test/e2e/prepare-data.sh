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

# Create a separate copy for PG engine (avoids DuckDB file locks between engines)
PG_DB_FILE="$DUCKDB_DIR/local_pg.duckdb"
rm -f "$PG_DB_FILE" "$PG_DB_FILE.wal"
cp "$DB_FILE" "$PG_DB_FILE"
echo "  Created $PG_DB_FILE (copy for PG engine)"

# Build DuckLake test data
# IMPORTANT: Always use Docker with a pinned DuckDB version matching the Go binary's
# embedded DuckDB (v1.4.x). The local duckdb CLI may be newer and create incompatible
# storage format files that the query-engine cannot read.
DUCKLAKE_DIR="$DUCKDB_DIR/ducklake"
DUCKLAKE_META="$DUCKLAKE_DIR/meta.duckdb"
DUCKLAKE_DATA="$DUCKLAKE_DIR/data"
DUCKDB_DOCKER_IMAGE="datacatering/duckdb:v1.4.4"

rm -f "$DUCKLAKE_META" "$DUCKLAKE_META.wal"
rm -rf "$DUCKLAKE_DATA"
mkdir -p "$DUCKLAKE_DATA"

echo "Building DuckLake test data (using Docker $DUCKDB_DOCKER_IMAGE)..."

# Mount the duckdb dir at /workspace/duckdb (same as the query-engine container)
# so that DATA_PATH stored in metadata matches what the query-engine will use.
DUCKLAKE_SQL="$(cat <<'EOSQL'
INSTALL ducklake; LOAD ducklake;
CREATE SECRET _dl_e2e_secret (
    TYPE ducklake,
    METADATA_PATH '/workspace/duckdb/ducklake/meta.duckdb',
    DATA_PATH '/workspace/duckdb/ducklake/data/'
);
ATTACH 'ducklake:_dl_e2e_secret' AS dl_test;

CREATE TABLE dl_test.main.sensors (
    id BIGINT,
    name VARCHAR,
    temperature DOUBLE,
    humidity DOUBLE,
    reading_time TIMESTAMP
);

-- Snapshot 2: first batch of sensor readings
INSERT INTO dl_test.main.sensors VALUES
    (1, 'sensor_a', 22.5, 65.0, '2025-01-01 10:00:00'),
    (2, 'sensor_b', 23.1, 70.2, '2025-01-01 10:05:00'),
    (3, 'sensor_c', 21.8, 55.5, '2025-01-01 10:10:00');

-- Snapshot 3: second batch of sensor readings
INSERT INTO dl_test.main.sensors VALUES
    (4, 'sensor_d', 25.0, 60.0, '2025-01-02 10:00:00'),
    (5, 'sensor_e', 19.5, 80.1, '2025-01-02 10:05:00');

-- Snapshot 4: update sensor_a with corrected readings
UPDATE dl_test.main.sensors SET temperature = 24.0, humidity = 68.0 WHERE id = 1;

DETACH dl_test;
EOSQL
)"

echo "$DUCKLAKE_SQL" | docker run --rm -i \
    -v "$DUCKDB_DIR:/workspace/duckdb" \
    "$DUCKDB_DOCKER_IMAGE" \
    ""
echo "  Built DuckLake at $DUCKLAKE_DIR"

# Build separate DuckLake for PG engine (avoids DuckDB file locks).
# Must be a separate build (not just a copy) because DuckLake stores
# DATA_PATH in metadata and rejects mismatches.
DUCKLAKE_PG_DIR="$DUCKDB_DIR/ducklake_pg"
rm -rf "$DUCKLAKE_PG_DIR"
mkdir -p "$DUCKLAKE_PG_DIR/data"

echo "Building DuckLake test data for PG engine..."

DUCKLAKE_PG_SQL="$(cat <<'EOSQL'
INSTALL ducklake; LOAD ducklake;
CREATE SECRET _dl_e2e_secret_pg (
    TYPE ducklake,
    METADATA_PATH '/workspace/duckdb/ducklake_pg/meta.duckdb',
    DATA_PATH '/workspace/duckdb/ducklake_pg/data/'
);
ATTACH 'ducklake:_dl_e2e_secret_pg' AS dl_test;

CREATE TABLE dl_test.main.sensors (
    id BIGINT,
    name VARCHAR,
    temperature DOUBLE,
    humidity DOUBLE,
    reading_time TIMESTAMP
);

INSERT INTO dl_test.main.sensors VALUES
    (1, 'sensor_a', 22.5, 65.0, '2025-01-01 10:00:00'),
    (2, 'sensor_b', 23.1, 70.2, '2025-01-01 10:05:00'),
    (3, 'sensor_c', 21.8, 55.5, '2025-01-01 10:10:00');

INSERT INTO dl_test.main.sensors VALUES
    (4, 'sensor_d', 25.0, 60.0, '2025-01-02 10:00:00'),
    (5, 'sensor_e', 19.5, 80.1, '2025-01-02 10:05:00');

UPDATE dl_test.main.sensors SET temperature = 24.0, humidity = 68.0 WHERE id = 1;

DETACH dl_test;
EOSQL
)"

echo "$DUCKLAKE_PG_SQL" | docker run --rm -i \
    -v "$DUCKDB_DIR:/workspace/duckdb" \
    "$DUCKDB_DOCKER_IMAGE" \
    ""
echo "  Built DuckLake at $DUCKLAKE_PG_DIR (for PG engine)"

echo "DuckDB data preparation complete."
