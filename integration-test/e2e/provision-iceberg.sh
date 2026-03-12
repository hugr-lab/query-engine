#!/bin/bash
# Seeds Iceberg test data and registers the Iceberg data source.
# Usage: ./provision-iceberg.sh <engine_url> <compose_file>
#
# Steps:
#   1. Register S3 storage (MinIO) in the query engine
#   2. Seed test data via DuckDB connected to iceberg-rest
#   3. Register and load the Iceberg data source

set -eo pipefail

ENGINE_URL="${1:-http://localhost:15000}"
COMPOSE_FILE="${2:-docker-compose.yml}"

gql() {
  local result
  result=$(curl -sf -X POST "$ENGINE_URL/query" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"$1\"}")
  echo "$result"
  if echo "$result" | jq -e '.errors | length > 0' > /dev/null 2>&1; then
    echo "  ERROR: $(echo "$result" | jq -r '.errors[0].message')" >&2
    return 1
  fi
  return 0
}

echo "Provisioning Iceberg data source..."

# 1. Register MinIO S3 storage so DuckDB can read iceberg data files
echo "  Registering MinIO S3 storage..."
gql 'mutation { function { core { storage { register_object_storage(type: \"S3\", name: \"minio_s3\", scope: \"s3://warehouse\", key: \"minioadmin\", secret: \"minioadmin\", region: \"us-east-1\", endpoint: \"minio:9000\", use_ssl: false, url_style: \"path\") { success message } } } } }'

# 2. Seed test data into iceberg-rest catalog via DuckDB
echo "  Seeding Iceberg test data..."

# Get the docker network name
E2E_NETWORK=$(docker compose -f "$COMPOSE_FILE" config --format json | jq -r '.networks | to_entries[0].value.name // empty' 2>/dev/null)
if [ -z "$E2E_NETWORK" ]; then
  E2E_NETWORK="e2e_default"
fi

DUCKDB_DOCKER_IMAGE="datacatering/duckdb:v1.5.0"

# Seed is best-effort — may fail if data already exists from a previous run
set +e
docker run --rm -i \
  --network "$E2E_NETWORK" \
  "$DUCKDB_DOCKER_IMAGE" \
  "" <<'EOSQL'
INSTALL iceberg; LOAD iceberg;

-- Create S3 secret for MinIO access
CREATE SECRET minio_s3 (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false
);

-- Attach to iceberg REST catalog (no auth)
ATTACH 'warehouse' AS ice_test (TYPE iceberg, ENDPOINT 'http://iceberg-rest:8181', AUTHORIZATION_TYPE 'none');

-- Create namespace (Iceberg requires explicit namespace creation)
CREATE SCHEMA ice_test."default";

-- Create test table
CREATE TABLE ice_test."default".sensors (
    id BIGINT,
    name VARCHAR,
    temperature DOUBLE,
    humidity DOUBLE,
    reading_time TIMESTAMP
);

-- Insert first batch (creates snapshot)
INSERT INTO ice_test."default".sensors VALUES
    (1, 'sensor_a', 22.5, 65.0, '2025-01-01 10:00:00'),
    (2, 'sensor_b', 23.1, 70.2, '2025-01-01 10:05:00'),
    (3, 'sensor_c', 21.8, 55.5, '2025-01-01 10:10:00');

-- Insert second batch (creates another snapshot for time-travel testing)
INSERT INTO ice_test."default".sensors VALUES
    (4, 'sensor_d', 25.0, 60.0, '2025-01-02 10:00:00'),
    (5, 'sensor_e', 19.5, 80.1, '2025-01-02 10:05:00');

DETACH ice_test;
EOSQL
set -eo pipefail

echo "  Iceberg test data seeded."

# 3. Clean up any existing iceberg source
echo "  Cleaning existing iceberg sources..."
gql "mutation { core { delete_catalog_sources(filter: { name: { eq: \\\"ice_test\\\" } }) { success } }" > /dev/null 2>&1 || true
gql "mutation { core { delete_data_sources(filter: { name: { eq: \\\"ice_test\\\" } }) { success } }" > /dev/null 2>&1 || true

# 4. Register Iceberg data source (self_defined = auto-discover tables)
echo "  Registering ice_test..."
gql 'mutation { core { insert_data_sources(data: { name: \"ice_test\", prefix: \"ice_test\", type: \"iceberg\", path: \"iceberg+http://iceberg-rest:8181/warehouse\", as_module: true, self_defined: true }) { name } } }'

# 5. Load the source
echo "  Loading ice_test..."
gql 'mutation { function { core { load_data_source(name: \"ice_test\") { success message } } } }'

# 6. Register DuckLake bridge source (ensure_db=true creates the PG database automatically)
echo "  Cleaning existing bridge sources..."
gql "mutation { core { delete_catalog_sources(filter: { name: { eq: \\\"dl_ice_bridge\\\" } }) { success } }" > /dev/null 2>&1 || true
gql "mutation { core { delete_data_sources(filter: { name: { eq: \\\"dl_ice_bridge\\\" } }) { success } }" > /dev/null 2>&1 || true

echo "  Registering dl_ice_bridge (DuckLake with ensure_db)..."
gql 'mutation { core { insert_data_sources(data: { name: \"dl_ice_bridge\", prefix: \"dl_ice_bridge\", type: \"ducklake\", path: \"postgres://test:test@postgres:5432/ducklake_ice_bridge?data_path=s3://warehouse/ducklake_bridge/&ensure_db=true\", as_module: true, self_defined: true }) { name } } }'

echo "  Loading dl_ice_bridge..."
gql 'mutation { function { core { load_data_source(name: \"dl_ice_bridge\") { success message } } } }'

echo "Iceberg data source provisioned and loaded."
