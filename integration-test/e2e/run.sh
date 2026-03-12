#!/bin/bash
# E2E test runner for the query engine.
# Full lifecycle: prepare → start → provision → test → teardown.
#
# Usage:
#   ./run.sh              # Full run with teardown (DuckDB + PG CoreDB)
#   ./run.sh --keep       # Keep containers running after tests
#   ./run.sh --duckdb-only  # Skip PostgreSQL CoreDB tests
#   UPDATE_EXPECTED=1 ./run.sh  # Update expected output files

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
QUERIES_DIR="$SCRIPT_DIR/testdata/queries"

PASS=0
FAIL=0
SKIP=0
KEEP=false
DUCKDB_ONLY=false

# jq filter to recursively sort arrays and round floats for deterministic comparison
JQ_DEEP_SORT='def normalize: if type == "array" then map(normalize) | sort_by(if type == "object" and has("name") then .name else tostring end) elif type == "object" then to_entries | sort_by(.key) | map(.value = (.value | normalize)) | from_entries elif type == "number" then (. * 100000 | round / 100000) else . end; . | normalize'

for arg in "$@"; do
  case $arg in
    --keep) KEEP=true ;;
    --duckdb-only) DUCKDB_ONLY=true ;;
  esac
done

cleanup() {
  if [ "$KEEP" = false ]; then
    echo ""
    echo "Tearing down..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  else
    echo ""
    echo "Containers kept running. Tear down with:"
    echo "  docker compose -f $COMPOSE_FILE down -v"
  fi
}

trap cleanup EXIT

# 1. Prepare DuckDB data files
echo "Preparing DuckDB test data..."
"$SCRIPT_DIR/prepare-data.sh"

# 2. Build and start
echo ""
echo "Starting E2E environment..."
if [ "$DUCKDB_ONLY" = true ]; then
  docker compose -f "$COMPOSE_FILE" up -d --build --wait query-engine
else
  docker compose -f "$COMPOSE_FILE" up -d --build --wait
fi

run_single_test() {
  local engine_url="$1"
  local test_dir="$2"
  local test_name="$3"
  local variant="${4:-}"
  local query_file="$test_dir/query.graphql"
  # Use engine-specific expected file if available (e.g., expected_pg.json)
  local expected_file="$test_dir/expected.json"
  if [ -n "$variant" ] && [ -f "$test_dir/expected_${variant}.json" ]; then
    expected_file="$test_dir/expected_${variant}.json"
  fi

  if [ ! -f "$query_file" ]; then
    return 1
  fi

  local query
  query=$(cat "$query_file")
  local actual
  actual=$(curl -sf -X POST "$engine_url/query" \
    -H "Content-Type: application/json" \
    -d "{\"query\": $(echo "$query" | jq -Rs .)}" 2>/dev/null) || {
    echo "  FAIL: $test_name (request failed)"
    FAIL=$((FAIL + 1))
    return 0
  }

  if [ "${UPDATE_EXPECTED:-0}" = "1" ]; then
    # When variant is specified and base expected.json exists, compare first.
    # Write to variant file only if result differs from base.
    local write_file="$expected_file"
    if [ -n "$variant" ] && [ -f "$test_dir/expected.json" ]; then
      local base_norm actual_upd_norm
      base_norm=$(jq -S "$JQ_DEEP_SORT" "$test_dir/expected.json" 2>/dev/null)
      actual_upd_norm=$(echo "$actual" | jq -S "$JQ_DEEP_SORT" 2>/dev/null)
      if [ "$base_norm" != "$actual_upd_norm" ]; then
        write_file="$test_dir/expected_${variant}.json"
      else
        rm -f "$test_dir/expected_${variant}.json"
        echo "  UPDATED: $test_name (same as base)"
        PASS=$((PASS + 1))
        return 0
      fi
    fi
    echo "$actual" | jq -S . > "$write_file"
    echo "  UPDATED: $test_name"
    PASS=$((PASS + 1))
    return 0
  fi

  if [ ! -f "$expected_file" ]; then
    echo "  SKIP: $test_name (no expected.json)"
    SKIP=$((SKIP + 1))
    return 0
  fi

  local actual_norm expected_norm
  actual_norm=$(echo "$actual" | jq -S "$JQ_DEEP_SORT" 2>/dev/null) || actual_norm="$actual"
  expected_norm=$(jq -S "$JQ_DEEP_SORT" "$expected_file" 2>/dev/null) || expected_norm=$(cat "$expected_file")

  if [ "$actual_norm" = "$expected_norm" ]; then
    echo "  PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $test_name"
    diff <(echo "$expected_norm") <(echo "$actual_norm") || true
    FAIL=$((FAIL + 1))
  fi
}

run_multistep_test() {
  local engine_url="$1"
  local test_dir="$2"
  local test_name="$3"

  # Find all numbered step files: 01_name.graphql, 02_name.graphql, ...
  local steps
  steps=$(ls "$test_dir"/*.graphql 2>/dev/null | sort)
  local step_count
  step_count=$(echo "$steps" | wc -l | tr -d ' ')

  local all_pass=true
  local step_num=0

  for query_file in $steps; do
    step_num=$((step_num + 1))
    local base
    base=$(basename "$query_file" .graphql)
    local expected_file="$test_dir/${base%}.json"
    # Try pattern: NN_name.graphql → NN_expected.json or NN_name.json
    if [ ! -f "$expected_file" ]; then
      local prefix
      prefix=$(echo "$base" | grep -o '^[0-9]*')
      expected_file="$test_dir/${prefix}_expected.json"
    fi

    local query
    query=$(cat "$query_file")
    local actual
    actual=$(curl -sf -X POST "$engine_url/query" \
      -H "Content-Type: application/json" \
      -d "{\"query\": $(echo "$query" | jq -Rs .)}" 2>/dev/null) || {
      echo "  FAIL: $test_name (step $step_num request failed)"
      FAIL=$((FAIL + 1))
      return 0
    }

    if [ "${UPDATE_EXPECTED:-0}" = "1" ]; then
      echo "$actual" | jq -S . > "$expected_file"
      continue
    fi

    if [ ! -f "$expected_file" ]; then
      echo "  SKIP: $test_name (step $step_num: no expected file)"
      SKIP=$((SKIP + 1))
      return 0
    fi

    local actual_norm expected_norm
    actual_norm=$(echo "$actual" | jq -S . 2>/dev/null) || actual_norm="$actual"
    expected_norm=$(jq -S . "$expected_file" 2>/dev/null) || expected_norm=$(cat "$expected_file")

    if [ "$actual_norm" != "$expected_norm" ]; then
      echo "  FAIL: $test_name (step $step_num)"
      diff <(echo "$expected_norm") <(echo "$actual_norm") || true
      all_pass=false
      break
    fi
  done

  if [ "${UPDATE_EXPECTED:-0}" = "1" ]; then
    echo "  UPDATED: $test_name ($step_count steps)"
    PASS=$((PASS + 1))
    return 0
  fi

  if [ "$all_pass" = true ]; then
    echo "  PASS: $test_name ($step_count steps)"
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
  fi
}

run_jq_test() {
  local engine_url="$1"
  local test_dir="$2"
  local test_name="$3"
  local request_file="$test_dir/request.json"
  local expected_file="$test_dir/expected.json"

  local actual
  actual=$(curl -sf -X POST "$engine_url/jq-query" \
    -H "Content-Type: application/json" \
    -d @"$request_file" 2>/dev/null) || {
    echo "  FAIL: $test_name (request failed)"
    FAIL=$((FAIL + 1))
    return 0
  }

  if [ "${UPDATE_EXPECTED:-0}" = "1" ]; then
    echo "$actual" | jq -S . > "$expected_file"
    echo "  UPDATED: $test_name"
    PASS=$((PASS + 1))
    return 0
  fi

  if [ ! -f "$expected_file" ]; then
    echo "  SKIP: $test_name (no expected.json)"
    SKIP=$((SKIP + 1))
    return 0
  fi

  local actual_norm expected_norm
  actual_norm=$(echo "$actual" | jq -S "$JQ_DEEP_SORT" 2>/dev/null) || actual_norm="$actual"
  expected_norm=$(jq -S "$JQ_DEEP_SORT" "$expected_file" 2>/dev/null) || expected_norm=$(cat "$expected_file")

  if [ "$actual_norm" = "$expected_norm" ]; then
    echo "  PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $test_name"
    diff <(echo "$expected_norm") <(echo "$actual_norm") || true
    FAIL=$((FAIL + 1))
  fi
}

# run_tests_against runs the full test suite against the given engine URL.
# $3 is an optional variant suffix for engine-specific expected files (e.g., "pg").
run_tests_against() {
  local engine_url="$1"
  local label="$2"
  local variant="${3:-}"

  echo ""
  echo "Running tests against $label ($engine_url)..."

  for category_dir in "$QUERIES_DIR"/*/; do
    [ -d "$category_dir" ] || continue
    category=$(basename "$category_dir")

    # Skip cluster tests — they use multi-node routing
    [ "$category" = "cluster" ] && continue
    # Skip iceberg tests for non-DuckDB engines and --duckdb-only mode
    if [ "$category" = "iceberg" ]; then
      [ -n "$variant" ] && continue
      [ "$DUCKDB_ONLY" = true ] && continue
    fi

    for test_dir in "$category_dir"*/; do
      [ -d "$test_dir" ] || continue
      test_name="$category/$(basename "$test_dir")"

      if [ -f "$test_dir/request.json" ]; then
        run_jq_test "$engine_url" "$test_dir" "$test_name"
      elif [ -f "$test_dir/query.graphql" ]; then
        run_single_test "$engine_url" "$test_dir" "$test_name" "$variant"
      elif ls "$test_dir"/*.graphql &>/dev/null; then
        run_multistep_test "$engine_url" "$test_dir" "$test_name"
      fi
    done
  done
}

# resolve_cluster_url reads the first "# @target:" comment from a .graphql file
# and returns the corresponding cluster node URL.
resolve_cluster_url() {
  local query_file="$1"
  local mgmt_url="$2"
  local w1_url="$3"
  local w2_url="$4"

  local target
  target=$(head -1 "$query_file" | grep -o '@target: *[a-z0-9_-]*' | sed 's/@target: *//' || true)

  case "$target" in
    mgmt|management) echo "$mgmt_url" ;;
    worker-1)        echo "$w1_url" ;;
    worker-2)        echo "$w2_url" ;;
    *)               echo "$mgmt_url" ;;  # default to management
  esac
}

# run_cluster_tests runs cluster-specific multistep tests with per-step node targeting.
run_cluster_tests() {
  local mgmt_url="$1"
  local w1_url="$2"
  local w2_url="$3"
  local cluster_dir="$QUERIES_DIR/cluster"

  echo ""
  echo "Running cluster tests (mgmt=$mgmt_url, w1=$w1_url, w2=$w2_url)..."

  for test_dir in "$cluster_dir"/*/; do
    [ -d "$test_dir" ] || continue
    local test_name="cluster/$(basename "$test_dir")"

    local steps
    steps=$(ls "$test_dir"/*.graphql 2>/dev/null | sort)
    [ -z "$steps" ] && continue
    local step_count
    step_count=$(echo "$steps" | wc -l | tr -d ' ')

    local all_pass=true
    local step_num=0

    for query_file in $steps; do
      step_num=$((step_num + 1))
      local base
      base=$(basename "$query_file" .graphql)
      local expected_file="$test_dir/${base}.json"
      if [ ! -f "$expected_file" ]; then
        local prefix
        prefix=$(echo "$base" | grep -o '^[0-9]*')
        expected_file="$test_dir/${prefix}_expected.json"
      fi

      # Resolve target URL from @target comment
      local step_url
      step_url=$(resolve_cluster_url "$query_file" "$mgmt_url" "$w1_url" "$w2_url")

      # Strip @target comment before sending query
      local query
      query=$(grep -v '^# *@target:' "$query_file")
      local actual
      actual=$(curl -sf -X POST "$step_url/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": $(echo "$query" | jq -Rs .)}" 2>/dev/null) || {
        echo "  FAIL: $test_name (step $step_num request failed)"
        FAIL=$((FAIL + 1))
        all_pass=false
        break
      }

      if [ "${UPDATE_EXPECTED:-0}" = "1" ]; then
        echo "$actual" | jq -S . > "$expected_file"
        continue
      fi

      if [ ! -f "$expected_file" ]; then
        echo "  SKIP: $test_name (step $step_num: no expected file)"
        SKIP=$((SKIP + 1))
        all_pass=false
        break
      fi

      local actual_norm expected_norm
      actual_norm=$(echo "$actual" | jq -S "$JQ_DEEP_SORT" 2>/dev/null) || actual_norm="$actual"
      expected_norm=$(jq -S "$JQ_DEEP_SORT" "$expected_file" 2>/dev/null) || expected_norm=$(cat "$expected_file")

      if [ "$actual_norm" != "$expected_norm" ]; then
        echo "  FAIL: $test_name (step $step_num)"
        diff <(echo "$expected_norm") <(echo "$actual_norm") || true
        all_pass=false
        break
      fi
    done

    if [ "${UPDATE_EXPECTED:-0}" = "1" ]; then
      echo "  UPDATED: $test_name ($step_count steps)"
      PASS=$((PASS + 1))
      continue
    fi

    if [ "$all_pass" = true ]; then
      echo "  PASS: $test_name ($step_count steps)"
      PASS=$((PASS + 1))
    else
      FAIL=$((FAIL + 1))
    fi
  done
}

# 2b. Build DuckLake data with PostgreSQL metadata (requires postgres container running)
if [ "$DUCKDB_ONLY" = false ]; then
  echo ""
  echo "Building DuckLake test data with PostgreSQL metadata..."
  DUCKDB_DIR="$SCRIPT_DIR/testdata/duckdb"
  DUCKDB_DOCKER_IMAGE="datacatering/duckdb:v1.5.0"
  DUCKLAKE_PGMETA_DIR="$DUCKDB_DIR/ducklake_pgmeta"
  rm -rf "$DUCKLAKE_PGMETA_DIR"
  mkdir -p "$DUCKLAKE_PGMETA_DIR/data"

  # Get the docker network name (compose project + _default)
  E2E_NETWORK=$(docker compose -f "$COMPOSE_FILE" config --format json | jq -r '.networks | to_entries[0].value.name // empty' 2>/dev/null)
  if [ -z "$E2E_NETWORK" ]; then
    E2E_NETWORK="e2e_default"
  fi

  docker run --rm -i \
    --network "$E2E_NETWORK" \
    -v "$DUCKDB_DIR:/workspace/duckdb" \
    "$DUCKDB_DOCKER_IMAGE" \
    "" <<'EOSQL'
INSTALL ducklake; LOAD ducklake;
INSTALL postgres; LOAD postgres;
CREATE SECRET _dl_pgmeta_pg_secret (
    TYPE postgres,
    HOST 'postgres',
    PORT '5432',
    DATABASE 'ducklake_meta',
    USER 'test',
    PASSWORD 'test'
);
CREATE SECRET _dl_pgmeta_secret (
    TYPE ducklake,
    METADATA_PATH '',
    DATA_PATH '/workspace/duckdb/ducklake_pgmeta/data/',
    METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': '_dl_pgmeta_pg_secret'}
);
ATTACH 'ducklake:_dl_pgmeta_secret' AS dl_test;

CREATE TABLE dl_test.main.sensors (
    id BIGINT,
    name VARCHAR,
    temperature DOUBLE,
    humidity DOUBLE,
    reading_time TIMESTAMP
);

-- Snapshot 2: first batch
INSERT INTO dl_test.main.sensors VALUES
    (1, 'sensor_a', 22.5, 65.0, '2025-01-01 10:00:00'),
    (2, 'sensor_b', 23.1, 70.2, '2025-01-01 10:05:00'),
    (3, 'sensor_c', 21.8, 55.5, '2025-01-01 10:10:00');

-- Snapshot 3: second batch
INSERT INTO dl_test.main.sensors VALUES
    (4, 'sensor_d', 25.0, 60.0, '2025-01-02 10:00:00'),
    (5, 'sensor_e', 19.5, 80.1, '2025-01-02 10:05:00');

-- Snapshot 4: update sensor_a
UPDATE dl_test.main.sensors SET temperature = 24.0, humidity = 68.0 WHERE id = 1;

DETACH dl_test;
EOSQL
  echo "  Built DuckLake at $DUCKLAKE_PGMETA_DIR (PostgreSQL metadata)"
fi

# 3. Provision Iceberg data source (requires iceberg-rest + MinIO)
if [ "$DUCKDB_ONLY" = false ]; then
  ENGINE_URL_DUCKDB="http://localhost:15000"

  echo ""
  echo "Provisioning Iceberg data source..."
  "$SCRIPT_DIR/provision-iceberg.sh" "$ENGINE_URL_DUCKDB" "$COMPOSE_FILE"
fi

# 4. Provision and test DuckDB-backed engine
ENGINE_URL_DUCKDB="http://localhost:15000"

echo ""
echo "Provisioning data sources (DuckDB CoreDB)..."
"$SCRIPT_DIR/provision-sources.sh" "$ENGINE_URL_DUCKDB"

run_tests_against "$ENGINE_URL_DUCKDB" "DuckDB CoreDB" ""

# 5. Provision and test PG-backed engine
if [ "$DUCKDB_ONLY" = false ]; then
  ENGINE_URL_PG="http://localhost:15001"

  # Reset shared PostgreSQL data before PG engine tests (DuckDB engine mutations
  # may have modified rows/sequences during its test run).
  echo ""
  echo "Resetting shared PostgreSQL data..."
  docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U test -d testdb -c "
    DROP SCHEMA public CASCADE;
    CREATE SCHEMA public;
  " > /dev/null 2>&1
  docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U test -d testdb \
    < "$SCRIPT_DIR/testdata/postgres/init.sql" > /dev/null 2>&1

  echo ""
  echo "Provisioning data sources (PostgreSQL CoreDB + DuckLake PG metadata)..."
  "$SCRIPT_DIR/provision-sources.sh" "$ENGINE_URL_PG" "/workspace/duckdb/local_pg.duckdb" "pgmeta"

  run_tests_against "$ENGINE_URL_PG" "PostgreSQL CoreDB" "pg"
fi

# 6. Cluster tests
if [ "$DUCKDB_ONLY" = false ]; then
  CLUSTER_MGMT_URL="http://localhost:15010"
  CLUSTER_W1_URL="http://localhost:15011"
  CLUSTER_W2_URL="http://localhost:15012"

  # Reset shared PostgreSQL data before cluster tests
  echo ""
  echo "Resetting shared PostgreSQL data for cluster tests..."
  docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U test -d testdb -c "
    DROP SCHEMA public CASCADE;
    CREATE SCHEMA public;
  " > /dev/null 2>&1
  docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U test -d testdb \
    < "$SCRIPT_DIR/testdata/postgres/init.sql" > /dev/null 2>&1

  echo ""
  echo "Provisioning cluster data sources..."
  "$SCRIPT_DIR/provision-cluster.sh" "$CLUSTER_MGMT_URL" "$CLUSTER_W1_URL"

  run_cluster_tests "$CLUSTER_MGMT_URL" "$CLUSTER_W1_URL" "$CLUSTER_W2_URL"
fi

# 7. Summary
echo ""
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"

if [ $FAIL -gt 0 ]; then
  exit 1
fi
exit 0
