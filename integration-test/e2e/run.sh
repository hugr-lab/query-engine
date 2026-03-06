#!/bin/bash
# E2E test runner for the query engine.
# Full lifecycle: prepare → start → provision → test → teardown.
#
# Usage:
#   ./run.sh              # Full run with teardown (DuckDB + PG CoreDB)
#   ./run.sh --keep       # Keep containers running after tests
#   ./run.sh --duckdb-only  # Skip PostgreSQL CoreDB tests
#   UPDATE_EXPECTED=1 ./run.sh  # Update expected output files

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
QUERIES_DIR="$SCRIPT_DIR/testdata/queries"

PASS=0
FAIL=0
SKIP=0
KEEP=false
DUCKDB_ONLY=false

# jq filter to recursively sort arrays of objects by "name" field for deterministic comparison
JQ_DEEP_SORT='def sort_arrays: if type == "array" then map(sort_arrays) | sort_by(if type == "object" and has("name") then .name else tostring end) elif type == "object" then to_entries | sort_by(.key) | map(.value = (.value | sort_arrays)) | from_entries else . end; . | sort_arrays'

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
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to start environment"
  exit 2
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

# 3. Provision and test DuckDB-backed engine
ENGINE_URL_DUCKDB="http://localhost:15000"

echo ""
echo "Provisioning data sources (DuckDB CoreDB)..."
"$SCRIPT_DIR/provision-sources.sh" "$ENGINE_URL_DUCKDB"
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to provision data sources (DuckDB CoreDB)"
  exit 2
fi

run_tests_against "$ENGINE_URL_DUCKDB" "DuckDB CoreDB" ""

# 4. Provision and test PG-backed engine
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
  echo "Provisioning data sources (PostgreSQL CoreDB)..."
  "$SCRIPT_DIR/provision-sources.sh" "$ENGINE_URL_PG" "/workspace/duckdb/local_pg.duckdb"
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to provision data sources (PostgreSQL CoreDB)"
    exit 2
  fi

  run_tests_against "$ENGINE_URL_PG" "PostgreSQL CoreDB" "pg"
fi

# 5. Cluster tests
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
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to provision cluster data sources"
    exit 2
  fi

  run_cluster_tests "$CLUSTER_MGMT_URL" "$CLUSTER_W1_URL" "$CLUSTER_W2_URL"
fi

# 6. Summary
echo ""
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"

if [ $FAIL -gt 0 ]; then
  exit 1
fi
exit 0
