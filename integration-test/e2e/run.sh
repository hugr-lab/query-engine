#!/bin/bash
# E2E test runner for the query engine.
# Full lifecycle: prepare → start → provision → test → teardown.
#
# Usage:
#   ./run.sh              # Full run with teardown
#   ./run.sh --keep       # Keep containers running after tests
#   UPDATE_EXPECTED=1 ./run.sh  # Update expected output files

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
ENGINE_URL="http://localhost:15000"
QUERIES_DIR="$SCRIPT_DIR/testdata/queries"

PASS=0
FAIL=0
SKIP=0
KEEP=false

# jq filter to recursively sort arrays of objects by "name" field for deterministic comparison
JQ_DEEP_SORT='def sort_arrays: if type == "array" then map(sort_arrays) | sort_by(if type == "object" and has("name") then .name else tostring end) elif type == "object" then to_entries | sort_by(.key) | map(.value = (.value | sort_arrays)) | from_entries else . end; . | sort_arrays'

for arg in "$@"; do
  case $arg in
    --keep) KEEP=true ;;
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
docker compose -f "$COMPOSE_FILE" up -d --build --wait
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to start environment"
  exit 2
fi

# 3. Provision data sources
echo ""
echo "Provisioning data sources..."
"$SCRIPT_DIR/provision-sources.sh" "$ENGINE_URL"
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to provision data sources"
  exit 2
fi

# 4. Run tests
echo ""
echo "Running tests..."

run_single_test() {
  local test_dir="$1"
  local test_name="$2"
  local query_file="$test_dir/query.graphql"
  local expected_file="$test_dir/expected.json"

  if [ ! -f "$query_file" ]; then
    return 1
  fi

  local query
  query=$(cat "$query_file")
  local actual
  actual=$(curl -sf -X POST "$ENGINE_URL/query" \
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
  local test_dir="$1"
  local test_name="$2"

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
    actual=$(curl -sf -X POST "$ENGINE_URL/query" \
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
  local test_dir="$1"
  local test_name="$2"
  local request_file="$test_dir/request.json"
  local expected_file="$test_dir/expected.json"

  local actual
  actual=$(curl -sf -X POST "$ENGINE_URL/jq-query" \
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

# Walk query directories
for category_dir in "$QUERIES_DIR"/*/; do
  [ -d "$category_dir" ] || continue
  category=$(basename "$category_dir")

  for test_dir in "$category_dir"*/; do
    [ -d "$test_dir" ] || continue
    test_name="$category/$(basename "$test_dir")"

    if [ -f "$test_dir/request.json" ]; then
      # JQ test (POST to /jq endpoint)
      run_jq_test "$test_dir" "$test_name"
    elif [ -f "$test_dir/query.graphql" ]; then
      # Single-step test
      run_single_test "$test_dir" "$test_name"
    elif ls "$test_dir"/*.graphql &>/dev/null; then
      # Multi-step test
      run_multistep_test "$test_dir" "$test_name"
    fi
  done
done

# 5. Summary
echo ""
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"

if [ $FAIL -gt 0 ]; then
  exit 1
fi
exit 0
