#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
HUGR_URL="http://localhost:15100"

echo "Starting E2E hugr-apps environment..."
docker compose up -d --build --wait 2>&1

echo ""
echo "Waiting for test-app to register..."
sleep 10

PASS=0
FAIL=0

run_test() {
    local name="$1"
    local query="$2"
    local expected="$3"

    result=$(curl -sf -X POST "$HUGR_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"$query\"}" 2>/dev/null || echo "CURL_FAILED")

    if echo "$result" | grep -q "$expected"; then
        echo "  PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $name"
        echo "    Expected to contain: $expected"
        echo "    Got: $result"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo "Running hugr-app E2E tests..."

# Happy path tests
run_test "app function add" \
    '{ function { test_app { add(a: 1, b: 2) } } }' \
    '"add":3'

run_test "app function echo" \
    '{ function { test_app { echo(msg: \"hello\") } } }' \
    '"echo":"hello"'

run_test "app table items" \
    '{ test_app { items { id name } } }' \
    '"name":"alpha"'

run_test "app table function search (parameterized view)" \
    '{ test_app { search(args: { query: \"alpha\" }) { id name } } }' \
    '"name":"alpha"'

run_test "app table function search empty query" \
    '{ test_app { search(args: { query: \"\" }) { id name } } }' \
    '"name":"beta"'

run_test "schema introspection has test_app" \
    '{ __schema { types { name } } }' \
    'test_app'

# Data source tests (PostgreSQL provisioned by app)
run_test "app DS events table (provisioned)" \
    '{ test_app { store { events { id event_type } } } }' \
    '"event_type":"app_start"'

run_test "app DS events count" \
    '{ test_app { store { events { id } } } }' \
    '"id":2'

# Multi-schema tests (named schema "admin" → nested module)
run_test "admin module function user_count" \
    '{ function { test_app { admin { user_count } } } }' \
    '"user_count":99'

run_test "admin module table function audit" \
    '{ test_app { admin { admin_audit(args: { limit: 10 }) { id action user_name } } } }' \
    '"action":"login"'

# HugrSchema test (custom SDL for DS — has payload field with description)
run_test "app DS with HugrSchema (custom SDL)" \
    '{ test_app { store { events { event_type payload } } } }' \
    '"payload"'

echo ""
echo "--- Lifecycle tests ---"
echo ""

# === Graceful shutdown ===
echo "Stopping test-app gracefully (SIGTERM)..."
docker compose stop test-app 2>/dev/null
sleep 3

run_test "after graceful stop: app not queryable" \
    '{ function { test_app { add(a: 1, b: 2) } } }' \
    'error'

# Note: app DS may not be accessible via test_app.store path after app unload
# because test_app module is removed. DS data persists in PostgreSQL though.

# === Restart with same version ===
echo "Restarting test-app (same version v1)..."
docker compose up -d test-app 2>/dev/null
sleep 20  # wait for startup + registration + provisioning + load

run_test "after restart v1: app function works" \
    '{ function { test_app { add(a: 10, b: 20) } } }' \
    '"add":30'

run_test "after restart v1: app DS works" \
    '{ test_app { store { events { id event_type } } } }' \
    '"event_type":"app_start"'

# === Version upgrade (v2) — tests cleanup + re-registration ===
echo "Stopping test-app for version upgrade..."
docker compose stop test-app 2>/dev/null
sleep 3

echo "Starting test-app v2 (with APP_VERSION override)..."
docker compose run -d -e APP_VERSION=2.0.0 test-app 2>/dev/null
sleep 15  # wait for startup + cleanup + re-provision

run_test "after v2 upgrade: app function works" \
    '{ function { test_app { add(a: 100, b: 200) } } }' \
    '"add":300'

run_test "after v2 upgrade: app DS re-provisioned" \
    '{ test_app { store { events { id event_type } } } }' \
    '"event_type"'

run_test "after v2 upgrade: admin module works" \
    '{ function { test_app { admin { user_count } } } }' \
    '"user_count":99'

echo ""
echo "Results: $PASS passed, $FAIL failed"
echo ""

if [ "${1:-}" != "--keep" ]; then
    echo "Tearing down..."
    docker compose down -v 2>/dev/null
fi

[ "$FAIL" -eq 0 ] || exit 1
