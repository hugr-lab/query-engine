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
echo "=== Happy path tests ==="
echo ""

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
echo "=== Lifecycle: clean restart ==="
echo ""

echo "Stopping ALL services..."
docker compose stop 2>/dev/null
sleep 2

echo "Starting ALL services (clean restart)..."
docker compose up -d --wait 2>&1
sleep 10

run_test "after restart: app function works" \
    '{ function { test_app { add(a: 10, b: 20) } } }' \
    '"add":30'

run_test "after restart: app DS works" \
    '{ test_app { store { events { id event_type } } } }' \
    '"event_type":"app_start"'

run_test "after restart: admin module works" \
    '{ function { test_app { admin { user_count } } } }' \
    '"user_count":99'

echo ""
echo "=== Lifecycle: app crash + recovery ==="
echo ""

echo "Killing test-app (simulating crash)..."
docker compose kill test-app 2>/dev/null

echo "Waiting for heartbeat to detect crash (~15s)..."
sleep 18

run_test "after crash: app functions unavailable" \
    '{ function { test_app { add(a: 1, b: 2) } } }' \
    'error'

echo "Restarting test-app..."
docker compose up -d test-app 2>/dev/null
sleep 15

run_test "after recovery: app function works" \
    '{ function { test_app { add(a: 5, b: 5) } } }' \
    '"add":10'

# Note: sub-DS (test_app.store) recovery after crash requires heartbeat
# monitor integration (not yet wired). Skipping DS check for now.

echo ""
echo "=== Lifecycle: version upgrade (migration) ==="
echo ""

echo "Stopping test-app for version upgrade..."
docker compose stop test-app 2>/dev/null
sleep 5

echo "Starting test-app with APP_VERSION=2.0.0..."
APP_VERSION=2.0.0 docker compose up -d test-app 2>/dev/null
sleep 15

run_test "after upgrade: app function works" \
    '{ function { test_app { add(a: 100, b: 200) } } }' \
    '"add":300'

run_test "after upgrade: migration ran (severity column)" \
    '{ test_app { store { events { event_type severity } } } }' \
    '"severity"'

echo ""
echo "=== Lifecycle: graceful shutdown ==="
echo ""

echo "Gracefully stopping test-app..."
docker compose stop test-app 2>/dev/null
sleep 5

run_test "after graceful stop: app unloaded (functions gone)" \
    '{ function { test_app { add(a: 1, b: 2) } } }' \
    'error'

echo ""
echo "Results: $PASS passed, $FAIL failed"
echo ""

if [ "${1:-}" != "--keep" ]; then
    echo "Tearing down..."
    docker compose down -v 2>/dev/null
fi

[ "$FAIL" -eq 0 ] || exit 1
