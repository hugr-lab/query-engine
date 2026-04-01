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
    '{ test_app { default_items { id name } } }' \
    '"name":"alpha"'

run_test "app table function search (parameterized view)" \
    '{ test_app { default_search(args: { query: \"alpha\" }) { id name } } }' \
    '"name":"alpha"'

run_test "app table function search empty query" \
    '{ test_app { default_search(args: { query: \"\" }) { id name } } }' \
    '"name":"beta"'

run_test "schema introspection has test_app" \
    '{ __schema { types { name } } }' \
    'test_app'

echo ""
echo "Results: $PASS passed, $FAIL failed"
echo ""

if [ "${1:-}" != "--keep" ]; then
    echo "Tearing down..."
    docker compose down -v 2>/dev/null
fi

[ "$FAIL" -eq 0 ] || exit 1
