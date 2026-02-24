#!/bin/bash
# Registers and loads data sources via GraphQL mutations.
# Usage: ./provision-sources.sh <engine_url>

set -e

ENGINE_URL="${1:-http://localhost:15000}"

gql() {
  local result
  result=$(curl -sf -X POST "$ENGINE_URL/query" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"$1\"}")
  echo "$result"
  # Check for errors in response
  if echo "$result" | jq -e '.errors' > /dev/null 2>&1; then
    if echo "$result" | jq -e '.errors | length > 0' > /dev/null 2>&1; then
      echo "  ERROR: $(echo "$result" | jq -r '.errors[0].message')" >&2
      return 1
    fi
  fi
  return 0
}

echo "Provisioning data sources..."

# 1. Add PostgreSQL data source
echo "  Registering pg_store..."
gql 'mutation { core { insert_data_sources(data: { name: \"pg_store\", prefix: \"pg_store\", type: \"postgres\", path: \"postgres://test:test@postgres:5432/testdb\", as_module: true, catalogs: [{ name: \"pg_store\", type: \"localFS\", path: \"/workspace/schemas/pg_store\" }] }) { name } } }'

# 2. Add DuckDB data source
echo "  Registering local_db..."
gql 'mutation { core { insert_data_sources(data: { name: \"local_db\", prefix: \"local_db\", type: \"duckdb\", path: \"/workspace/duckdb/local.duckdb\", as_module: true, catalogs: [{ name: \"local_db\", type: \"localFS\", path: \"/workspace/schemas/local_db\" }] }) { name } } }'

# 3. Add HTTP data source
echo "  Registering rest_api..."
gql 'mutation { core { insert_data_sources(data: { name: \"rest_api\", prefix: \"rest_api\", type: \"http\", path: \"http://http-service:17000\", as_module: true, catalogs: [{ name: \"rest_api\", type: \"localFS\", path: \"/workspace/schemas/rest_api\" }] }) { name } } }'

# 4. Load all sources
for src in pg_store local_db rest_api; do
  echo "  Loading $src..."
  gql "mutation { function { core { load_data_source(name: \\\"$src\\\") { success message } } } }"
done

echo "Data sources provisioned and loaded."
