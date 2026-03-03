#!/bin/bash
# Registers and loads data sources via GraphQL mutations.
# Usage: ./provision-sources.sh <engine_url> [duckdb_path]
#   duckdb_path defaults to /workspace/duckdb/local.duckdb

set -e

ENGINE_URL="${1:-http://localhost:15000}"
DUCKDB_PATH="${2:-/workspace/duckdb/local.duckdb}"

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

# Remove existing sources first (idempotent for --keep reruns on PG CoreDB).
# No cascade deletes in DB, so delete from all 3 tables manually:
# data_source_catalogs (M2M) → catalog_sources → data_sources.
echo "  Cleaning existing data sources..."
for src in ext_bridge rest_api local_db pg_store; do
  gql "mutation { core { delete_data_source_catalogs(filter: { data_source_name: { _eq: \\\"$src\\\" } }) { data_source_name } } }" > /dev/null 2>&1 || true
  gql "mutation { core { delete_catalog_sources(filter: { name: { _eq: \\\"$src\\\" } }) { name } } }" > /dev/null 2>&1 || true
  gql "mutation { core { delete_data_sources(filter: { name: { _eq: \\\"$src\\\" } }) { name } } }" > /dev/null 2>&1 || true
done

# 1. Add PostgreSQL data source
echo "  Registering pg_store..."
gql 'mutation { core { insert_data_sources(data: { name: \"pg_store\", prefix: \"pg_store\", type: \"postgres\", path: \"postgres://test:test@postgres:5432/testdb\", as_module: true, catalogs: [{ name: \"pg_store\", type: \"localFS\", path: \"/workspace/schemas/pg_store\" }] }) { name } } }'

# 2. Add DuckDB data source
echo "  Registering local_db..."
gql "mutation { core { insert_data_sources(data: { name: \\\"local_db\\\", prefix: \\\"local_db\\\", type: \\\"duckdb\\\", path: \\\"$DUCKDB_PATH\\\", as_module: true, catalogs: [{ name: \\\"local_db\\\", type: \\\"localFS\\\", path: \\\"/workspace/schemas/local_db\\\" }] }) { name } } }"

# 3. Add HTTP data source
echo "  Registering rest_api..."
gql 'mutation { core { insert_data_sources(data: { name: \"rest_api\", prefix: \"rest_api\", type: \"http\", path: \"http://http-service:17000\", as_module: true, catalogs: [{ name: \"rest_api\", type: \"localFS\", path: \"/workspace/schemas/rest_api\" }] }) { name } } }'

# 4. Add extension data source (bridges local_db items and events)
echo "  Registering ext_bridge..."
gql 'mutation { core { insert_data_sources(data: { name: \"ext_bridge\", prefix: \"ext_bridge\", type: \"extension\", path: \"\", as_module: false, catalogs: [{ name: \"ext_bridge\", type: \"localFS\", path: \"/workspace/schemas/ext_bridge\" }] }) { name } } }'

# 5. Load all sources (ext_bridge must load after its dependencies)
for src in pg_store local_db rest_api ext_bridge; do
  echo "  Loading $src..."
  gql "mutation { function { core { load_data_source(name: \\\"$src\\\") { success message } } } }"
done

echo "Data sources provisioned and loaded."
