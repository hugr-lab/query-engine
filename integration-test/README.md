# Integration Tests

Integration tests for the query engine, covering the catalog storage, CoreDB schema management, and end-to-end query execution.

```
integration-test/
├── catalog/entity/ # Entity catalog storage: engine boot, lifecycle, MCP, benches
├── cluster/        # Cluster mode integration tests (management + worker nodes)
├── coredb/         # CoreDB schema init & migration tests (DuckDB + PostgreSQL)
├── e2e/            # Docker-based end-to-end query tests
└── mcp/            # MCP endpoint integration tests
```

> The `compiler/` golden suite and the `compare/` utilities are gone with the
> GENERATE / ASSEMBLE rules they exercised (design-036). The generated schema is
> now produced on read by the catalog storage, and its oracle is the frozen
> snapshot set in `pkg/catalog/store/testdata/golden/`.

---

## Catalog Storage Tests

**Location**: `catalog/entity/`

Boots a real `hugr.Service` on the entity catalog storage (design 034) — the schema of every source lives in the `catalog.*` tables as a logical model and the GraphQL schema is generated from it on the fly. These cover the storage end to end rather than a provider in isolation: runtime sources compile and store, a user source loads and is queryable, the catalog's own state is visible through the `entity_*` views, curation reaches introspection, and the MCP `catalog-*` tools read and rank over it (including under a restricted role).

### Running

```bash
CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow ./integration-test/catalog/entity/ -v
```

No Docker needed — DuckDB in-memory CoreDB.

### Benchmarks

`metapath_bench_test.go` measures the meta-query path every `catalog-*` MCP tool reads, and the tools themselves:

```bash
CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow -run=NONE \
    -bench=BenchmarkMetaPath ./integration-test/catalog/entity/
```

`BenchmarkCatalogTools` has a vector arm that needs a live embedder (`EMBEDDER_URL` + `EMBEDDER_VECTOR_SIZE`) and skips without one; the lexical arm always runs.

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `EMBEDDER_URL` | Embedder for the vector-ranking tests and bench arm | — (those tests skip if unset) |
| `EMBEDDER_VECTOR_SIZE` | Vector dimension, must match the embedder | — |

---

## CoreDB Tests

**Location**: `coredb/`

Tests CoreDB schema initialization and migration against DuckDB (in-memory) and PostgreSQL (Docker). Verifies that `_schema_*` tables and other CoreDB tables are created correctly and migrations apply cleanly.

### Running

```bash
cd integration-test/coredb

# Full run: DuckDB + PostgreSQL
./run.sh

# DuckDB only
./run.sh --duckdb

# Keep PostgreSQL container running
./run.sh --keep

# Run directly with go test
CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow ./integration-test/coredb/ -run TestDuckDB -v

COREDB_TEST_PG_DSN="postgres://test:test@localhost:5434/coredb_test?sslmode=disable" \
  CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow ./integration-test/coredb/ -run TestPostgres -v
```

### Docker Setup

PostgreSQL on port **5434** (separate from DB Provider tests on 5435).

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `COREDB_TEST_PG_DSN` | PostgreSQL connection string | — (tests skip if unset) |
| `HUGR_MIGRATIONS_PATH` | Path to hugr migrations directory | Auto-resolved from sibling `hugr/migrations` repo |

---

## E2E Tests

**Location**: `e2e/`

Docker-based end-to-end tests. Spins up PostgreSQL (PostGIS), DuckDB, and an HTTP test service, provisions data sources via GraphQL mutations, then executes queries and compares results against expected JSON.

### Running

```bash
cd integration-test/e2e

# Run all tests
./run.sh

# Update expected output after intentional changes
UPDATE_EXPECTED=1 ./run.sh

# Keep containers running after tests (for debugging)
./run.sh --keep
```

### Architecture

```
docker-compose.yml
├── postgres (postgis/postgis:16-3.4)    — port 5433
├── http-service (internal test service)  — port 17000
└── query-engine (dev-server)             — port 15000
```

**Lifecycle**: prepare DuckDB data → build & start containers → provision sources via GraphQL mutations → run tests → teardown.

### Data Sources

| Source | Type | Engine | Description |
|--------|------|--------|-------------|
| `pg_store` | PostgreSQL | PostGIS | Products, categories, tags, locations with geometry |
| `local_db` | DuckDB | DuckDB | Events with comprehensive type coverage |
| `rest_api` | HTTP | — | GET/POST test endpoints |

Sources are registered and loaded via `provision-sources.sh` using GraphQL mutations against the running engine.

### Seed Data

| Source | File | Tables |
|--------|------|--------|
| PostgreSQL | `testdata/postgres/init.sql` | categories (3), tags (3), products (5), product_tags (6), locations (2 with PostGIS) |
| DuckDB | `testdata/duckdb/init.sql` | events (5), event_tags |

### Schemas

| Source | File | Features |
|--------|------|----------|
| `pg_store` | `testdata/schemas/pg_store/schema.graphql` | Tables, references, M2M, views, parameterized views, geometry, self-referential |
| `local_db` | `testdata/schemas/local_db/schema.graphql` | Tables, field references, function extensions (generate_series) |
| `rest_api` | `testdata/schemas/rest_api/schema.graphql` | HTTP functions (GET/POST), JSON cast |

### Test Types

Tests are auto-detected by the runner based on file presence:

**Single-step** — `query.graphql` + `expected.json`:
```
testdata/queries/tables/select_one/
├── query.graphql       # GraphQL query
└── expected.json       # Expected response
```

**Multi-step** — numbered files (e.g., mutations with insert then verify):
```
testdata/queries/mutations/insert/
├── 01_insert.graphql   # Step 1: insert data
├── 01_expected.json    # Step 1: expected result
├── 02_verify.graphql   # Step 2: verify inserted data
└── 02_expected.json    # Step 2: expected result
```

**JQ** — `request.json` (POST to `/jq-query`):
```
testdata/queries/jq/transform/
├── request.json        # JQ query request body
└── expected.json       # Expected response
```

### Test Cases (79 total, 151 steps across 18 categories)

| Category | Tests | Covers |
|----------|-------|--------|
| **aggregations** (8) | basic, bucket, bucket_ordered, sub_aggregation, struct_aggregation, + others | Aggregation queries, bucketing, nested/struct aggregation |
| **cluster** (7) | node_registration, schema_sync, heartbeat, ghost_cleanup, secret_sync, + others | Cluster mode lifecycle and coordination |
| **core** (4) | load_unload, + others | Data source lifecycle |
| **extensions** (8) | references_forward, references_reverse, references_aggregation, function_call_http, cross_source_join, + others | Extension fields, cross-source extensions |
| **fields** (2) | field tests | Field-level operations |
| **filters** (11) | and_or, by_reference, by_reference_reverse, eq, gt_lt, in, is_null, json_filter, like_ilike, + others | Filter operators, reference filtering, `any_of` |
| **functions** (1) | table_function | Table-generating functions |
| **h3** (1) | basic | H3 geospatial aggregation |
| **http** (2) | get, post | HTTP data source integration |
| **joins** (1) | cross_source | `_join` across data sources |
| **jq** (1) | transform | JQ transformation inside GraphQL |
| **metadata** (9) | catalog_sources, data_sources, describe_schema, duckdb_columns, duckdb_databases, duckdb_tables, introspection, introspection_type, roles | Schema introspection and metadata queries |
| **mutations** (4) | delete, insert, insert_with_defaults, update | CRUD operations (multi-step) |
| **references** (5) | forward_join, m2m, nested_deep, reverse_join, self_referential | All reference types |
| **spatial** (1) | intersects | `_spatial` intersection queries |
| **tables** (3) | duckdb_select, select_list, select_one | Basic table queries |
| **types** (9) | bigint, boolean, date_time, geometry, json, + others | Data type coverage |
| **views** (2) | parameterized, simple | Simple and parameterized views |

### Comparison Logic

JSON comparison uses a recursive deep-sort jq filter to normalize non-deterministic array ordering (e.g., from Go map iteration). Arrays of objects are sorted by `name` field when present.

---

## Cluster Integration Tests

**Location**: `cluster/`

Docker-based tests for cluster mode. Validates management + worker node coordination, including node registration in `_cluster_nodes`, schema version tracking, heartbeat monitoring, ghost node cleanup, and secret synchronization via `x-hugr-secret` header.

---

## MCP Integration Tests

**Location**: `mcp/`

Tests for the MCP (Model Context Protocol) endpoint. Validates the 14 MCP tools (discovery-*, schema-*, data-*), structured output schemas (`WithOutputSchema`), and correct behavior of `schema-enum_values` using catalog tables instead of introspection.
