# Integration Tests

Integration tests for the query engine, covering schema compilation, the catalog storage, CoreDB schema management, and end-to-end query execution.

```
integration-test/
├── catalog/entity/ # Entity catalog storage: engine boot, lifecycle, MCP, benches
├── cluster/        # Cluster mode integration tests (management + worker nodes)
├── compiler/       # Schema compiler golden tests & integration tests
├── compare/        # Schema comparison utilities (used by compiler tests)
├── coredb/         # CoreDB schema init & migration tests (DuckDB + PostgreSQL)
├── e2e/            # Docker-based end-to-end query tests
└── mcp/            # MCP endpoint integration tests
```

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

## Compiler Tests

**Location**: `compiler/`

Golden test framework that compiles GraphQL schema definitions and compares the output against expected (golden) files. Tests cover single-catalog, multi-catalog, extension, and error scenarios.

### Running

```bash
# Run all compiler tests
CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow ./integration-test/compiler/ -v

# Update golden files after intentional changes
UPDATE_GOLDEN=1 CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow ./integration-test/compiler/ -v
```

### Test Files

| File | Purpose |
|------|---------|
| `golden_test.go` | Golden test framework — loads `config.json` + schema files, compiles, compares against `expected/schema.graphql` |
| `integration_test.go` | Integration tests — multi-catalog lifecycle, cross-catalog references, extensions, engine capabilities |
| `comptest_test.go` | Compiler parity tests — compares old vs new compiler output |
| `cross_compiler_test.go` | AST comparison utilities for compiler parity checks |

### Test Case Structure

Each test case is a directory under `testdata/`:

```
testdata/NN_test_name/
├── config.json                 # Test configuration
├── schemes/
│   └── 01_schema.graphql       # Input schema(s)
└── expected/
    └── schema.graphql          # Golden output
```

**config.json** fields:

```json
{
  "catalogs": [
    {
      "file": "01_schema.graphql",
      "name": "catalog_name",
      "engine": "duckdb|postgres",
      "as_module": false,
      "read_only": false,
      "is_extension": false,
      "prefix": "",
      "capabilities": "duckdb|postgres|duckdb_cross_catalog|"
    }
  ],
  "expected_error": "",
  "skip_types": []
}
```

### Test Cases (68 total)

**Single-Catalog (01–17)**:

| # | Name | Covers |
|---|------|--------|
| 01 | basic_table | `@table` directive |
| 02 | table_with_default | `@default` directive |
| 03 | table_with_unique | `@unique` constraint |
| 04 | table_references | `@references` relationships |
| 05 | table_m2m | Many-to-many (`is_m2m: true`) |
| 06 | view_simple | `@view` directive |
| 07 | view_parameterized | Views with `@args` |
| 08 | function | `@function` directive |
| 09 | module_nested | Nested `@module` paths |
| 10 | as_module | `as_module` flag |
| 11 | read_only | `read_only` flag |
| 12 | table_with_join | `@table_function_call_join` |
| 13 | cube | `@cube` OLAP directive |
| 14 | hypertable | `@hypertable` (TimescaleDB) |
| 15 | view_with_function_call | `@function_call` in views |
| 16 | vector | `@vector` directive |
| 17 | vector_embeddings | Vector embeddings |

**Multi-Catalog (20–29)**:

| # | Name | Covers |
|---|------|--------|
| 20 | multi_catalog_basic | Two independent catalogs |
| 21 | multi_catalog_overlapping_modules | Shared module namespaces |
| 22 | multi_catalog_cross_ref | Cross-catalog references with capabilities |
| 23 | multi_catalog_with_extension | Catalogs + extension |
| 24 | multi_catalog_modules_mixed | Mixed `as_module` and regular |
| 25 | complex_airport | 3-catalog complex scenario |
| 26 | extension_cross_catalog_bridge | Extension bridging catalogs |
| 27 | same_schema_different_prefixes | Same schema with different prefixes |
| 28 | function_with_modules | Functions with module organization |
| 29 | function_with_modules_as_module | Functions compiled as module |

**Error Cases (30–37)** — expect compilation to fail:

| # | Name | Expected Error |
|---|------|----------------|
| 30 | error_missing_pk | `@pk` |
| 31 | error_extension_with_table | `data objects` |
| 32 | error_extension_with_function | `functions` |
| 33 | error_cross_catalog_no_capability | `cross-catalog` |
| 34 | error_invalid_reference_target | `not found` |
| 35 | error_reference_field_mismatch | `fields` |
| 36 | error_redefine_system_type | `system type` |
| 37 | error_cube_without_table | `@cube` |

**Extensions (38–46)**:

| # | Name | Covers |
|---|------|--------|
| 38 | extension_basic | Basic extension fields |
| 39 | extension_with_prefix | Extension with prefix |
| 40 | table_struct_aggregation | Struct fields in aggregation types |
| 41 | extension_join | `@join` in extensions |
| 42 | extension_function_call | `@function_call` in extensions |
| 43 | extension_table_function_call_join | `@table_function_call_join` in extensions |
| 44 | extension_references | `@references` in extensions |
| 45 | extension_prefix | Extension prefix handling |
| 46 | extension_cross_source | Cross-source extension fields |

---

## Compare Package

**Location**: `compare/`

Utilities for structural comparison of two GraphQL schemas. Used by compiler parity tests.

Key functions:
- `Compare()` — structural comparison of two schemas
- `SkipSystemTypes()`, `IgnoreDescriptions()`, `IgnoreDirectiveArgs()`, `SkipTypes()` — comparison options
- `KnownIssues()` — track expected differences between compilers

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
