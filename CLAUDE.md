# query-engine Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-21

## Active Technologies
- Go 1.26 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag: `duckdb_arrow`), `apache/arrow-go/v18`
- Go 1.26 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `duckdb-go/v2` (CGo bindings, build tag: `duckdb_arrow`), `gqlparser/v2` (GraphQL AST), `apache/arrow-go/v18` (001-ducklake-source)
- DuckDB (embedded, via ATTACH) + CoreDB (DuckDB or PostgreSQL for schema metadata) (001-ducklake-source)
- Go 1.26 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `duckdb-go/v2` (CGo, build tag `duckdb_arrow`), `gqlparser/v2` (GraphQL AST), `apache/arrow-go/v18`, DuckDB iceberg extension (001-iceberg-source)
- DuckDB embedded (via ATTACH for Iceberg catalogs), PostgreSQL (CoreDB for schema metadata, optional DuckLake metadata backend) (001-iceberg-source)
- Go 1.26 (main module), Go 1.22 (client sub-module) + arrow-go/v18, msgpack/v5, gqlparser/v2 (all pure Go for pkg/types); duckdb-go/v2 (CGo, stays in pkg/db only) (001-client-module)
- N/A (refactoring, no storage changes) (001-client-module)
- Go 1.26 + duckdb-go/v2 (CGo, UDF registration), gqlparser/v2 (GraphQL SDL), arrow-go/v18 (014-core-auth-source)
- N/A (reads from request context only) (014-core-auth-source)
- Go 1.26 (with `iter.Seq`, range-over-func) + `duckdb-go/v2` (CGo, build tag `duckdb_arrow`), `gqlparser/v2` (GraphQL AST), `apache/arrow-go/v18`, `itchyny/gojq` (JQ transforms) (001-timezone-support)
- DuckDB embedded (primary), PostgreSQL (CoreDB for metadata, data source) (001-timezone-support)
- Go 1.26 (with `iter.Seq`, range-over-func) + `duckdb-go/v2` (CGo, `duckdb_arrow`), `hugr-lab/airport-go` v0.2.1 (Arrow Flight gRPC), `gqlparser/v2` (GraphQL AST), `apache/arrow-go/v18` (001-hugr-apps)
- DuckDB embedded (primary), PostgreSQL (CoreDB + app databases) (001-hugr-apps)
- Go 1.26 + `duckdb-go/v2` (CGo, `duckdb_arrow`), `gqlparser/v2`, `apache/arrow-go/v18` + DuckDB embedded, existing `Source`/`RuntimeSource` interfaces, `ScalarFunctionWithArgs` UDF pattern (001-ai-model-sources)
- DuckDB (embedded), PostgreSQL (CoreDB for metadata) (001-ai-model-sources)
- Redis 6.0+ (key-value), DuckDB (embedded), PostgreSQL (CoreDB) (001-redis-store-ratelimit)
- Go 1.26 with `iter.Seq`, range-over-func + `duckdb-go/v2` (CGo, build tag `duckdb_arrow`), `gqlparser/v2` (GraphQL AST), `apache/arrow-go/v18`, `gorilla/websocket` (existing dep), `redis/go-redis/v9` (existing dep for store) (001-graphql-subscriptions)
- DuckDB embedded (primary query engine), PostgreSQL CoreDB (schema metadata), Redis (store source) (001-graphql-subscriptions)
- Go 1.26 (with `iter.Seq`, range-over-func) + `duckdb-go/v2` (CGo, `duckdb_arrow`), `gqlparser/v2`, `gorilla/websocket`, `apache/arrow-go/v18` (001-client-impersonation)
- N/A (reads from existing permission system via `core.roles_by_pk`) (001-client-impersonation)
- DuckDB embedded + PostgreSQL CoreDB (roles table migration in hugr repo) (001-perm-impersonation)
- Go 1.26 (with `iter.Seq`, range-over-func) + `gqlparser/v2` (GraphQL AST + directive parsing), `duckdb-go/v2` (CGo, `duckdb_arrow`), `apache/arrow-go/v18` (001-function-arg-placeholders)
- DuckDB embedded + PostgreSQL CoreDB (no migration needed — `_schema_arguments.directives` JSON column already exists) (001-function-arg-placeholders)
- Go 1.26 (with `iter.Seq`, range-over-func) + `gqlparser/v2` (GraphQL AST + directive parsing), `duckdb-go/v2` (CGo, `duckdb_arrow`), `apache/arrow-go/v18`, `airport-go` (Arrow Flight protocol for hugr-app catalog) (001-hugrapp-struct-args)
- DuckDB embedded; no schema changes (001-hugrapp-struct-args)
- Go 1.26 with `duckdb_arrow` build tag + `duckdb-go/v2`, `gqlparser/v2`, `apache/arrow-go/v18` (001-streaming-tool-calls)
- N/A (no storage changes) (001-streaming-tool-calls)

## Project Structure

```text
src/
tests/
```

## Code Style

Go 1.26: Follow standard conventions

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->

## Recent Changes
- 001-streaming-tool-calls: Added Go 1.26 with `duckdb_arrow` build tag + `duckdb-go/v2`, `gqlparser/v2`, `apache/arrow-go/v18`
- 001-hugrapp-struct-args: Added Go 1.26 (with `iter.Seq`, range-over-func) + `gqlparser/v2` (GraphQL AST + directive parsing), `duckdb-go/v2` (CGo, `duckdb_arrow`), `apache/arrow-go/v18`, `airport-go` (Arrow Flight protocol for hugr-app catalog)
- 001-function-arg-placeholders: Added Go 1.26 (with `iter.Seq`, range-over-func) + `gqlparser/v2` (GraphQL AST + directive parsing), `duckdb-go/v2` (CGo, `duckdb_arrow`), `apache/arrow-go/v18`
