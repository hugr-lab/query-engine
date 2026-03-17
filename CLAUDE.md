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
- 014-core-auth-source: Added Go 1.26 + duckdb-go/v2 (CGo, UDF registration), gqlparser/v2 (GraphQL SDL), arrow-go/v18
- 001-client-module: Added Go 1.26 (main module), Go 1.22 (client sub-module) + arrow-go/v18, msgpack/v5, gqlparser/v2 (all pure Go for pkg/types); duckdb-go/v2 (CGo, stays in pkg/db only)
- 001-iceberg-source: Added Go 1.26 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `duckdb-go/v2` (CGo, build tag `duckdb_arrow`), `gqlparser/v2` (GraphQL AST), `apache/arrow-go/v18`, DuckDB iceberg extension
