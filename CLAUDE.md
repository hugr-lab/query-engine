# query-engine Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-21

## Active Technologies
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST) (002-rule-based-compiler)
- N/A (schema compilation produces in-memory DDL feed) (002-rule-based-compiler)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag for tests) (003-multi-catalog-compilation)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag) (004-compiler-integration-tests)
- N/A (all in-memory schema compilation) (004-compiler-integration-tests)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `pkg/schema/compiler` (rule-based compiler), `pkg/schema/static` (Provider with `Update()`/`DropCatalog()`) (005-source-adapters)
- Go 1.25 (query engine), Bash (test runner scripts) + Docker, Docker Compose, curl, jq, duckdb CLI (006-docker-e2e-testing)
- PostgreSQL 16 (PostGIS), DuckDB (in-process, file-based), in-memory CoreDB (006-docker-e2e-testing)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST parsing/validation), `pkg/schema/compiler/base` (system SDL sources), `pkg/schema/types` (scalar registry) (007-static-provider-init)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag), `pkg/types` (runtime parsing) (008-sdl-migration)
- N/A (in-memory schema AST manipulation) (008-sdl-migration)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag for tests), `crypto/sha256` (version hashing) (001-sources-cleanup-docs)
- N/A (in-memory schema compilation) (001-sources-cleanup-docs)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST parsing), `duckdb-go/v2` (build tag for tests), `crypto/sha256` (version hashing) (010-incremental-schema-compilation)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST) + `gqlparser/v2/ast` (AST types), `pkg/catalog/compiler/base` (HugrType constants, directive name constants) (011-db-schema-serialization)
- N/A (this package generates DDL strings; no direct DB access) (011-db-schema-serialization)
- CoreDB (DuckDB / attached PostgreSQL) — DDL added to existing `schema.sql` init + migration in `hugr/migrations/0.0.9/` (011-db-schema-serialization)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `hashicorp/golang-lru/v2` (LRU cache with TTL), `duckdb-go/v2` (build tag: `duckdb_arrow`), `pkg/catalog/db/schema` (Spec 01 serialization), `pkg/db` (Pool, transaction support) (012-db-provider-core)
- CoreDB `_schema_*` tables (DuckDB in-memory/file or attached PostgreSQL) (012-db-provider-core)

- Go 1.25 + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag), `apache/arrow-go/v18` (001-compiler-generators-decouple)

## Project Structure

```text
src/
tests/
```

## Commands

# Add commands for Go 1.25

## Code Style

Go 1.25: Follow standard conventions

## Recent Changes
- 012-db-provider-core: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `hashicorp/golang-lru/v2` (LRU cache with TTL), `duckdb-go/v2` (build tag: `duckdb_arrow`), `pkg/catalog/db/schema` (Spec 01 serialization), `pkg/db` (Pool, transaction support)
- 011-db-schema-serialization: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST) + `gqlparser/v2/ast` (AST types), `pkg/catalog/compiler/base` (HugrType constants, directive name constants)
- 011-db-schema-serialization: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST) + `gqlparser/v2/ast` (AST types), `pkg/catalog/compiler/base` (HugrType constants, directive name constants)


<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
