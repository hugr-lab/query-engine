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
- 007-static-provider-init: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST parsing/validation), `pkg/schema/compiler/base` (system SDL sources), `pkg/schema/types` (scalar registry)
- 006-docker-e2e-testing: Added Go 1.25 (query engine), Bash (test runner scripts) + Docker, Docker Compose, curl, jq, duckdb CLI
- 005-source-adapters: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `pkg/schema/compiler` (rule-based compiler), `pkg/schema/static` (Provider with `Update()`/`DropCatalog()`)


<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
