# query-engine Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-21

## Active Technologies
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST) (002-rule-based-compiler)
- N/A (schema compilation produces in-memory DDL feed) (002-rule-based-compiler)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag for tests) (003-multi-catalog-compilation)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag) (004-compiler-integration-tests)
- N/A (all in-memory schema compilation) (004-compiler-integration-tests)
- Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `pkg/schema/compiler` (rule-based compiler), `pkg/schema/static` (Provider with `Update()`/`DropCatalog()`) (005-source-adapters)

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
- 005-source-adapters: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `pkg/schema/compiler` (rule-based compiler), `pkg/schema/static` (Provider with `Update()`/`DropCatalog()`)
- 004-compiler-integration-tests: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag)
- 003-multi-catalog-compilation: Added Go 1.25 (with `iter.Seq`, `iter.Seq2`, range-over-func) + `gqlparser/v2` (GraphQL AST), `duckdb-go/v2` (build tag for tests)


<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
