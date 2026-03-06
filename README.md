# The Hugr query-engine

The Hugr query engine is a part of the Hugr project, which provides a powerful and flexible GraphQL query engine designed to work with various data sources. It supports features like caching, authentication, and schema extensions, making it suitable for building scalable and efficient applications.

See the [Hugr documentation](https://hugr-lab.github.io) for more details.

## Features

- HTTP handlers for GraphQL API (IPC multipart/mixed with Arrow + JSON)
- GraphiQL UI to execute queries
- GraphQL schema definition and rule-based compilation
- Data sources management (DuckDB, PostgreSQL, SQLite, MySQL, S3/Delta/Iceberg)
- Schema extensions to add sub-queries to existing data sources
- Schema catalog module with DB-backed metadata, vector embeddings, and LLM summarization
- MCP (Model Context Protocol) endpoint for AI tool integration
- Authentication and authorization (API keys, JWT/JWKS, DB-managed keys)
- Caching: L1 (in-memory BigCache) and L2 (Redis or Memcached)
- Query parsing, validation, and parallel execution
- Result transformation (jq expressions)

## Architecture

```
cmd/dev-server/       Dev server with env-based configuration
cmd/hugr-tools/       CLI utilities (summarize, reindex, schema-info)
pkg/catalog/          Schema catalog: compiler, static/db providers
pkg/data-sources/     Runtime data source management
pkg/planner/          Query planner and SQL generation
pkg/cache/            Two-level caching (L1 + L2)
pkg/auth/             Authentication middleware
pkg/mcp/              MCP server integration
docs/                 Internal documentation
```

## Quick Start

### Build

```bash
# Dev server
CGO_CFLAGS="-O1 -g" go build -tags=duckdb_arrow -o hugr ./cmd/dev-server

# CLI tools (summarize, reindex, schema-info)
CGO_CFLAGS="-O1 -g" go build -tags=duckdb_arrow -o hugr-tools ./cmd/hugr-tools
```

### Install DuckDB extensions

```bash
./hugr -install
```

### Run

```bash
# Minimal (in-memory)
./hugr

# With persistent CoreDB
CORE_DB_PATH=./core.db ./hugr

# With MCP endpoint + embeddings
CORE_DB_PATH=./core.db MCP_ENABLED=true \
  EMBEDDER_URL='http://localhost:8080/embed?model=text-embedding-3-small&api_key=sk-...' \
  ./hugr
```

### Summarize schema with AI

```bash
# Generate descriptions for all schema entities using an LLM
hugr-tools summarize --api-key sk-... --provider openai --model gpt-4o-mini

# Recompute vector embeddings after summarization
hugr-tools reindex --batch-size 100
```

The server reads configuration from environment variables (or a `.env` file).
See [docs/configuration.md](docs/configuration.md) for the full reference.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/configuration.md](docs/configuration.md) | Dev server configuration reference |
| [docs/hugr-tools.md](docs/hugr-tools.md) | CLI utilities (summarize, reindex, schema-info) |
| [docs/compiler/01-overview.md](docs/compiler/01-overview.md) | Compiler architecture |
| [docs/compiler/02-ddl-operations.md](docs/compiler/02-ddl-operations.md) | DDL operations |
| [docs/compiler/03-directives.md](docs/compiler/03-directives.md) | Directive reference |
| [docs/compiler/04-tables-views.md](docs/compiler/04-tables-views.md) | Tables and views |
| [docs/compiler/05-functions.md](docs/compiler/05-functions.md) | Functions |
| [docs/compiler/06-subqueries.md](docs/compiler/06-subqueries.md) | Subqueries and references |
| [docs/compiler/07-extensions.md](docs/compiler/07-extensions.md) | Extensions and modules |

## Dependencies

Key packages used:

- [github.com/duckdb/duckdb-go/v2](https://github.com/duckdb/duckdb-go) — DuckDB driver
- [github.com/apache/arrow-go/v18](https://github.com/apache/arrow-go) — Apache Arrow
- [github.com/vektah/gqlparser/v2](https://github.com/vektah/gqlparser) — GraphQL parser
- [github.com/paulmach/orb](https://github.com/paulmach/orb) — Geometry types
- [github.com/eko/gocache/v4](https://github.com/eko/gocache) — Cache abstraction
- [github.com/golang-jwt/jwt/v5](https://github.com/golang-jwt/jwt) — JWT authentication
- [github.com/mark3labs/mcp-go](https://github.com/mark3labs/mcp-go) — MCP server
- [github.com/tmc/langchaingo](https://github.com/tmc/langchaingo) — LLM integration (hugr-tools)
- [github.com/itchyny/gojq](https://github.com/itchyny/gojq) — jq transformation
- [github.com/hashicorp/golang-lru/v2](https://github.com/hashicorp/golang-lru) — LRU cache
