# Dev Server Configuration

The dev server (`cmd/dev-server`) is configured via environment variables.
A `.env` file in the working directory is loaded automatically.

## Server

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `BIND` | string | `:15000` | Listen address |
| `ADMIN_UI` | bool | `true` | Enable GraphiQL admin UI |
| `ADMIN_UI_FETCH_PATH` | string | _(empty)_ | Custom fetch path for admin UI (defaults to `/query`) |
| `DEBUG` | bool | `false` | Enable debug logging |
| `HTTP_PROFILING` | bool | `false` | Enable pprof endpoints at `/debug/pprof/*` |

## Query Execution

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `ALLOW_PARALLEL` | bool | `true` | Allow parallel query execution |
| `MAX_PARALLEL_QUERIES` | int | `0` | Max concurrent queries (0 = unlimited) |
| `MAX_DEPTH` | int | `0` | Max GraphQL query depth (0 = default 7) |

## Schema Cache

The DB-backed schema provider uses an LRU cache to avoid repeated database reads.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SCHEMA_CACHE_MAX_ENTRIES` | int | `0` | LRU cache max entries (0 = default 10000) |
| `SCHEMA_CACHE_TTL` | duration | `0s` | Cache entry TTL (0 = default 10m) |

## MCP Endpoint

When enabled, the server exposes an MCP (Model Context Protocol) endpoint at `/mcp`
for AI tool integration. Requires an embedder URL for vector search.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `MCP_ENABLED` | bool | `false` | Enable MCP endpoint |
| `EMBEDDER_URL` | string | _(empty)_ | Embedder service URL (required when MCP is enabled) |

The embedder URL format: `http://host:port/path?model=<model>&api_key=<key>&timeout=<duration>`

## DuckDB

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DB_PATH` | string | _(empty)_ | DuckDB file path (empty = in-memory) |
| `DB_MAX_OPEN_CONNS` | int | `0` | Max open connections |
| `DB_MAX_IDLE_CONNS` | int | `0` | Max idle connections |
| `DB_HOME_DIRECTORY` | string | _(empty)_ | DuckDB home directory |
| `DB_ALLOWED_DIRECTORIES` | string | _(empty)_ | Comma-separated allowed directories |
| `DB_ALLOWED_PATHS` | string | _(empty)_ | Comma-separated allowed file paths |
| `DB_ENABLE_LOGGING` | bool | `false` | Enable DuckDB query logging |
| `DB_MAX_MEMORY` | int | `0` | Max memory in GB |
| `DB_MAX_TEMP_DIRECTORY_SIZE` | int | `0` | Max temp directory size in GB |
| `DB_TEMP_DIRECTORY` | string | _(empty)_ | Temporary directory path |
| `DB_WORKER_THREADS` | int | `0` | Number of worker threads |
| `DB_PG_CONNECTION_LIMIT` | int | `0` | PostgreSQL scanner connection limit |
| `DB_PG_PAGES_PER_TASK` | int | `0` | PostgreSQL scanner pages per task |

## CoreDB

CoreDB stores schema metadata, data source definitions, and vector embeddings.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CORE_DB_PATH` | string | _(empty)_ | CoreDB path (empty = in-memory). Supports DuckDB file, `postgres://`, or `s3://` |
| `CORE_DB_READONLY` | bool | `false` | Read-only mode (requires a path) |
| `CORE_DB_S3_ENDPOINT` | string | _(empty)_ | S3 endpoint URL |
| `CORE_DB_S3_REGION` | string | _(empty)_ | S3 region |
| `CORE_DB_S3_KEY` | string | _(empty)_ | S3 access key |
| `CORE_DB_S3_SECRET` | string | _(empty)_ | S3 secret key |
| `CORE_DB_S3_USE_SSL` | bool | `false` | Use SSL for S3 |

## CORS

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CORS_ALLOWED_ORIGINS` | string | _(empty)_ | Comma-separated allowed origins (empty = CORS disabled, `*` = allow all) |
| `CORS_ALLOWED_HEADERS` | string | `Content-Type, Authorization, x-api-key, ...` | Comma-separated allowed headers |
| `CORS_ALLOWED_METHODS` | string | `GET, POST, PUT, DELETE, OPTIONS` | Comma-separated allowed methods |

## Authentication

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `ALLOWED_ANONYMOUS` | bool | `true` | Allow anonymous access |
| `ANONYMOUS_ROLE` | string | `admin` | Role assigned to anonymous users |
| `ALLOWED_DB_API_KEYS` | bool | `false` | Enable DB-managed API keys |
| `SECRET_KEY` | string | _(empty)_ | Secret API key (sent via `x-hugr-secret-key` header) |
| `AUTH_CONFIG_FILE` | string | _(empty)_ | Path to auth config file (JSON or YAML) |

### Auth Config File

When `AUTH_CONFIG_FILE` is set, the server loads authentication rules from a JSON/YAML file:

```json
{
  "db_api_keys_enabled": true,
  "secret_key": "...",
  "anonymous": {
    "allowed": false,
    "role": "viewer"
  },
  "api_keys": {
    "my-service": {
      "key": "sk-...",
      "header": "x-api-key",
      "default_role": "editor",
      "headers": {
        "role": "x-role",
        "user_id": "x-user-id",
        "user_name": "x-user-name"
      }
    }
  },
  "jwt": {
    "auth0": {
      "issuer": "https://example.auth0.com/",
      "audience": "https://api.example.com",
      "jwks_uri": "https://example.auth0.com/.well-known/jwks.json",
      "roles_claim": "https://example.com/roles"
    }
  }
}
```

## Cache

### L1 (In-Memory — BigCache)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CACHE_TTL` | duration | _(empty)_ | Default cache TTL |
| `CACHE_L1_ENABLED` | bool | `false` | Enable L1 in-memory cache |
| `CACHE_L1_MAX_SIZE` | int | `0` | Max cache size in MB |
| `CACHE_L1_MAX_ITEM_SIZE` | int | `100` | Max single item size in MB |
| `CACHE_L1_SHARDS` | int | `64` | Number of shards |
| `CACHE_L1_CLEAN_TIME` | duration | _(empty)_ | Cleanup interval |
| `CACHE_L1_EVICTION_TIME` | duration | _(empty)_ | Item eviction time |

### L2 (Redis / Memcached)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CACHE_L2_ENABLED` | bool | `false` | Enable L2 distributed cache |
| `CACHE_L2_BACKEND` | string | _(empty)_ | Backend: `redis` or `memcached` |
| `CACHE_L2_ADDRESSES` | string | _(empty)_ | Comma-separated server addresses |
| `CACHE_L2_DATABASE` | int | `0` | Redis database number |
| `CACHE_L2_USERNAME` | string | _(empty)_ | Username |
| `CACHE_L2_PASSWORD` | string | _(empty)_ | Password |

## Command-Line Flags

| Flag | Description |
|------|-------------|
| `-install` | Install required DuckDB extensions and exit |

## Example `.env`

```bash
BIND=:15000
CORE_DB_PATH=./core.db
CORE_DB_READONLY=false
DEBUG=false

# MCP + embedder
MCP_ENABLED=true
EMBEDDER_URL=http://localhost:8080/embed?model=text-embedding-3-small&api_key=sk-...

# Authentication
ALLOWED_ANONYMOUS=false
AUTH_CONFIG_FILE=./auth.json

# Cache
CACHE_L1_ENABLED=true
CACHE_L1_MAX_SIZE=256
CACHE_TTL=5m
```
