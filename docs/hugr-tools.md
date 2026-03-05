# hugr-tools

CLI utilities for managing Hugr schema metadata.

```
hugr-tools <command> [flags]
```

## Global Flags

All commands share these connection flags:

| Flag | Env Variable | Default | Description |
|------|-------------|---------|-------------|
| `--url` | `HUGR_URL` | `http://localhost:15000/ipc` | Engine GraphQL endpoint |
| `--secret` | `HUGR_SECRET` | _(empty)_ | API key for authentication |
| `--secret-header` | `HUGR_SECRET_HEADER` | `x-api-key` | API key header name |
| `--timeout` | | `30s` | Request timeout |

## Commands

### `summarize`

AI-powered schema summarization. Generates short/long descriptions for types, fields, functions, data sources, and modules using an LLM.

**Full pipeline** (no entity flags):

Runs 4 phases sequentially, summarizing all unsummarized entities:

1. Data objects (tables/views) — parallel
2. Functions — parallel
3. Data sources (catalogs) — parallel
4. Modules — sequential (sub-modules before parents)

Each phase shows a progress bar with ETA.

**Single-entity mode:**

| Flag | Description |
|------|-------------|
| `--type <name>` | Summarize a single table or view |
| `--function <type.field>` | Summarize a single function |
| `--module <name>` | Summarize a single module |
| `--source <name>` | Summarize a single data source |

**LLM flags:**

| Flag | Env Variable | Default | Description |
|------|-------------|---------|-------------|
| `--provider` | `SUMMARIZE_PROVIDER` | `openai` | LLM provider: `openai`, `anthropic`, `custom` |
| `--model` | `SUMMARIZE_MODEL` | `gpt-4o-mini` | LLM model name |
| `--base-url` | `SUMMARIZE_BASE_URL` | _(empty)_ | Custom LLM API URL (required for `custom` provider) |
| `--api-key` | `SUMMARIZE_API_KEY` | _(empty)_ | LLM API key (required) |
| `--max-connections` | | `5` | Concurrent LLM requests |
| `--llm-timeout` | | `60s` | Per-request LLM timeout |
| `--catalog` | | _(empty)_ | Only summarize this catalog (default: all) |

**Example:**

```bash
hugr-tools summarize \
  --api-key sk-... \
  --provider openai \
  --model gpt-4o-mini \
  --max-connections 10
```

### `reindex`

Recompute vector embeddings for schema entities. Requires an embedder service configured on the engine.

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | _(empty)_ | Reindex single type (+ its fields); empty = all |
| `--batch-size` | `50` | Batch size (1-200) |

**Example:**

```bash
hugr-tools reindex --batch-size 100
hugr-tools reindex --name my_table
```

### `schema-info`

Display a human-readable schema overview: tables, views, functions, and submodules.

| Flag | Default | Description |
|------|---------|-------------|
| `--module` | _(empty)_ | Module to inspect (default: root) |
| `--format` | `table` | Output format: `table` or `json` |

**Example:**

```bash
hugr-tools schema-info
hugr-tools schema-info --module geo --format json
```
