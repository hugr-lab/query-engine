# Known Issues

## 1. Stale indexes on `_schema_*` tables after schema upgrade (DuckDB)

When the core runtime source (`core-db`) upgrades the `_schema_*` table schemas at startup, DuckDB indexes on those tables may become stale. This can cause modules (including system modules) to fail loading if their version changed and they were recompiled.

**Symptoms**: Modules don't load after engine upgrade; catalog reload silently skips updates.

**Status**: the catalog no longer lives in `_schema_*` — it is stored as entities in the `catalog` schema and the GraphQL schema is generated on the fly. This applies only to a CoreDB still carrying the old tables.

**Affected table**: `_schema_module_type_catalogs`

**Workaround** (DuckDB CLI):

```sql
-- 1. Create a copy
CREATE TABLE _schema_module_type_catalogs_bak AS
  SELECT * FROM _schema_module_type_catalogs;

-- 2. Clear the original (resets indexes)
DELETE FROM _schema_module_type_catalogs;

-- 3. Restore data
INSERT INTO _schema_module_type_catalogs
  SELECT * FROM _schema_module_type_catalogs_bak;

-- 4. Drop the backup
DROP TABLE _schema_module_type_catalogs_bak;
```

Then restart the engine.

## 2. System and scalar type schema updates skipped on reload — RESOLVED

`InitSystemTypes` persisted the system layer into `_schema_*` and could skip the update when definitions changed between releases. The entity storage never persists that layer: scalars, introspection and `@system` types are assembled from the binary at startup and held in memory, so they cannot go stale relative to it.

## 3. `import_descriptions(recompute_embeddings: true)` not yet implemented

The `core.db.import_descriptions` mutation accepts a `recompute_embeddings` parameter, but the actual reindexing logic is not wired up from the UDF context. The parameter is accepted and a placeholder message is returned.

**Workaround**: Use `include_embeddings: true` (default) to copy existing vectors from the source file, or reindex after import — `core.catalog.reindex_embeddings`, or `Store.ReindexEmbeddings` programmatically.

**Status**: Requires access to the embedding provider from the UDF execution context.
