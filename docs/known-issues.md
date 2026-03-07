# Known Issues

## 1. Stale indexes on `_schema_*` tables after schema upgrade (DuckDB)

When the core runtime source (`core-db`) or the catalog DB provider (`catalog/db`) upgrades the `_schema_*` table schemas at startup, DuckDB indexes on those tables may become stale. This can cause modules (including system modules) to fail loading if their version changed and they were recompiled.

**Symptoms**: Modules don't load after engine upgrade; `InitSystemTypes` or catalog reload silently skips updates.

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

## 2. System and scalar type schema updates skipped on reload

When system or scalar type definitions change between releases, `InitSystemTypes` may skip the update step, leaving the DB schema out of sync with the compiled types. This is a separate issue that will be addressed independently.

**Symptoms**: New or modified scalar/system types not reflected in the running schema after upgrade.

**Status**: To be investigated separately.
