# DDL Operations

## Definition Operations

Each type in the source SDL undergoes one of these operations during compilation:

| Operation | Directive | Behavior |
|-----------|-----------|----------|
| **Add** | _(default)_ | Definition added to output. Fails if type already exists. |
| **Drop** | `@drop` | Definition removed from target schema. Not added to output. |
| **Replace** | `@replace` | Definition replaces existing type in target schema. |
| **If Not Exists** | `@if_not_exists` | Definition added only if type doesn't already exist. |

## Extension Operations

Extensions modify existing types by adding or removing fields:

| Operation | Mechanism | Description |
|-----------|-----------|-------------|
| **Field Add** | Extension field without directive | Adds field to existing type |
| **Field Drop** | `@drop` on extension field | Removes field from existing type |
| **Field Replace** | `@replace` on extension field | Replaces field definition |
| **Field Merge** | Multiple extensions to same type | Fields accumulated via `AddExtension()` |
| **Directive Drop** | `@drop_directive(name: "...")` | Removes directive from existing type |

## Provider.Update() Flow

When `Provider.Update(ctx, compiled)` is called, the compiled catalog is applied in three phases:

### 1. Validate

- Checks that definitions don't conflict with existing schema (unless `@replace` or `@if_not_exists`).
- Rejects names starting with `__` (reserved by the GraphQL spec for introspection). This applies to type names, field names, and enum value names. Drop operations are exempt. The system catalog (`_system`) is exempt in the DB provider since it legitimately defines introspection types.

### 2. Apply

Processes the update changeset:

- **toDrop**: Types marked with `@drop` are removed from schema
- **toAdd**: New definitions are inserted into schema
- **extensions**: For each extension:
  - `fieldsToDrop`: Fields removed from target type
  - `fieldsToAdd`: Fields added to target type
  - `fieldsToMerge`: Fields merged (replace if exists, add if not)

### 3. Update Relationships

Rebuilds type relationships (implements, possibleTypes) after schema changes.

## @catalog Tagging

The `@catalog` directive marks types and fields with their source catalog name and engine:

```graphql
directive @catalog(name: String!, engine: String!) on OBJECT | INPUT_OBJECT | FIELD_DEFINITION | INPUT_FIELD_DEFINITION
```

### How @catalog is applied

1. **Source OBJECT types**: `CatalogTagger` rule (PhasePrepare) adds `@catalog` to source types with `@table` or `@view`
2. **Generated types**: Generator rules (PhaseGenerate) add `@catalog` to all generated types:
   - `<Name>_filter` (INPUT_OBJECT)
   - `<Name>_mut_input_data` (INPUT_OBJECT)
   - `<Name>_mut_data` (INPUT_OBJECT)
   - `<Name>_list_filter` (INPUT_OBJECT)
   - `_<Name>_aggregation` (OBJECT)
   - `_<Name>_aggregation_bucket` (OBJECT)
   - `_<Name>_aggregation_sub_aggregation` (OBJECT)
3. **Query/Mutation fields**: Generated field definitions carry `@catalog` on `FIELD_DEFINITION`
4. **Filter extension fields**: Reference filter fields carry `@catalog` on `INPUT_FIELD_DEFINITION`

## DropCatalog

`Provider.DropCatalog(ctx, name, cascade)` removes all schema artifacts belonging to a catalog. The process runs in 5 steps:

### Step 1: Collect definitions to drop

- Types with `@catalog(name: ...)` matching the catalog → full drop
- Types with `@dependency(name: ...)` matching the catalog → cascade drop (requires `cascade=true`)
- Fields with `@catalog(name: ...)` or `@dependency(name: ...)` → partial drop (field removal)
- Enum values, interface implementations, union members belonging to the catalog → partial drop

### Step 2: Drop collected definitions

Removes definitions from `schema.Types` and cleans up `PossibleTypes` / `Implements` references.

### Step 3: Partially drop definitions

Removes collected fields, enum values, interfaces, and union members from surviving definitions.

### Step 4: Module catalog cleanup

Handles the many-to-many relationship between modules and catalogs via `@module_catalog`:

- Removes `@module_catalog(name: "dropped")` directives from all types and fields
- Deletes `@module_root` types that have no remaining `@module_catalog` directives
- Deletes fields that have no remaining `@module_catalog` directives (were previously tracked)

This ensures shared module types survive when only one of their contributing catalogs is removed.

### Step 5: Orphan cleanup

Iteratively removes dangling references left by type deletions:

- Fields whose target type no longer exists in the schema
- Empty `@module_root` types (0 fields remaining)
- Repeats until stable (cascading orphans are handled)

This is a safety net for edge cases not covered by explicit directive tracking.
