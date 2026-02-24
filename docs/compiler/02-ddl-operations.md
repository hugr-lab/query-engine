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

Checks that definitions don't conflict with existing schema (unless `@replace` or `@if_not_exists`).

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

`Provider.DropCatalog(ctx, name, cascade)` removes all types and fields tagged with `@catalog(name: ...)`:

1. Scans all schema types for `@catalog` directive matching the given name
2. Removes matched definitions (OBJECT, INPUT_OBJECT)
3. Removes matched fields from extension types (FIELD_DEFINITION, INPUT_FIELD_DEFINITION)
4. When `cascade=true`, also removes types with `@dependency(name: ...)` matching the catalog

This ensures clean removal of an entire catalog including all generated filter, mutation input, and aggregation types.
