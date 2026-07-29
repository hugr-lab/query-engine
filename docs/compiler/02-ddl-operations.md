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

## How the operations are applied

These directives are read when a source is INGESTED: the pipeline resolves each
definition against what the catalog storage already holds, and the storage writes
the result. There is no separate apply step over a compiled schema — a source's
rows are replaced wholesale in one transaction, so a definition the source
stopped declaring cannot survive its reload.

### Validation

- Checks that definitions don't conflict with the stored model (unless `@replace` or `@if_not_exists`).
- Rejects names starting with `__` (reserved by the GraphQL spec for introspection). This applies to type names, field names, and enum value names. Drop operations are exempt.

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

## Removing a source

There is no DropCatalog walk over a compiled schema any more — removal is a
delete of the source's rows.

`RemoveCatalog(ctx, name)` deletes the source's entities, its metadata, its
dependency edges and its seed annotations, in one transaction. What used to be
five steps of untangling a merged AST is now a property of the storage:

- **Its own artifacts** go with its rows — every entity carries its
  `data_source`.
- **Fields it contributed to OTHER sources' objects** go too: they are stored
  with `dependency_data_source` set to the contributing source.
- **Shared modules survive.** A module exists while any source contributes to
  it, which the module ↔ source table records directly; nothing has to be
  reference-counted through directives.
- **Dependent sources are SUSPENDED, not dropped.** Their rows stay and their
  curation with them; `reactivateSuspended` restores them when the dependency
  returns.

`@catalog`, `@dependency` and `@module_catalog` are still emitted on the served
schema — they describe provenance to a client. Nothing reads them back to
decide what to delete.

