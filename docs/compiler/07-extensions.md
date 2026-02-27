# Extensions & Modules

## Extension Compilation

Extension mode is activated with `IsExtension: true` in compile options:

```go
opts := base.Options{
    Name:        "my_extension",
    EngineType:  "duckdb",
    IsExtension: true,
}
```

### Validation Rules

`ExtensionValidator` (PhaseValidate) enforces:
- Only `@view` types allowed (no `@table`, `@module`, `@function`)
- Views must have SQL definitions (`@view(sql: "...")`)

### Dependency Tagging

All extension fields get `@dependency(name: "<extension_name>")` automatically via `AddExtension()`. This enables cascade removal when the extension's dependencies are dropped.

### DependentCompiledCatalog

Extension compilation returns a `DependentCompiledCatalog` that tracks dependencies:

```go
depCatalog := compiled.(base.DependentCompiledCatalog)
deps := depCatalog.Dependencies() // e.g., ["base_catalog"]
```

Dependencies are collected by:
- `DependencyCollector` rule (PhaseValidate) — collects `@dependency` directives from source types
- Automatic tracking via cross-type references in `CompilationContext.RegisterDependency()`

## Module System

Modules organize types into namespaced hierarchies. Activated with `@module(name: "...")` on source types or `AsModule: true` in compile options.

### Module Assembly (`ModuleAssembler`)

The `ModuleAssembler` (PhaseAssemble) creates module root types:

For a catalog with `AsModule: true` and `Name: "pg_crm"` containing types with `@module(name: "crm")`:

```
Query
  └── pg_crm: _module_pg_crm_query
        └── crm: _module_crm_query
              ├── Customer: [Customer]
              ├── Customer_by_pk: Customer
              ├── Order: [Order]
              └── Order_by_pk: Order

Mutation (if not ReadOnly)
  └── pg_crm: _module_pg_crm_mutation
        └── crm: _module_crm_mutation
              ├── insert_Customer: Customer
              └── insert_Order: Order
```

### Module Nesting

Dot-separated module names create nested hierarchies:

```graphql
type Flight @table(name: "flights") @module(name: "transport.air") { ... }
```

Creates: `_module_<catalog>_query → transport → air → Flight`

### Module–Catalog Tracking (`@module_catalog`)

Module types and wiring fields have a **many-to-many** relationship with catalogs. A single module type (e.g., `_module_crm_query`) can be shared across multiple catalogs, and a single catalog can contribute to multiple modules.

This is tracked via the repeatable `@module_catalog(name: String!)` directive:

```graphql
directive @module_catalog(name: String!) repeatable on OBJECT | FIELD_DEFINITION
```

**On module type definitions** — tracks which catalogs contribute to this module:
```graphql
type _module_crm_query @module_root(name: "crm", type: QUERY)
  @module_catalog(name: "pg_crm") @module_catalog(name: "support") { ... }
```

**On wiring fields** — tracks which catalogs use this wiring path:
```graphql
type Query {
  crm: _module_crm_query @module_catalog(name: "pg_crm") @module_catalog(name: "support")
}
```

Data fields on module types (pointing to actual data objects) still use `@catalog(name, engine)` since they belong to exactly one catalog.

### Module Merging Across Catalogs

When multiple catalogs contribute types to the same module name:

1. First catalog creates the module root type — gets `@module_catalog(name: "first")`
2. Second catalog finds the type already exists in the provider — adds `@module_catalog(name: "second")` via extension merge
3. Both catalogs' data fields are added as extensions to the shared module type with their respective `@catalog`
4. Wiring fields follow the same pattern — first catalog creates the field with `@module_catalog`, subsequent catalogs merge their `@module_catalog` directive onto the existing field

### Module Cleanup on DropCatalog

When a catalog is dropped, `@module_catalog` directives are handled in DropCatalog step 4:

1. Remove `@module_catalog(name: "dropped")` from all types and fields
2. If a `@module_root` type has no remaining `@module_catalog` → delete the type
3. If a wiring field has no remaining `@module_catalog` → delete the field
4. Orphan cleanup catches any remaining dangling references

Example with shared module `crm` (catalogs `pg_crm` + `support`):

```
Before:  _module_crm_query @module_catalog(pg_crm) @module_catalog(support)
           field Customer @catalog(pg_crm)
           field Ticket @catalog(support)

Drop pg_crm:
  → @module_catalog(pg_crm) removed from type and wiring fields
  → field Customer removed (@catalog match)
  → _module_crm_query survives (still has @module_catalog(support))
  → Query.crm survives (still has @module_catalog(support))

Drop support:
  → @module_catalog(support) removed — none left
  → field Ticket removed (@catalog match)
  → _module_crm_query deleted (no @module_catalog remaining)
  → Query.crm deleted (no @module_catalog remaining)
```

## Root Type Assembly (`RootTypeAssembler`)

The `RootTypeAssembler` (PhaseAssemble) creates extensions to the global `Query` and `Mutation` types:

### Without AsModule

Query fields are added directly to the Query root:

```graphql
extend type Query {
  Widget: [Widget] @query(...) @catalog(...)
  Widget_by_pk: Widget @query(...) @catalog(...)
}
```

### With AsModule

Query fields are wrapped in the module hierarchy. The top-level module wiring field extends the Query root with `@module_catalog` (not `@catalog`, since it may be shared):

```graphql
extend type Query {
  pg_crm: _module_pg_crm_query @module_catalog(name: "pg_crm")
}
```

### Function Fields

Function query and mutation fields are added to the Query/Mutation root (or module root if `AsModule`):

```graphql
extend type Query {
  my_function(args...): ReturnType @catalog(...)
}
```

The `function` gateway field on Query/Mutation (pointing to the `Function`/`MutationFunction` type) uses `@module_catalog` since multiple catalogs can contribute functions:

```graphql
extend type Query {
  function: Function @module_catalog(name: "catalog_with_functions")
}
```

## Lifecycle

### Adding a Catalog

```
1. Compile SDL with catalog options (Name, EngineType, AsModule, etc.)
2. Provider.Update(ctx, compiled)
   — adds data types with @catalog(name: "catalog")
   — creates or extends module types with @module_catalog(name: "catalog")
   — creates or merges module wiring fields with @module_catalog(name: "catalog")
```

### Adding an Extension

```
1. Compile extension SDL with IsExtension=true
2. Provider.Update(ctx, compiled) — adds extension types and field extensions
3. Extension fields carry @dependency(name: "ext_name")
```

### Removing a Catalog

```
1. Provider.DropCatalog(ctx, "catalog_name", cascade=true)
   — Step 1-3: removes @catalog-tagged types and fields
   — Step 3: cascade removes @dependency-tagged extension types/fields
   — Step 4: removes @module_catalog(name: "catalog_name") from shared module types/fields
     • module types with no remaining @module_catalog are deleted
     • wiring fields with no remaining @module_catalog are deleted
   — Step 5: orphan cleanup removes dangling type references
2. Schema remains valid for remaining catalogs
```

### Removing an Extension

```
1. Provider.DropCatalog(ctx, "ext_name", cascade=true)
2. Types with @catalog(name: "ext_name") removed
3. Fields with @dependency(name: "ext_name") removed (cascade)
```

### Reloading a Catalog

```
1. Provider.DropCatalog(ctx, "catalog_name", cascade=true)
   — removes all catalog artifacts while preserving shared modules used by other catalogs
2. Recompile with updated SDL
3. Provider.Update(ctx, compiled) — re-adds all types, re-registers @module_catalog
```
