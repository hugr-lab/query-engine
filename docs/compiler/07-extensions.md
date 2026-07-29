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
- Automatic tracking via cross-type references (`DependencyCollector`, recorded on the source)

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

### Shared modules when a source is removed

A module belongs to every source that contributes to it, and the storage
records that directly (one row per module × source). Removing a source deletes
its rows; the module survives while another source still has one.

`@module_catalog` is emitted on the served module roots so a client can see
which sources feed a module. It is not what the removal consults.

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
1. Ingest the SDL with the source's options (Name, EngineType, AsModule, ...)
2. The storage writes the physical model: data objects tagged
   @catalog(name: "source"), their fields, relations, functions, and the
   modules they belong to
3. The module roots - and their @module_catalog attribution - are synthesized
   on READ from the modules a source contributes to
   — creates or merges module wiring fields with @module_catalog(name: "catalog")
```

### Adding an Extension

```
1. Ingest the extension SDL with IsExtension=true
2. The storage writes its own objects plus the fields it contributes to OTHER
   sources' objects, attributed to the source the DATA comes from
3. Extension fields carry @dependency(name: "ext_name"), which gates them: the
   field is served only while every declared dependency is active
```

### Removing a Catalog

```
1. RemoveCatalog(ctx, "source_name") deletes the source's rows; sources that
   DEPEND on it are suspended rather than dropped, so their curation survives
2. Modules shared with other sources stay - a module exists while any source
   contributes to it
3. Reactivation is the Add path again: an unchanged version just clears the flag
```
