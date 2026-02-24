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

### Module Merging Across Catalogs

When multiple catalogs contribute types to the same module name:

1. First catalog creates the module root type with `@if_not_exists`
2. Second catalog's module root type is skipped (already exists)
3. Both catalogs' query fields are added as extensions to the shared module type

This is handled by `@if_not_exists` on module root types.

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

Query fields are wrapped in the module hierarchy, and only the top-level module field extends the Query root:

```graphql
extend type Query {
  pg_crm: _module_pg_crm_query @module_root(...) @catalog(...)
}
```

### Function Fields

Function query and mutation fields are added to the Query/Mutation root (or module root if `AsModule`):

```graphql
extend type Query {
  my_function(args...): ReturnType @catalog(...)
}
```

## Lifecycle

### Adding an Extension

```
1. Compile extension SDL with IsExtension=true
2. Provider.Update(ctx, compiled) — adds extension types and field extensions
3. Extension fields carry @dependency(name: "ext_name")
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
   — removes all @catalog-tagged types (OBJECT + INPUT_OBJECT + generated)
   — cascade removes @dependency-tagged extension fields
2. Recompile with updated SDL
3. Provider.Update(ctx, compiled) — re-adds all types
```
