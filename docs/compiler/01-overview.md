# Schema Architecture

## Overview

A data source describes itself in GraphQL SDL. The engine turns that into two
different things, in two different places:

- **on WRITE** — `pkg/catalog/ingest` validates and prepares the source's SDL,
  and `pkg/catalog/store` persists the resulting PHYSICAL model as rows in the
  CoreDB `catalog.*` tables: data objects, their fields, relations, functions,
  modules, residual types.
- **on READ** — `pkg/catalog/store/gen_*.go` generates the SERVED GraphQL
  surface from those rows on demand: filters, mutation inputs, aggregations,
  navigation fields, module roots.

Nothing in between is stored. There is no compiled schema sitting in a table
waiting to be served; the schema you introspect is built from the logical model
each time it is asked for, and cached by name.

## Data flow

```
Catalog source (file / URI / DB / string / merged)
  → catsrc.Catalog (definitions, extensions, compile options, version)
  → ingest.New(ingest.Default()...).Compile(ctx, store, source, opts)
      VALIDATE → PREPARE → [FINALIZE validators]
      (mutates the source's definitions IN PLACE)
  → store.collect(source)          — the physical model
  → store.writeSource(...)         — one transaction, catalog.* rows
                ⋮
  → store.ForName(ctx, name)       — generation on read
  → ast.Definition
```

`Compile` returns only what cannot be applied in place: the extensions
(cross-source `extend type`, module roots) and the `@dependency` set. The
storage reads the definitions from the SOURCE, which the pipeline has just
rewritten — that is the contract between the two halves.

## The write pipeline

The phase machinery still has five phases; two of them have no rules, and an
empty phase is skipped.

| Phase | Purpose | Rules |
|-------|---------|-------|
| **VALIDATE** | The source's SDL is well formed and internally consistent | `ExtensionValidator`, `DependencyCollector`, `SourceValidator`, `DefinitionValidator` |
| **PREPARE** | Merge the source's own extensions, tag `@catalog`, apply the prefix | `InternalExtensionMerger`, `CatalogTagger`, `PrefixPreparer` |
| **GENERATE** | — | *(none: generation happens on read)* |
| **ASSEMBLE** | — | *(none: module roots are synthesized on read)* |
| **FINALIZE** | Check the declarations that reach beyond one field | `JoinValidator`, `FunctionCallValidator` |

The two FINALIZE validators run last so the definitions they walk are already
prefixed and merged, and they run BEFORE anything is written: a rejected
`@join` or `@function_call` never reaches `writeSource`. They resolve
cross-source targets through the storage itself, which is passed as the
compilation provider — the same on-demand reconstruction the served schema uses.

## Rule types

### DefinitionRule

Processes individual source definitions matching a predicate. Each rule is called once per matching definition per phase.

```go
type DefinitionRule interface {
    Name() string
    Phase() Phase
    Match(def *ast.Definition) bool
    Process(ctx CompilationContext, def *ast.Definition) error
}
```

### BatchRule

Performs cross-cutting work after all DefinitionRules in the same phase complete.

```go
type BatchRule interface {
    Name() string
    Phase() Phase
    ProcessAll(ctx CompilationContext) error
}
```

Rules execute in registration order within a phase; the order is
`ingest.Default()`.

## CompileOptions

The `base.Options` struct controls how a source is ingested:

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Data source name (used in `@catalog` directives) |
| `EngineType` | `string` | Target engine (`"duckdb"`, `"postgres"`, etc.) |
| `ReadOnly` | `bool` | Suppress mutation generation |
| `Prefix` | `string` | Prefix added to the source's type names |
| `AsModule` | `bool` | Expose the source as a module of its own |
| `IsExtension` | `bool` | Extension source: may reach into other sources |
| `Capabilities` | `*EngineCapabilities` | Engine feature support flags |

## Key interfaces

### Catalog

Source of schema definitions. Each catalog source (file, URI, DB, string, HTTP)
implements this directly, using `static.NewDocumentProvider` internally.

```go
type Catalog interface {
    ingest.Catalog             // DefinitionsSource + CompileOptions()
    Name() string
    Description() string
    Version(ctx context.Context) (string, error)
    Engine() engines.Engine
}
```

Version strategies:
- **File-based** (FileSource, StringSource): SHA-256 content hash — changes only when source changes
- **Dynamic** (URISource, DB, HTTP): Timestamp — always triggers recompilation
- **Merged**: SHA-256 of all sub-catalog versions

The version is the reload gate: an unchanged source is not recompiled and not
rewritten, so a redeploy costs one metadata read.

### CompilationContext

Provided to rules during processing. Gives access to:
- **Source definitions** via `Source()` — the SDL being ingested, which PREPARE rules mutate in place
- **Type lookup** via `LookupType()` — output first, then the target provider (the storage, which reconstructs on demand)
- **Extension output** via `AddExtension()` — what cannot be merged in place
- **Promoted definitions** via `PromoteToSource()` / `PromotedDefinitions()` — how a source's functions travel through the pipeline
- **Scalar registry** via `ScalarLookup()` — scalar type metadata
- **Shared metadata** via `RegisterObject()` / `GetObject()` — primary keys, table names, module info

### Provider

Read-only interface to a schema: type lookups, roots, directive definitions.
The catalog storage implements it (generating on read); `static.Provider`
implements it over the system prelude compiled into the binary.

### CatalogManager

The lifecycle surface: `AddCatalog`, `ReloadCatalog`, `SuspendCatalog`,
`ReactivateCatalog`, `RemoveCatalog`. The storage implements it, and
`catalog.NewService` detects that on the provider it is given.

### CompiledCatalog

Output of `Compile()`: the extensions and the dependency list
(`DependentCompiledCatalog`). Its definitions half is empty by design — see the
data flow above.
