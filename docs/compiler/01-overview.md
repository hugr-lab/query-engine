# Compiler Architecture

## Overview

The query-engine compiler transforms user-provided GraphQL SDL into a fully populated schema with generated types, query/mutation fields, and metadata. It operates as a **rule-based pipeline** with 5 sequential phases.

## Data Flow

```
Catalog source (file / URI / DB / string / merged)
  → catalog.Catalog (definitions, extensions, compile options, version)
  → Compiler.Compile(ctx, provider, source, opts)
    → [VALIDATE → PREPARE → GENERATE → ASSEMBLE → FINALIZE]
  → CompiledCatalog (definitions + extensions)
  → Provider.Update(ctx, compiled)
  → Schema (ast.Schema)
```

Each source implements `catalog.Catalog` directly, providing definitions, extensions,
compile options, and a version identifier. The version enables future compilation caching —
file-based sources use content hashes, dynamic sources use timestamps.

## Phases

| Phase | Purpose | Example Rules |
|-------|---------|---------------|
| **VALIDATE** | Structural validation of source schema | `SourceValidator`, `DefinitionValidator`, `ExtensionValidator`, `DependencyCollector` |
| **PREPARE** | Metadata preparation, prefix application, catalog tagging | `InternalExtensionMerger`, `CatalogTagger`, `PrefixPreparer` |
| **GENERATE** | Type generation (filters, mutations, aggregations, subqueries) | `PassthroughRule`, `TableRule`, `ViewRule`, `AggregationRule`, `ReferencesRule`, `FunctionRule`, `JoinSpatialRule` |
| **ASSEMBLE** | Root Query/Mutation assembly, module hierarchy | `ModuleAssembler`, `RootTypeAssembler` |
| **FINALIZE** | Post-compilation validation and constraints | `ReadOnlyFinalizer`, `JoinValidator`, `FunctionCallValidator`, `ArgumentTypeValidator`, `PostValidator` |

## Rule Types

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

## Rule Registration Order

Rules are registered in `rules/init.go` and execute in registration order within each phase:

```
VALIDATE:  ExtensionValidator → DependencyCollector → SourceValidator → DefinitionValidator
PREPARE:   InternalExtensionMerger → CatalogTagger → PrefixPreparer
GENERATE:  PassthroughRule → TableRule → ViewRule → CubeHypertableRule → UniqueRule →
           AggregationRule → ReferencesRule → JoinSpatialRule → H3Rule →
           FunctionRule → ExtraFieldRule → VectorSearchRule → EmbeddingsRule
ASSEMBLE:  ModuleAssembler → RootTypeAssembler
FINALIZE:  ReadOnlyFinalizer → JoinValidator → FunctionCallValidator → ArgumentTypeValidator → PostValidator
```

## CompileOptions

The `Options` struct controls compilation behavior:

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Catalog name (used in `@catalog` directives) |
| `EngineType` | `string` | Target engine (`"duckdb"`, `"postgres"`, etc.) |
| `ReadOnly` | `bool` | Suppress mutation generation |
| `Prefix` | `string` | Prefix added to all generated type names |
| `AsModule` | `bool` | Wrap output in module hierarchy |
| `IsExtension` | `bool` | Extension compilation mode (views only, auto-dependency) |
| `Capabilities` | `*EngineCapabilities` | Engine feature support flags |

## Key Interfaces

### Catalog

Source of schema definitions for compilation. Each catalog source (file, URI, DB, string, HTTP)
implements this interface directly using `static.NewDocumentProvider` internally.

```go
type Catalog interface {
    compiler.Catalog           // DefinitionsSource + CompileOptions()
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

### CompilationContext

Provided to rules during processing. Gives access to:
- **Source definitions** via `Source()` — read-only access to input SDL
- **Type lookup** via `LookupType()` — checks compilation output first, then target schema
- **Output writing** via `AddDefinition()`, `AddExtension()`, `AddDefinitionReplaceOrCreate()`
- **Scalar registry** via `ScalarLookup()` — scalar type metadata (filter types, aggregation types)
- **Shared metadata** via `RegisterObject()` / `GetObject()` — primary keys, table names, module info
- **Field collectors** via `RegisterQueryFields()`, `RegisterMutationFields()` — for ASSEMBLE phase

### Provider

Read-only interface to an existing schema. Provides type lookups against the already-compiled schema for multi-catalog compilation.

### MutableProvider

Extends Provider with mutation support:
- `Update(ctx, compiled)` — applies a CompiledCatalog to the schema
- `DropCatalog(ctx, name, cascade)` — removes all types tagged with `@catalog(name: ...)`

### CompiledCatalog

Output of `Compile()`. Provides:
- Iteration over generated definitions and extensions
- O(1) name-based type lookups
- Dependency list (for extension compilation via `DependentCompiledCatalog`)
