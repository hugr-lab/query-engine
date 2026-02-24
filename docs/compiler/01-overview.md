# Compiler Architecture

## Overview

The query-engine compiler transforms user-provided GraphQL SDL into a fully populated schema with generated types, query/mutation fields, and metadata. It operates as a **rule-based pipeline** with 5 sequential phases.

## Data Flow

```
Source SDL → parser.ParseSchema()
  → DefinitionsSource (source definitions)
  → Compiler.Compile(ctx, provider, source, opts)
    → [VALIDATE → PREPARE → GENERATE → ASSEMBLE → FINALIZE]
  → CompiledCatalog (definitions + extensions)
  → Provider.Update(ctx, compiled)
  → Schema (ast.Schema)
```

## Phases

| Phase | Purpose | Example Rules |
|-------|---------|---------------|
| **VALIDATE** | Structural validation of source schema | `SourceValidator`, `DefinitionValidator`, `ExtensionValidator`, `DependencyCollector` |
| **PREPARE** | Metadata preparation, prefix application, catalog tagging | `CatalogTagger`, `PrefixPreparer` |
| **GENERATE** | Type generation (filters, mutations, aggregations, subqueries) | `TableRule`, `ViewRule`, `ReferencesRule`, `FunctionRule`, `JoinSpatialRule` |
| **ASSEMBLE** | Root Query/Mutation assembly, module hierarchy | `ModuleAssembler`, `RootTypeAssembler` |
| **FINALIZE** | Post-compilation validation and constraints | `ReadOnlyFinalizer`, `JoinValidator`, `PostValidator` |

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
PREPARE:   CatalogTagger → PrefixPreparer
GENERATE:  TableRule → ViewRule → CubeHypertableRule → UniqueRule → ReferencesRule →
           JoinSpatialRule → H3Rule → AggregationRule → FunctionRule → ExtraFieldRule →
           VectorSearchRule → EmbeddingsRule
ASSEMBLE:  ModuleAssembler → RootTypeAssembler
FINALIZE:  ReadOnlyFinalizer → JoinValidator → FunctionCallValidator → PostValidator
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
