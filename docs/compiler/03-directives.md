# Directive Reference

Directives fall into three categories: **source** (user-provided), **control** (stripped before output), and **generated** (added by the compiler).

## Source Directives

These are written by users in their SDL and preserved in the compiled output.

### Data Object Directives

| Directive | Location | Description |
|-----------|----------|-------------|
| `@table(name)` | OBJECT | Declares a table-backed type |
| `@view(name, sql)` | OBJECT | Declares a view-backed type |
| `@pk` | FIELD_DEFINITION | Marks field as primary key |
| `@unique(fields, query_suffix)` | OBJECT | Declares unique constraint, generates by-unique query |
| `@default(value, sequence)` | FIELD_DEFINITION | Default field value or sequence |

### Relationship Directives

| Directive | Location | Description |
|-----------|----------|-------------|
| `@references(...)` | OBJECT | Declares foreign key reference between types |
| `@field_references(...)` | FIELD_DEFINITION | Per-field shorthand for `@references` |
| `@join(...)` | FIELD_DEFINITION | Declares a join subquery field |

### Module & Organization

| Directive | Location | Description |
|-----------|----------|-------------|
| `@module(name)` | OBJECT \| FIELD_DEFINITION | Assigns type to a module namespace |
| `@args(name, required)` | OBJECT | Parameterized view arguments |

### Function Directives

| Directive | Location | Description |
|-----------|----------|-------------|
| `@function(name, type)` | OBJECT | Declares a function type |
| `@function_call(...)` | FIELD_DEFINITION | Declares a function call subquery field |
| `@table_function_call_join(...)` | FIELD_DEFINITION | Declares a table function call join field |

### Analytics Directives

| Directive | Location | Description |
|-----------|----------|-------------|
| `@cube(...)` | OBJECT | Declares a cube (OLAP) type |
| `@hypertable(...)` | OBJECT | Declares a hypertable (TimescaleDB) type |
| `@measurement(...)` | FIELD_DEFINITION | Measurement field for hypertable |
| `@timescale_key` | FIELD_DEFINITION | TimescaleDB partition key |

### Extension Directives

| Directive | Location | Description |
|-----------|----------|-------------|
| `@dependency(name)` | OBJECT \| FIELD_DEFINITION | Marks dependency on a catalog |
| `@embeddings(...)` | OBJECT | Vector embeddings configuration |

## Control Directives

These control compilation behavior and are consumed/stripped by the compiler.

| Directive | Location | Description |
|-----------|----------|-------------|
| `@drop` | OBJECT \| FIELD_DEFINITION | Marks type/field for removal |
| `@replace` | OBJECT \| FIELD_DEFINITION | Marks type/field for replacement |
| `@if_not_exists` | OBJECT | Add type only if not already defined |
| `@drop_directive(name)` | OBJECT | Removes a directive from existing type |
| `@system` | various | Marks type as system-internal |

## Generated Directives

These are added by the compiler during PhaseGenerate and PhaseAssemble.

### Type Metadata

| Directive | Location | Description |
|-----------|----------|-------------|
| `@catalog(name, engine)` | OBJECT \| INPUT_OBJECT \| FIELD_DEFINITION \| INPUT_FIELD_DEFINITION | Tags type/field with source catalog |
| `@original_name(name)` | various | Preserves pre-prefix type name |
| `@module_root(name, type)` | OBJECT | Marks module root query/mutation type |

### Generated Type Markers

| Directive | Location | Description |
|-----------|----------|-------------|
| `@filter_input(name)` | OBJECT \| INPUT_OBJECT | Links to source type's filter input |
| `@filter_list_input(name)` | OBJECT \| INPUT_OBJECT | Links to source type's list filter input |
| `@data_input(name)` | OBJECT \| INPUT_OBJECT | Links to source type's mutation input |

### Query/Mutation Markers

| Directive | Location | Description |
|-----------|----------|-------------|
| `@query(name, type)` | OBJECT \| FIELD_DEFINITION | Marks a query field (SELECT or SELECT_ONE) |
| `@mutation(name, type, data_input)` | OBJECT \| FIELD_DEFINITION | Marks a mutation field (INSERT, UPDATE, DELETE) |

### Aggregation Markers

| Directive | Location | Description |
|-----------|----------|-------------|
| `@aggregation(name, is_bucket, level)` | OBJECT | Marks aggregation type with depth level |
| `@field_aggregation(name)` | OBJECT \| FIELD_DEFINITION | Links aggregation field to source field |
| `@aggregation_query(name, is_bucket)` | FIELD_DEFINITION | Marks query-level aggregation field |

### Reference Markers

| Directive | Location | Description |
|-----------|----------|-------------|
| `@references_query(references_name, name, is_m2m, m2m_name)` | FIELD_DEFINITION | Marks reference subquery field |

### Extra Field Markers

| Directive | Location | Description |
|-----------|----------|-------------|
| `@extra_field(name, base_field, base_type)` | FIELD_DEFINITION | Auto-generated field from scalar (e.g., `_date_part` for Timestamp) |
