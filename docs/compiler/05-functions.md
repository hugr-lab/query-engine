# Functions

## Function Compilation (`FunctionRule`)

Types with `@function(name, type)` are compiled as function types. Functions generate query or mutation fields depending on their type.

### Function Types

| `@function(type:)` | Generated On | Description |
|--------------------|--------------|-------------|
| `QUERY` / `SELECT` | Query root | Read-only function returning data |
| `MUTATION` | Mutation root | Function with side effects |

### Generation Process

1. **Type output**: Function type definition added with `@if_not_exists` (allows cross-catalog merging)
2. **Query/Mutation field**: Function field registered with appropriate root type
3. **@catalog tagging**: Function fields carry `@catalog(name, engine)` on FIELD_DEFINITION
4. **Module integration**: When `AsModule=true`, `@module` directive added to function fields

### Function Fields as Extensions

Functions can be defined as fields on existing types using `@function_call` or `@table_function_call_join`:

```graphql
type Airport @table(name: "airports") {
  nearby_airports(radius: Float!): [Airport]
    @function_call(name: "find_nearby", module: "geo")
}
```

These fields:
- Get `@catalog` directive during table/view compilation
- Get `@module` directive added when `AsModule=true`
- Are excluded from filter and mutation input generation
- Generate sub-aggregation types for list-returning fields

### Function Aggregation Fields

For function fields returning lists, the compiler generates sub-aggregation entries:

```graphql
type _Airport_aggregation {
  nearby_airports_aggregation: _Airport_aggregation_sub_aggregation
    @field_aggregation(name: "nearby_airports")
}
```

### Validation

`FunctionCallValidator` (PhaseFinalize) validates:
- Referenced function types exist
- Argument types match function parameters
- Module references are consistent
