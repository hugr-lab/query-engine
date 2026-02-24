# Tables & Views

## Table Compilation (`TableRule`)

A type with `@table(name: "...")` triggers the full table compilation pipeline:

### Generated Types

For a source type `Widget`:

| Generated Type | Kind | Purpose |
|----------------|------|---------|
| `Widget_filter` | INPUT_OBJECT | Filter input for WHERE clauses |
| `Widget_mut_input_data` | INPUT_OBJECT | Insert mutation input (scalar fields only) |
| `Widget_mut_data` | INPUT_OBJECT | Update mutation input (all fields) |
| `_Widget_aggregation` | OBJECT | Aggregation type with `_rows_count` + per-field aggregations |
| `_Widget_aggregation_bucket` | OBJECT | Bucket aggregation with `key` + `aggregations` |

All generated types carry `@catalog(name, engine)` for clean DropCatalog removal.

### Generated Query Fields

Registered on the Query root type:

| Field | Type | Arguments | Description |
|-------|------|-----------|-------------|
| `Widget` | `[Widget]` | `filter`, `order_by`, `limit`, `offset`, `distinct_on` | List query (SELECT) |
| `Widget_by_pk` | `Widget` | PK fields | Single-record lookup (SELECT_ONE) |

### Generated Mutation Fields

Registered on the Mutation root type (gated by `ReadOnly` and `Capabilities`):

| Field | Return Type | Arguments | Gated By |
|-------|-------------|-----------|----------|
| `insert_Widget` | `Widget` or `OperationResult` | `data: Widget_mut_input_data!` | `SupportInsert()` |
| `update_Widget` | `OperationResult` | `filter`, `data: Widget_mut_data!` | `SupportUpdate()` + PKs |
| `delete_Widget` | `OperationResult` | `filter` | `SupportDelete()` + PKs |

Insert returns the object type if `SupportInsertReturning()` is true and the type has PKs.

## Filter Type Structure

For a type with fields `id: Int!`, `name: String!`, `created_at: Timestamp`:

```graphql
input Widget_filter @filter_input(name: "Widget") @catalog(...) {
  id: IntFilter
  name: StringFilter
  created_at: TimestampFilter
  _and: [Widget_filter]
  _or: [Widget_filter]
  _not: Widget_filter
}
```

- Only scalar fields with `Filterable` interface get filter fields
- Fields with `@function_call` or `@table_function_call_join` are excluded
- `@pk`, `@default`, `@measurement`, `@timescale_key` directives are copied to filter fields
- Logical operators `_and`, `_or`, `_not` enable compound filtering

## List Filter Type

Created lazily when back-references need list filtering:

```graphql
input Widget_list_filter @filter_list_input(name: "Widget") @catalog(...) {
  any_of: Widget_filter
  all_of: Widget_filter
  none_of: Widget_filter
}
```

## Aggregation Type Structure

```graphql
type _Widget_aggregation @aggregation(name: "Widget", is_bucket: false, level: 1) @catalog(...) {
  _rows_count: BigInt
  id: IntAggregation @field_aggregation(name: "id")
  name: StringAggregation @field_aggregation(name: "name")
  created_at: TimestampAggregation @field_aggregation(name: "created_at")
}
```

- `_rows_count` is always present
- Per-field aggregation uses scalar type's `AggregationTypeName()` (e.g., `IntAggregation`, `StringAggregation`)
- Field arguments (bucket, transforms, struct) are copied from source fields

### Bucket Aggregation

```graphql
type _Widget_aggregation_bucket @aggregation(name: "Widget", is_bucket: true, level: 1) @catalog(...) {
  key: Widget
  aggregations(filter: Widget_filter, order_by: [OrderByField]): _Widget_aggregation
}
```

## View Compilation (`ViewRule`)

Views follow the same generation pattern as tables with key differences:

- **No mutation fields** — views are read-only
- **@args support** — parameterized views via `@args(name: "InputTypeName")`
- View input types get `@catalog` directive for clean DropCatalog support
- `required` is auto-computed from NonNull fields in the args input type

### Parameterized View Example

```graphql
type FlightLog @view(name: "flight_log_view") @args(name: "FlightLogArgs") {
  flight_id: Int! @pk
  departure: Timestamp
}

input FlightLogArgs {
  from_date: Timestamp!
  to_date: Timestamp!
}
```

Generates queries with an `args` parameter:

```graphql
FlightLog(args: FlightLogArgs!, filter: FlightLog_filter, ...): [FlightLog]
```

## Scalar Field Arguments

Before aggregation generation, `setScalarFieldArguments()` adds field arguments from scalar types:
- `bucket` / `bucket_interval` for Timestamp fields
- `transforms` for Geometry fields
- `struct` for JSON fields

These arguments are copied to aggregation type fields.
