# Subqueries

## @references

The `@references` directive declares foreign key relationships between types. The compiler generates subquery fields, filter nesting, aggregation fields, and mutation input fields.

### Directive Arguments

```graphql
@references(
  name: "ref_name"              # Unique reference identifier
  references_name: "TargetType" # Referenced type name
  source_fields: ["field_a"]    # Fields on source type
  references_fields: ["field_b"] # Fields on target type
  query: "target_field_name"    # Forward reference field name
  references_query: "back_ref"  # Back-reference field name on target
  is_m2m: false                 # Many-to-many flag
  m2m_name: ""                  # M2M junction table name
)
```

### Generated Fields — Forward Reference

On the source type:
- `target_field_name: TargetType` — single-object subquery (for 1:1 / N:1)
- Arguments: `inner: Boolean` (inner vs left join)

On the source filter type:
- `target_field_name: TargetType_filter` — nested filter

On the source aggregation type:
- `target_field_name: _TargetType_aggregation` — aggregation subquery

### Generated Fields — Back-Reference

On the target type:
- `back_ref: [SourceType]` — list subquery (for 1:N)
- Arguments: `filter`, `order_by`, `limit`, `offset`, `distinct_on`, `inner`, `nested_order_by`, `nested_limit`, `nested_offset`

On the target filter type:
- `back_ref: SourceType_list_filter` — list filter (any_of / all_of / none_of)

On the target aggregation type:
- `back_ref: _SourceType_aggregation` — reference aggregation
- `back_ref_aggregation: _SourceType_aggregation_sub_aggregation` — sub-aggregation

On the target mutation input:
- `back_ref: [SourceType_mut_input_data]` — nested insert support

## @field_references

Shorthand for `@references` on a single field:

```graphql
type Order @table(name: "orders") {
  customer_id: Int! @field_references(
    references_name: "Customer"
    field: "id"
    query: "customer"
    references_query: "orders"
  )
}
```

Converted to an object-level `@references` directive during ReferencesRule processing. Also copies the `@field_references` directive to the corresponding filter field.

## Many-to-Many (M2M)

M2M relationships use a junction table with `@table(is_m2m: true)` and two `@references`:

```graphql
type StudentCourse @table(name: "student_courses", is_m2m: true)
  @references(references_name: "Student", source_fields: ["student_id"], references_fields: ["id"], query: "student", references_query: "courses")
  @references(references_name: "Course", source_fields: ["course_id"], references_fields: ["id"], query: "course", references_query: "students") {
  student_id: Int! @pk
  course_id: Int! @pk
}
```

Each side gets:
- List reference field to the other side through the junction table
- Filter nesting with `_list_filter`
- Aggregation + bucket_aggregation fields
- Mutation insert support (unless cross-catalog)

## Cross-Catalog References

References between types in different catalogs require:

```go
Capabilities: &EngineCapabilities{
    General: EngineGeneralCapabilities{
        SupportCrossCatalogReferences: true,
    },
}
```

Cross-catalog restrictions:
- Mutation insert fields are **not** generated for cross-catalog references
- Both forward and back-reference query fields work normally
- Filter nesting and aggregation work normally

## @join

The `@join` directive creates join subquery fields with optional similarity/semantic matching:

```graphql
type Airport @table(name: "airports") {
  nearby: [Airport] @join(
    table: "airports"
    fields: ["iata_code"]
    references_fields: ["iata_code"]
  )
}
```

Generates `_join` and `_join_aggregation` types. Validated by `JoinValidator` in PhaseFinalize.

## Sub-Aggregation

Sub-aggregation types are created when list references need nested aggregation:

| Depth | Type Name Pattern | Scalar Types |
|-------|-------------------|--------------|
| 0 (base) | `_Type_aggregation` | `IntAggregation`, etc. |
| 1 (sub) | `_Type_aggregation_sub_aggregation` | `IntSubAggregation`, etc. |
| 2 (max) | `_Type_aggregation_sub_aggregation_sub_aggregation` | Only `_rows_count` |

Maximum depth is 2 (`maxAggDepth`). At max depth, only `_rows_count` is included.

Sub-aggregation types:
- Include `_rows_count` with `BigIntAggregation` (or `BigIntSubAggregation` at max depth)
- Include scalar fields mapped to `SubAggregation` variants
- Include extra fields (measurement, part) from the base aggregation's extensions
- Carry `@catalog` directive for clean DropCatalog removal
