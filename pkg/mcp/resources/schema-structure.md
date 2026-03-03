# Schema Structure

## Data Objects

Standard args: `filter`, `order_by`, `limit`, `offset`, `distinct_on`, `args`.
Nested args (post-join): `nested_order_by`, `nested_limit`, `nested_offset`, `inner`.

### Relations

- one-to-one → single field
- one-to-many / many-to-many → `<relation>`, `<relation>_aggregation`, `<relation>_bucket_aggregation`

### Fields

- May include scalars, nested objects, relations (subqueries), function call results.
- Each relation field (or aggregation/bucket_aggregation) must specify its own fields.
- Each relation field for one-to-many or many-to-many can accept standard args and nested args.

## Special Subqueries

### _join
- Arg `fields`: array of source field names
- Each subfield also requires `fields`
- Supports records, aggregation, bucket_aggregation
- Standard args apply **before** join, nested args apply **after** join

```graphql
query {
  marketing {
    customers_infos {
      customer_id
      revenue
      _join(fields: ["customer_id"]) {
        customer(fields: ["marketing_id"]) {
          id name category
          orders_aggregation {
            _rows_count
            total: amount { sum avg }
          }
        }
      }
    }
  }
}
```

**Catalog prefix note:** When data sources use a `prefix`, type names and query field names inside `_join` and `_spatial` include that prefix. For example, if the catalog prefix is `pg_store`, the field would be `pg_store_orders_aggregation` instead of `orders_aggregation`.

### _spatial
- Args: `field`, `type` (`INTERSECTS`, `WITHIN`, `CONTAINS`, `DISJOIN`, `DWITHIN`), `buffer` (for `DWITHIN`)
- Subfields must specify `field` of joined object
- Supports records, aggregation, bucket_aggregation

```graphql
query {
  gis {
    areas { id name geom
      _spatial(field: "geom", type: INTERSECTS) {
        roads(field: "geom" nested_limit: 5) {
          id name length
        }
        roads_aggregation(field: "geom") {
          _rows_count
          total: length { sum avg }
        }
      }
    }
  }
}
```

## Order By

- `order_by` accepts array: `[{field: "name", direction: ASC}]`
- **Fields used in `order_by` must be included in the selection set**, otherwise the query returns an error.
- For bucket aggregations, field paths use dot notation: `"key.category"`, `"aggregations.total.sum"`.
- `nested_order_by`, `nested_limit`, `nested_offset` apply **after** a join/spatial operation (post-join ordering and pagination). Use these on subquery fields inside `_join` or `_spatial` to control the result set of the joined data independently from the pre-join `order_by`/`limit`/`offset`.

```graphql
query {
  orders_bucket_aggregation(
    order_by: [
      {field: "key.category", direction: ASC},
      {field: "aggregations.total.sum", direction: DESC}
    ]
    limit: 10
  ) {
    key { category }
    aggregations { total: amount { sum avg } }
  }
}
```

## Distinct On

- Accepts array of field names to return distinct rows
- Fields must be selected in the query

## Subquery Fields (Relations, Joins, Function Calls)

Relations defined via `@references` or `@field_references` produce subquery fields on the parent type. For one-to-many relations, three fields are generated: `<relation>`, `<relation>_aggregation`, `<relation>_bucket_aggregation`. Each accepts standard args (`filter`, `order_by`, `limit`, `offset`) and nested args (`nested_order_by`, `nested_limit`, `nested_offset`, `inner`).

### The `inner` Argument

`inner: true` converts a LEFT JOIN to an INNER JOIN, excluding parent rows that have no matching related records. This works on:
- Reference subquery fields (one-to-one and one-to-many relations)
- `@join` and `@table_function_call_join` fields
- Nested subqueries within `_join` and `_spatial`

Filtering by related objects only works for `@references`/`@field_references` relations. For `@join` or `@table_function_call_join` fields, use `inner: true` instead to restrict parent rows.

### Function Call Fields

Fields defined with `@function_call` or `@table_function_call_join` appear as regular fields on the parent type. If the function accepts parameters not mapped from parent fields, those become query arguments on the field. Table function calls returning arrays also generate `_aggregation` and `_bucket_aggregation` variants.
