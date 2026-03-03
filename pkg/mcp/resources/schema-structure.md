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
- Field must be included in the selection set
- Use `nested_order_by` for post-join ordering

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
