# Query Patterns

## Basic Select

```graphql
query {
  module_name {
    table_name(filter: {field: {eq: "value"}}, limit: 10) {
      field1
      field2
    }
  }
}
```

## Reference Traversal

```graphql
query {
  module {
    orders {
      id total
      customer { name category }
    }
  }
}
```

## Function Calls

Functions are called via the top-level `function` field:

```graphql
query {
  function {
    module {
      my_function(arg1: "value") {
        result_field
      }
    }
  }
}
```

## Mutation Functions

```graphql
mutation {
  mutation_function {
    module {
      my_mutation(input: "data") {
        status
        affected_rows
      }
    }
  }
}
```

## Select One (by PK)

```graphql
query {
  module {
    table_by_pk(id: 1) {
      id name
    }
  }
}
```

## JQ Transform

Apply jq transformation to query results at the root level:

```graphql
query {
  jq(query: "{ module { table { id name } } }", jq: ".module.table | map(.name)")
}
```

## H3 Spatial Aggregation

H3 is a root-level query that aggregates data into hexagonal cells. The `resolution` argument is required at the root level. Inside the `data` field, use `<table>_aggregation(field: "<geometry_field>")` to aggregate by geometry.

```graphql
query {
  h3(resolution: 4) {
    cell
    resolution
    geom
    data {
      pg_store_locations_aggregation(field: "point") {
        _rows_count
      }
    }
  }
}
```

Note: Inside `data`, table names use the **catalog prefix** (not the module name). For example, if the catalog prefix is `pg_store`, the field is `pg_store_locations_aggregation`. Both `_aggregation` and `_bucket_aggregation` variants are available.

## Cube Tables (@cube)

Tables with `@cube` are OLAP cube types. Fields marked `@measurement` receive a `measurement_func` argument that selects the aggregation function (SUM, AVG, MIN, MAX, ANY) to apply before returning data. This enables pre-aggregated queries on fact tables.

```graphql
query {
  SalesCube(filter: {region: {eq: "US"}}) {
    region
    product
    revenue(measurement_func: SUM)
    quantity(measurement_func: AVG)
  }
}
```

Cube tables also generate standard aggregation and bucket aggregation queries.

## Query-Time Join (`_join`)

Join data from different tables at query time by matching field values. The `_join` field is available on every table/view. Inside `_join`, table names use the **catalog prefix** (not the module name).

```graphql
query {
  pg_store {
    products(filter: { id: { eq: 1 } }) {
      id
      name
      category_id
      _join(fields: ["category_id"]) {
        pg_store_categories(fields: ["id"]) {
          id
          name
        }
      }
    }
  }
}
```

Arguments on `_join` subfields:
- `fields: [String!]!` — target table fields to match against source `_join(fields: ...)`
- `filter`, `order_by`, `limit`, `offset`, `distinct_on` — applied **before** the join
- `nested_order_by`, `nested_limit`, `nested_offset` — applied **after** the join (controls result ordering/pagination of joined data)
- `inner: true` — use INNER JOIN instead of LEFT JOIN (excludes source rows without matches)

Aggregation variants are also available inside `_join`:

```graphql
_join(fields: ["category_id"]) {
  pg_store_products_aggregation(fields: ["category_id"]) {
    _rows_count
    price { sum avg }
  }
  pg_store_products_bucket_aggregation(fields: ["category_id"]) {
    key { name }
    aggregations { _rows_count }
  }
}
```

## Spatial Join (`_spatial`)

Join data using geometry intersection. The `_spatial` field is available on tables/views with geometry columns.

```graphql
query {
  pg_store {
    locations(filter: { id: { eq: 1 } }) {
      id
      name
      _spatial(field: "point", type: INTERSECTS) {
        pg_store_locations(field: "area") {
          id
          name
        }
      }
    }
  }
}
```

Arguments on `_spatial`:
- `field: String!` — source geometry field name
- `type: GeometrySpatialQueryType!` — spatial relation: `INTERSECTS`, `WITHIN`, `CONTAINS`, `DISJOIN`, `DWITHIN`
- `buffer: Int` — buffer distance in meters (required for `DWITHIN`)

Subfield arguments (same as `_join`):
- `field: String!` — target geometry field name
- `filter`, `order_by`, `limit`, `offset`, `distinct_on` — pre-join filtering
- `nested_order_by`, `nested_limit`, `nested_offset` — post-join ordering
- `inner: true` — INNER JOIN mode

Table names inside `_spatial` also use the **catalog prefix**.