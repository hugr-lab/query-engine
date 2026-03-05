# Aggregations

## Single-Row Aggregation

```graphql
query {
  module {
    table_aggregation(filter: {...}) {
      _rows_count
      price { sum avg min max }
    }
  }
}
```

## Bucket Aggregation (GROUP BY)

```graphql
query {
  module {
    table_bucket_aggregation {
      key { status }
      aggregations {
        _rows_count
        total: amount { sum avg }
      }
    }
  }
}
```

Group by nested fields:

```graphql
table_bucket_aggregation {
  key { customer { category { name } } }
  aggregations { _rows_count }
}
```

## Filtered Aggregation

Apply filter inside aggregation (acts as SQL `FILTER (WHERE ...)`). Use aliases for multiple filtered aggregations:

```graphql
query {
  module {
    orders_bucket_aggregation {
      key { status }
      all: aggregations { _rows_count total: amount { sum avg } }
      in_stock: aggregations(filter: {stock_quantity: {gt: 0}}) { _rows_count }
      premium: aggregations(filter: {category: {eq: "premium"}}) {
        _rows_count
        total: amount { sum avg }
      }
    }
  }
}
```

## Sorting Bucket Aggregations

Use dot-notation paths to sort by keys or aggregation values:

```graphql
orders_bucket_aggregation(
  order_by: [
    {field: "aggregations.total.sum", direction: DESC}
    {field: "key.status", direction: ASC}
  ]
  limit: 10
) {
  key { status }
  aggregations { total: amount { sum avg } }
}
```

Sorting by filtered aggregation alias:

```graphql
order_by: [{field: "filtered.total.sum", direction: DESC}]
```

## Aggregation Over Relations (Subquery Aggregation)

Aggregate related data for each parent record. Works with references, joins, and function calls:

```graphql
query {
  module {
    customers {
      id name
      orders_aggregation {
        _rows_count
        total: amount { sum avg }
      }
      completed: orders_aggregation(filter: {status: {eq: "completed"}}) {
        _rows_count
      }
      orders_bucket_aggregation {
        key { status }
        aggregations { _rows_count total: amount { sum avg } }
      }
    }
  }
}
```

## Sub-Aggregation (Aggregation of Aggregations)

Sub-aggregation applies aggregation functions to already-aggregated results:

```graphql
query {
  module {
    categories_aggregation {
      _rows_count
      products_aggregation {
        _rows_count { sum }
        price { avg { avg min max } }
      }
    }
  }
}
```

Here `products_aggregation` is a sub-aggregation: `_rows_count { sum }` computes the sum of per-category product counts, and `price { avg { avg } }` computes the average of per-category average prices.

## Time-Based Aggregations

Bucket by time periods:

```graphql
orders_bucket_aggregation {
  key { created_at(bucket: month) }
  aggregations { _rows_count total: amount { sum } }
}
```

Buckets: `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`.
Custom intervals: `created_at(bucket_interval: "15 minutes")`.
Extract parts: `_created_at_part(extract: year)`.

## Aggregations with _join and _spatial

Aggregation variants are available inside `_join` and `_spatial`:

```graphql
_join(fields: ["category_id"]) {
  prefix_products_aggregation(fields: ["category_id"]) {
    _rows_count
    price { sum avg }
  }
  prefix_products_bucket_aggregation(fields: ["category_id"]) {
    key { name }
    aggregations { _rows_count }
  }
}
```

## Available Functions

- `_rows_count` — row count
- `sum`, `avg`, `min`, `max` — numeric
- `count`, `count(distinct)` — count
- `stddev`, `variance` — statistical
- `string_agg` — string concatenation
- `list` — collect into array
- `distinct` — distinct values
- `bool_and`, `bool_or` — boolean
- `any`, `last` — arbitrary value
- JSON fields support `path` parameter for nested aggregation
