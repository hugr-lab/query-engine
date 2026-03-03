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

## Filtered Aggregation

Apply filter inside aggregation (acts as SQL `FILTER (WHERE ...)`):

```graphql
query {
  module {
    orders_bucket_aggregation {
      key { status }
      aggregations { _rows_count total: amount { sum avg } }
      filtered: aggregations(filter: {category: {eq: "premium"}}) {
        _rows_count
        total: amount { sum avg }
      }
    }
  }
}
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
      orders_bucket_aggregation {
        key { status }
        aggregations { _rows_count total: amount { sum avg } }
      }
    }
  }
}
```

## Sub-Aggregation (Aggregation of Aggregations)

Sub-aggregation applies aggregation functions to already-aggregated results. For aggregated subquery fields, each aggregation field exposes sub-aggregation functions:

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

## Ordering Bucket Aggregations

```graphql
query {
  module {
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
  }
}
```

## Available Functions

- `_rows_count` — row count
- `sum`, `avg`, `min`, `max` — numeric
- `string_agg` — string concatenation
- `list` — collect into array
- `distinct` — distinct values
