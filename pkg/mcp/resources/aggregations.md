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

## Sub-Aggregation on Relations

Aggregate over one-to-many / many-to-many relations:

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
