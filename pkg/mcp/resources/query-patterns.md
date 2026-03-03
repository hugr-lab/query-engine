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

```graphql
query {
  h3 {
    module {
      table_name(field: "geom", resolution: 7) {
        h3_index
        _rows_count
        value { avg }
      }
    }
  }
}
```
