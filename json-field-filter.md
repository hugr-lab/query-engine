# JSON Field Filter — nested JSON field filtering

## Description

New filter operators for JSON fields allow filtering by nested fields with an arbitrary path.
Previously, the only way to filter nested JSON values was the `contains` operator (exact match).
Comparison operators, string operators, multiple values, and logical combinators are now available.

## New types in the GraphQL schema

### JSONFieldFilter

```graphql
input JSONFieldFilter @system {
  path: String!          # path to nested field (dot notation: "catalog.field_name")
  coalesce: JSON         # default when value is NULL (optional)
  eq: JSON
  gt: JSON
  gte: JSON
  lt: JSON
  lte: JSON
  in: [JSON!]
  not_in: [JSON!]
  like: String
  ilike: String
  regex: String
  is_null: Boolean
}
```

### Extended JSONFilter

```graphql
input JSONFilter @system {
  # existing operators (unchanged)
  eq: JSON
  has: String
  has_all: [String!]
  contains: JSON
  is_null: Boolean

  # new operators
  field: [JSONFieldFilter!]   # filter on nested fields
  not: JSONFilter             # logical NOT
  or: [JSONFilter!]           # logical OR
  and: [JSONFilter!]          # explicit AND
}
```

## Usage examples

### Numeric field > 0
```graphql
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.ko", gt: 0 }]
  }
}
```

### IN — multiple values
```graphql
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.fkr_map", in: [2, 3] }]
  }
}
```

### String search (case-insensitive)
```graphql
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.address", ilike: "%школа%" }]
  }
}
```

### NULL handling with `coalesce`
```graphql
# Treat NULL as 0, then check > 0
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.ko", coalesce: 0, gt: 0 }]
  }
}
```

### Combining conditions
```graphql
# Objects where ko > 0 OR roof > 0
filter: {
  attributes: {
    or: [
      { field: [{ path: "gkh_kapremont_2026.ko", gt: 0 }] },
      { field: [{ path: "gkh_kapremont_2026.roof", gt: 0 }] }
    ]
  }
}
```

### NOT — negation
```graphql
# Objects not matching the program filter
filter: {
  attributes: {
    not: { contains: { gkh_kapremont_2026: { year: 2026 } } }
  }
}
```

## SQL mapping

| Operator | PostgreSQL | DuckDB |
|----------|-----------|--------|
| `gt: 0` (path) | `COALESCE(attr @@ '$.path > 0', false)` | `attr['path'] > $1` |
| `in: [2,3]` (path) | `(.. @@ '== 2') OR (.. @@ '== 3')` | `attr['path'] = $1 OR ... = $2` |
| `like` (path) | `attr->>'path' LIKE $1` | `attr['path'] LIKE $1` |
| `ilike` (path) | `attr->>'path' ILIKE $1` | `attr['path'] ILIKE $1` |
| `regex` (path) | `attr @@ '$.path like_regex "..."'` | `regexp_matches(attr['path'], $1)` |
| `is_null` (path) | `attr->'path' IS NULL` | `attr['path'] IS NULL` |
| `coalesce + gt` | `COALESCE(attr->'path', 0) > $1` | `COALESCE(attr['path'], 0) > $1` |
| `not` | `NOT(filter_sql)` | `NOT(filter_sql)` |
| `or` | `(f1) OR (f2)` | `(f1) OR (f2)` |

## Changed files

1. **`pkg/catalog/types/scalar_json.go`** — added `JSONFieldFilter` input type and `field`, `not`, `or`, `and` on `JSONFilter`
2. **`pkg/planner/node_select_params.go`** — handling of `field`, `not`, `or`, `and` in `filterSQLValue` + new `jsonFieldFilterSQL`
3. **`pkg/engines/json_field_filter_test.go`** — 28 tests for PostgreSQL and DuckDB

## Tests

```
go test ./pkg/engines/ -run TestJsonFieldFilterSQL -v
```

28 tests: 18 PostgreSQL, 10 DuckDB. They cover all operators: eq, gt, gte, lt, lte, is_null, like, ilike, regex, in, not_in, coalesce, and combinations.
