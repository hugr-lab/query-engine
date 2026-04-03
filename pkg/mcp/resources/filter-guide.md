# Filter Guide

## Scalar Operators

Operators depend on the field type (check with `schema-type_fields`):

| Operator | Types | Description |
|----------|-------|-------------|
| `eq` | all scalars | Equal to |
| `gt`, `gte` | numeric, temporal, Interval | Greater than (or equal) |
| `lt`, `lte` | numeric, temporal, Interval | Less than (or equal) |
| `in` | String, numeric, Date, Time, Timestamp, DateTime | In list |
| `is_null` | all | Is null check |
| `like` | String | SQL LIKE pattern (`%` wildcard) |
| `ilike` | String | Case-insensitive LIKE |
| `regex` | String | Regular expression |
| `has` | JSON | Has key |
| `has_all` | JSON | Has all keys |
| `contains` | JSON, Geometry, List, Range | Contains value/geometry/list/range |
| `intersects` | Geometry, List, Range | Intersects with |
| `includes` | Range | Range includes another range |
| `eq` (List) | List types | List contains value |

Boolean type supports only: `eq`, `is_null`.

## Logical Operators

```graphql
filter: {
  _and: [
    {status: {eq: "active"}}
    {_or: [
      {age: {gte: 18}}
      {role: {eq: "admin"}}
    ]}
  ]
}
```

## Relation Filters

Filter by related objects (up to 4 levels deep):

```graphql
filter: {
  category: {description: {ilike: "%premium%"}}
  customers: {any_of: {country: {eq: "US"}}}
}
```

For lists and one-to-many/many-to-many relations use: `any_of`, `all_of`, `none_of`:

```graphql
orders(filter: {
  items: {any_of: {product: {category: {eq: "electronics"}}}}
}) {
  id total
  items { product { name category } }
}
```

## Tips

- Check the filter input type fields using `schema-type_fields` to see available operators.
- Filter by relations to limit data early.
- Combine filters to minimize query size and processing time.
