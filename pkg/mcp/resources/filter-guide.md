# Filter Guide

## Scalar Operators

Operators depend on the field type (check with `schema-type_fields`):

| Operator | Types | Description |
|----------|-------|-------------|
| `eq` | all | Equal to |
| `neq` | all | Not equal to |
| `gt`, `gte` | numeric, temporal | Greater than (or equal) |
| `lt`, `lte` | numeric, temporal | Less than (or equal) |
| `in` | all | In list |
| `nin` | all | Not in list |
| `is_null` | all | Is null check |
| `like` | String | SQL LIKE pattern |
| `ilike` | String | Case-insensitive LIKE |
| `regex` | String | Regular expression |

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
