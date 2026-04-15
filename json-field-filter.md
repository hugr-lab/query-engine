# JSON Field Filter — фильтрация по вложенным полям JSON

## Описание

Добавлены новые операторы фильтрации для JSON-полей, позволяющие фильтровать по вложенным полям
с произвольным путём. Ранее единственным способом фильтрации по вложенным JSON-значениям был
оператор `contains` (точное совпадение). Теперь доступны операторы сравнения, строковые операторы,
множественные значения и логические комбинаторы.

## Новые типы в GraphQL-схеме

### JSONFieldFilter

```graphql
input JSONFieldFilter @system {
  path: String!          # путь к вложенному полю (dot notation: "catalog.field_name")
  coalesce: JSON         # значение по умолчанию для NULL (опционально)
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

### Расширенный JSONFilter

```graphql
input JSONFilter @system {
  # существующие операторы (без изменений)
  eq: JSON
  has: String
  has_all: [String!]
  contains: JSON
  is_null: Boolean

  # новые операторы
  field: [JSONFieldFilter!]   # фильтрация по вложенным полям
  not: JSONFilter             # логическое NOT
  or: [JSONFilter!]           # логическое OR
  and: [JSONFilter!]          # явное AND
}
```

## Примеры использования

### Числовое поле > 0
```graphql
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.ko", gt: 0 }]
  }
}
```

### IN — множественные значения
```graphql
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.fkr_map", in: [2, 3] }]
  }
}
```

### Строковый поиск (case-insensitive)
```graphql
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.address", ilike: "%школа%" }]
  }
}
```

### NULL-обработка с coalesce
```graphql
# Считать NULL как 0, затем проверить > 0
filter: {
  attributes: {
    field: [{ path: "gkh_kapremont_2026.ko", coalesce: 0, gt: 0 }]
  }
}
```

### Комбинация условий
```graphql
# Объекты, где ko > 0 ИЛИ roof > 0
filter: {
  attributes: {
    or: [
      { field: [{ path: "gkh_kapremont_2026.ko", gt: 0 }] },
      { field: [{ path: "gkh_kapremont_2026.roof", gt: 0 }] }
    ]
  }
}
```

### NOT — инверсия
```graphql
# Объекты, НЕ входящие в программу
filter: {
  attributes: {
    not: { contains: { gkh_kapremont_2026: { year: 2026 } } }
  }
}
```

## SQL-маппинг

| Оператор | PostgreSQL | DuckDB |
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

## Изменённые файлы

1. **`pkg/catalog/types/scalar_json.go`** — добавлены `JSONFieldFilter` input type и поля `field`, `not`, `or`, `and` в `JSONFilter`
2. **`pkg/planner/node_select_params.go`** — обработка `field`, `not`, `or`, `and` в `filterSQLValue` + новая функция `jsonFieldFilterSQL`
3. **`pkg/engines/json_field_filter_test.go`** — 28 тестов для PostgreSQL и DuckDB

## Тесты

```
go test ./pkg/engines/ -run TestJsonFieldFilterSQL -v
```

28 тестов: 18 для PostgreSQL, 10 для DuckDB. Покрывают все операторы: eq, gt, gte, lt, lte, is_null, like, ilike, regex, in, not_in, coalesce + комбинации.
