# Hugr Data Mesh GraphQL Engine

Hugr is a GraphQL-over-SQL analytics engine that federates data from multiple sources (PostgreSQL, DuckDB, HTTP APIs) into a unified modular GraphQL schema. It uses DuckDB as its query execution engine.

## Key Concepts

- **Data Sources**: External databases attached to DuckDB (Postgres, DuckDB files, HTTP, MySQL, MSSQL)
- **Modules**: Hierarchical namespace grouping of types, queries, mutations, and functions
- **Catalogs**: Schema compilation units — each data source produces a catalog
- **Runtime Sources**: Built-in sources (core, cache, GIS, catalog, meta, storage)

## Architecture

Queries flow through: GraphQL parsing → schema validation → SQL planning → DuckDB execution.

## Schema Organization

- **Hierarchical modules**: Modules may contain submodules, objects, functions.
- Queries nest modules as fields:
  ```graphql
  query {
    sales {
      analytics {
        orders { id total }
      }
    }
  }
  ```
- **Functions in modules**: Invoked via top-level `function` field:
  ```graphql
  query {
    function {
      sales {
        customer_stats(customer_id: 123) {
          total_orders
          total_spent
        }
      }
    }
  }
  ```
