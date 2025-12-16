# The Hugr query-engine

The Hugr query engine is a part of the Hugr project, which provides a powerful and flexible GraphQL query engine designed to work with various data sources. It supports features like caching, authentication, and schema extensions, making it suitable for building scalable and efficient applications.

See the [Hugr documentation](https://hugr-lab.github.io) for more details.

## Dependencies

It used the following packages:

- [github.com/duckdb/duckdb-go/v2](https://github.com/marcboeker/go-duckdb)
- [github.com/apache/arrow-go/v18](https://github.com/apache/arrow-go)
- [github.com/paulmach/orb](https://github.com/paulmach/orb)
- [github.com/vektah/gqlparser/v2](https://github.com/vektah/gqlparser)
- [github.com/eko/gocache/v4](https://github.com/eko/gocache)
- [github.com/vmihailenco/msgpack/v5](https://github.com/vmihailenco/msgpack)
- [github.com/itchyny/gojq](https://github.com/itchyny/gojq)
- [github.com/golang-jwt/jwt/v5](https://github.com/golang-jwt/jwt)
- [github.com/getkin/kin-openapi](https://github.com/getkin/kin-openapi)

## Features

- Http handlers for GraphQL API
- GraphiQL UI to execute queries
- GraphQL schema definition and compilation
- Data sources management
- Schema extensions to add sub queries to existing data sources
- Authentication and authorization
- Caching: L1 (inmemory - bigcache) and L2 (redis, memcached or pegasus)
- Query parsing and validation
- Query execution
- Transformation results (jq transformation)
