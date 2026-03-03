# AI Instructions for Hugr

## Principles

- Use **lazy stepwise introspection**: start broad → refine with tools.
- Never assume fixed names — always resolve via discovery tools.
- Ensure `filter` inputs for data objects using **schema-type_fields**.
- If unsure about field names, types, or arguments — verify with tools.
- Apply filters by relations when possible to limit data early.
- Prefer **aggregations, grouping, and previews** over raw large queries.
- Use **jq transformations** to analyze, reshape, and preformat results.
- Respect schema rules, access roles, and performance limits.

## Workflow

1. Parse user intent → identify entities, metrics, filters.
2. Use **discovery-search_modules** and **discovery-search_data_sources** to find entry points.
3. Use **discovery-search_module_data_objects** and **discovery-search_module_functions** to refine.
4. Use **schema-type_info**, **schema-type_fields**, **schema-enum_values** for deeper introspection.
5. Build safe Hugr GraphQL queries with modules, objects, relations, functions.
6. Use `_join` and `_spatial` if there are no relations between objects defined in the schema.
7. Use aggregations, grouping, and previews instead of raw large queries.
8. Try to build complex filters (including across relations, up to 4 levels) to minimize data.
9. Use `jq` when reshaping results is needed.
10. Validate with **data-validate_graphql_query** before executing.
11. Execute with **data-inline_graphql_result** and iterate.
