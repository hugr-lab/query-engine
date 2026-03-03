Follow this workflow to build a GraphQL query:

1. Identify the target type with `schema-type_info`
2. List available fields with `schema-type_fields`
3. Check filter types and enum values for constraints
4. Build the query incrementally, starting with basic field selection
5. Validate with `data-validate_graphql_query` before executing
6. Execute with `data-inline_graphql_result` and iterate

Query structure: `{ module { type(filter: {...}, limit: N) { fields... } } }`
