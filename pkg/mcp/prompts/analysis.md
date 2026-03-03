Follow this workflow for data analysis:

1. Discover relevant data objects using `discovery-search_module_data_objects`
2. Understand the schema with `schema-type_info` and `schema-type_fields`
3. Use aggregation queries for summaries: append `_aggregation` to the type name
4. Use bucket aggregation for time series: append `_bucket_aggregation`
5. Combine filters with `_and` / `_or` for complex conditions
6. Navigate references to join related data across tables
