package db

// commonIndexes contains index DDL shared between DuckDB and PostgreSQL.
// For DuckDB: table names use "core." prefix.
// For PostgreSQL: table names have no prefix (public schema).
var commonIndexes = []struct {
	name  string
	table string
	cols  string
}{
	{"idx_schema_types_catalog", "_schema_types", "catalog"},
	{"idx_schema_types_hugr_type", "_schema_types", "hugr_type"},
	{"idx_schema_types_kind", "_schema_types", "kind"},
	{"idx_schema_fields_type_name", "_schema_fields", "type_name"},
	{"idx_schema_fields_catalog", "_schema_fields", "catalog"},
	{"idx_schema_fields_hugr_type", "_schema_fields", "hugr_type"},
	{"idx_schema_fields_dependency_catalog", "_schema_fields", "dependency_catalog"},
	{"idx_schema_args_type_name", "_schema_arguments", "type_name"},
	{"idx_schema_args_type_field", "_schema_arguments", "type_name, field_name"},
	{"idx_schema_enumvals_type_name", "_schema_enum_values", "type_name"},
	{"idx_schema_mtc_catalog_name", "_schema_module_type_catalogs", "catalog_name"},
	{"idx_schema_doq_object_name", "_schema_data_object_queries", "object_name"},
	{"idx_schema_catdeps_depends_on", "_schema_catalog_dependencies", "depends_on"},
}

// pgVectorIndexes contains PostgreSQL-specific HNSW vector indexes.
var pgVectorIndexes = []struct {
	name  string
	table string
}{
	{"_schema_catalogs_vec_idx", "_schema_catalogs"},
	{"_schema_types_vec_idx", "_schema_types"},
	{"_schema_fields_vec_idx", "_schema_fields"},
	{"_schema_modules_vec_idx", "_schema_modules"},
}
