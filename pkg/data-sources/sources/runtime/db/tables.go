package db

// coreDBTables lists all CoreDB tables eligible for export/import.
// Excludes _cluster_nodes (node-local, not portable).
var coreDBTables = []string{
	"version",
	"data_sources",
	"data_source_catalogs",
	"catalog_sources",
	"roles",
	"permissions",
	"api_keys",
	"_schema_catalogs",
	"_schema_catalog_dependencies",
	"_schema_types",
	"_schema_fields",
	"_schema_arguments",
	"_schema_enum_values",
	"_schema_directives",
	"_schema_modules",
	"_schema_module_type_catalogs",
	"_schema_data_objects",
	"_schema_data_object_queries",
	"_schema_settings",
}

// schemaTables lists only _schema_* tables for reset operations.
var schemaTables = []string{
	"_schema_data_object_queries",
	"_schema_data_objects",
	"_schema_module_type_catalogs",
	"_schema_modules",
	"_schema_catalog_dependencies",
	"_schema_enum_values",
	"_schema_arguments",
	"_schema_fields",
	"_schema_types",
	"_schema_directives",
	"_schema_catalogs",
}
