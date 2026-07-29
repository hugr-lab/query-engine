package db

// The index set hugr_coredb_recreate_indexes rebuilds. It mirrors the DDL in
// core-db/hugr_catalog.sql — keep the two in sync; this list is what a running
// engine can re-apply without a migration.
//
// PostgreSQL only. The catalog tables are small and DuckDB's ART indexes only
// get in the way, which is why hugr_catalog.sql declares them under
// {{ if isPostgres }} too. Until design-036 this list covered the eleven
// compiled-schema tables; they are gone, and so is the DuckDB half.
var commonIndexes = []struct {
	name  string
	table string
	cols  string
}{
	{"idx_hc_data_objects_source", "catalog.data_objects", "data_source"},
	{"idx_hc_data_objects_module", "catalog.data_objects", "module"},
	{"idx_hc_module_sources_src", "catalog.module_data_sources", "data_source"},
	{"idx_hc_fields_source", "catalog.fields", "data_source"},
	{"idx_hc_fields_dependency", "catalog.fields", "dependency_data_source"},
	{"idx_hc_relations_dest", "catalog.relations", "destination"},
	{"idx_hc_relations_data_source", "catalog.relations", "data_source"},
	{"idx_hc_functions_source", "catalog.functions", "data_source"},
	{"idx_hc_functions_module", "catalog.functions", "module"},
	{"idx_hc_types_source", "catalog.types", "data_source"},
	{"idx_hc_annotations_parent", "catalog.annotations", "entity_kind, parent"},
}

// pgVectorIndexes contains PostgreSQL-specific HNSW vector indexes. The
// annotations overlay is the only vector-carrying table left: the legacy
// _schema_* tables kept their own vectors, and search now ranks over the
// annotations.
var pgVectorIndexes = []struct {
	name  string
	table string
}{
	{"idx_hc_annotations_vec", "catalog.annotations"},
}
