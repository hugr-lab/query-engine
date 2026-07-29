package db

import "fmt"

// coreDBTable names one table of the CoreDB, with the schema it lives in —
// "" for the default one, "catalog" for the entity namespace. The distinction
// matters because export/import addresses the same table through two databases
// (the attached CoreDB and the export file) and both must be qualified the
// same way.
type coreDBTable struct {
	schema string
	name   string
}

// in returns the table qualified for a given attached database alias.
func (t coreDBTable) in(alias string) string {
	if t.schema == "" {
		return fmt.Sprintf("%s.%q", alias, t.name)
	}
	return fmt.Sprintf("%s.%s.%q", alias, t.schema, t.name)
}

// coreDBTables lists all CoreDB tables eligible for export/import, in an order
// safe to insert sequentially. Excludes _cluster_nodes (node-local, not
// portable).
//
// The eleven _schema_* tables were dropped in design-036 and the catalog
// namespace replaced them. _schema_settings stays: despite the name it is not
// compiled-schema data but the schema_version counter.
var coreDBTables = []coreDBTable{
	{"", "version"},
	{"", "data_sources"},
	{"", "data_source_catalogs"},
	{"", "catalog_sources"},
	{"", "roles"},
	{"", "permissions"},
	{"", "api_keys"},
	{"", "_schema_settings"},
	{"catalog", "data_source_meta"},
	{"catalog", "data_source_dependencies"},
	{"catalog", "modules"},
	{"catalog", "module_data_sources"},
	{"catalog", "data_objects"},
	{"catalog", "fields"},
	{"catalog", "relations"},
	{"catalog", "functions"},
	{"catalog", "types"},
	{"catalog", "annotations"},
}

// schemaTables lists the catalog namespace only — what a schema reset clears,
// leaving the registry (data sources, roles, permissions) alone. Ordered so a
// sequential DELETE never trips a foreign key.
var schemaTables = []coreDBTable{
	{"catalog", "annotations"},
	{"catalog", "relations"},
	{"catalog", "fields"},
	{"catalog", "functions"},
	{"catalog", "types"},
	{"catalog", "data_objects"},
	{"catalog", "module_data_sources"},
	{"catalog", "modules"},
	{"catalog", "data_source_dependencies"},
	{"catalog", "data_source_meta"},
}

// annotationsTable is the curation overlay — the one table description
// import/export touches.
var annotationsTable = coreDBTable{"catalog", "annotations"}
