package store

import (
	"sort"
	"strings"
)

// The entity models mirror the CoreDB `catalog` schema tables one-to-one
// (core-db/hugr_catalog.sql). Change granularity is the data source — there are
// no per-row hashes; the writer gates on the per-source version+flags and
// rewrites a changed source's rows wholesale (tier-1).
//
// Property bags (dataObjectProperties / fieldProperties / functionProperties)
// live in properties.go; they are the typed JSON stored in the STRUCT/JSONB
// property columns.

// module is one row of catalog.modules. Parent is DERIVED from the dotted name
// by the writer (a.b.c → a.b; "" for top-level) so child selection is a plain
// equality. The root module ("") is implicit and never stored.
type module struct {
	Name        string
	Parent      string
	Description string
}

// moduleSource is one row of catalog.module_data_sources — the module→source
// CLOSURE: a source that backs data objects / functions in the module OR any
// of its submodules is recorded on the module AND every ancestor, so module
// visibility is a plain semi-join against data_source_meta with no tree walk.
type moduleSource struct {
	Module     string
	DataSource string
}

// dataObject is one row of catalog.data_objects. Name is the compiled (prefixed)
// type name; OriginalName is the source's unprefixed name (@original_name),
// re-attached on read.
type dataObject struct {
	Name         string
	OriginalName string
	DataSource   string
	Module       string
	Kind         string // table | view
	Properties   *dataObjectProperties
	Description  string
}

// field is one row of catalog.fields. Declared @join / @function_call /
// @table_function_call_join fields are ordinary field rows carrying their
// typed property bags (D17) — they are NOT relations.
type field struct {
	TypeName             string
	Name                 string
	FieldType            string
	Properties           *fieldProperties
	DataSource           string
	DependencyDataSource string
	IsPK                 bool
	Ordinal              int
	DeprecationReason    string // @deprecated(reason:) — "" means active
	Description          string
}

// relation is one row of catalog.relations: ONE logical @references edge
// (fk | m2m) readable from both sides. Identity is (Source, Name); DataSource
// is definition provenance / reconcile attribution only, never identity.
type relation struct {
	Source      string
	Name        string
	Kind        string // fk | m2m
	Destination string
	M2MObject   string
	// Physical reference field lists. An m2m relation is just two ordinary
	// @field_references on the junction table (@table(is_m2m: true)) — each a
	// plain source→target leg — so keys are []string; the m2m navigation is
	// synthesized at read time from is_m2m + the two legs.
	SourceKeys                  []string
	DestinationKeys             []string
	SourceField                 string
	SourceFieldDescription      string
	DestinationField            string
	DestinationFieldDescription string
	DataSource                  string
}

// function is one row of catalog.functions. Kind is function | mutation |
// subscription; Args is the ordered argument list stored as one JSON array.
type function struct {
	Module            string
	Name              string
	Kind              string
	DataSource        string
	Returns           string
	IsTable           bool
	Args              []functionArgument
	Properties        *functionProperties
	DeprecationReason string // @deprecated(reason:) — "" means active
	Description       string
}

// sourceType is one row of catalog.types: a residual source-defined base type
// (structural object, input, enum, interface, union) stored as raw SDL.
type sourceType struct {
	Name        string
	Kind        string
	DataSource  string
	Module      string
	Definition  string
	Description string
}

// dataSourceDependency is one row of catalog.data_source_dependencies.
type dataSourceDependency struct {
	DataSource string
	DependsOn  string
}

// desired is the full desired model state of the catalog namespace for one
// reconcile sweep, keyed by table primary keys.
type desired struct {
	modules       map[string]*module               // key: name
	moduleSources map[string]*moduleSource         // key: module\x1fsource
	dataObjects   map[string]*dataObject           // key: name
	fields        map[string]*field                // key: typeName\x1fname
	relations     map[string]*relation             // key: source\x1fname
	functions     map[string]*function             // key: module\x1fname
	types         map[string]*sourceType           // key: name
	dependencies  map[string]*dataSourceDependency // key: source\x1fdependsOn
}

func newDesired() *desired {
	return &desired{
		modules:       map[string]*module{},
		moduleSources: map[string]*moduleSource{},
		dataObjects:   map[string]*dataObject{},
		fields:        map[string]*field{},
		relations:     map[string]*relation{},
		functions:     map[string]*function{},
		types:         map[string]*sourceType{},
		dependencies:  map[string]*dataSourceDependency{},
	}
}

// pkKey joins primary-key parts into a map key (composite-PK tables).
func pkKey(parts ...string) string { return strings.Join(parts, "\x1f") }

// sortedKeys returns a map's keys in deterministic order (stable SQL).
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
