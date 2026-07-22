package store

// Property bags are the typed JSON stored in the STRUCT (DuckDB) / JSONB
// (Postgres) property columns. These Go structs marshal to JSON on write and
// the entity views declare the matching GraphQL types, so the engine's
// structural-field machinery casts the stored JSON into the requested
// selection set automatically.
//
// The bags are the TYPED decomposition of a source's directives — the raw
// directives are dropped and everything the store needs, including executable
// SQL, is a typed member here. SQL bodies are stored AND exposed (so a UI can
// display a view / function / computed-field definition): @view(sql:) →
// dataObjectProperties.SQL, @function(sql:) → functionBinding.SQL, @sql(exp:) →
// fieldProperties.SQL, @join(sql:) → joinBinding.SQL, @function_call /
// @table_function_call_join (sql:) → functionCallBinding.SQL,
// @default(insert_exp:/update_exp:) → defaultValue. The reader re-attaches these to the
// reconstructed AST for the planner. An absent directive stores NO key
// (omitempty / nil sub-structs) — the writer collapses an empty bag to SQL
// NULL, never "{}"/"[]".
//
// Field tags are the STRUCT member names in core-db/hugr_catalog.sql and must
// stay column-for-column in sync (properties_test.go pins this).

// dataObjectProperties is catalog.data_objects.properties.
type dataObjectProperties struct {
	Name         string             `json:"name,omitempty"` // physical name in the source
	SQL          string             `json:"sql,omitempty"`  // @view(sql:) body; empty for tables / native views
	SoftDelete   bool               `json:"soft_delete,omitempty"`
	IsCube       bool               `json:"is_cube,omitempty"`
	IsM2M        bool               `json:"is_m2m,omitempty"`
	IsHypertable bool               `json:"is_hypertable,omitempty"`
	Embeddings   *embeddingsConfig  `json:"embeddings,omitempty"`
	ArgsTypeName string             `json:"args_type_name,omitempty"`
	RequiredArgs bool               `json:"required_args,omitempty"`
	Unique       []uniqueConstraint `json:"unique,omitempty"`
	Dependencies []string           `json:"dependencies,omitempty"`
	Cache        *cacheSettings     `json:"cache,omitempty"`
	At           *atPin             `json:"at,omitempty"`
}

// fieldProperties is catalog.fields.properties.
type fieldProperties struct {
	Source                string               `json:"source,omitempty"`   // physical column
	Computed              bool                 `json:"computed,omitempty"` // @sql(exp:) marker
	SQL                   string               `json:"sql,omitempty"`      // @sql(exp:) computed expression
	Default               *defaultValue        `json:"default,omitempty"`
	Geometry              *geometryInfo        `json:"geometry,omitempty"`
	Measurement           bool                 `json:"measurement,omitempty"`
	TimescaleKey          bool                 `json:"timescale_key,omitempty"`
	ExcludeFilter         []string             `json:"exclude_filter,omitempty"`
	FilterRequired        bool                 `json:"filter_required,omitempty"`
	ExcludeMCP            bool                 `json:"exclude_mcp,omitempty"`
	Cache                 *cacheSettings       `json:"cache,omitempty"`
	Join                  *joinBinding         `json:"join,omitempty"`
	FunctionCall          *functionCallBinding `json:"function_call,omitempty"`
	TableFunctionCallJoin *functionCallBinding `json:"table_function_call_join,omitempty"`
}

// functionProperties is catalog.functions.properties.
type functionProperties struct {
	Function *functionBinding `json:"function,omitempty"`
	Cache    *cacheSettings   `json:"cache,omitempty"`
}

// functionArgument is one ordered entry of catalog.functions.args.
type functionArgument struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	Default     string `json:"default,omitempty"`
	ArgDefault  string `json:"arg_default,omitempty"` // server-injected @arg_default placeholder
}

type embeddingsConfig struct {
	Model    string `json:"model,omitempty"`
	Vector   string `json:"vector,omitempty"`
	Distance string `json:"distance,omitempty"`
}

type uniqueConstraint struct {
	Fields      []string `json:"fields,omitempty"`
	QuerySuffix string   `json:"query_suffix,omitempty"`
	SkipQuery   bool     `json:"skip_query,omitempty"`
}

type cacheSettings struct {
	TTL  string   `json:"ttl,omitempty"`
	Key  string   `json:"key,omitempty"`
	Tags []string `json:"tags,omitempty"`
}

type atPin struct {
	Version   any    `json:"version,omitempty"` // Int as provided by the directive (BIGINT column)
	Timestamp string `json:"timestamp,omitempty"`
}

type defaultValue struct {
	Value     any    `json:"value,omitempty"` // static default value
	Sequence  string `json:"sequence,omitempty"`
	InsertExp string `json:"insert_exp,omitempty"` // @default(insert_exp:) SQL expression
	UpdateExp string `json:"update_exp,omitempty"` // @default(update_exp:) SQL expression
}

type geometryInfo struct {
	Type string `json:"type,omitempty"`
	SRID any    `json:"srid,omitempty"` // BIGINT column
}

type joinBinding struct {
	ReferencesName   string   `json:"references_name,omitempty"`
	SourceFields     []string `json:"source_fields,omitempty"`
	ReferencesFields []string `json:"references_fields,omitempty"`
	SQL              string   `json:"sql,omitempty"` // @join(sql:) raw condition
}

type functionCallBinding struct {
	Function         functionRef `json:"function"`
	Args             any         `json:"args,omitempty"` // field → argument map
	SourceFields     []string    `json:"source_fields,omitempty"`
	ReferencesFields []string    `json:"references_fields,omitempty"`
	SQL              string      `json:"sql,omitempty"` // @function_call / @table_function_call_join (sql:) condition
}

type functionRef struct {
	Module string `json:"module,omitempty"`
	Name   string `json:"name,omitempty"`
}

type functionBinding struct {
	Name        string `json:"name,omitempty"`
	SQL         string `json:"sql,omitempty"` // @function(sql:) body
	SkipNullArg bool   `json:"skip_null_arg,omitempty"`
	IsTable     bool   `json:"is_table,omitempty"`
	JSONCast    bool   `json:"json_cast,omitempty"`
}
