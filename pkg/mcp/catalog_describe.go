package mcp

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

// catalog-describe answers "how do I call these exact ones" for names the
// agent already has — from catalog-list, catalog-search, or an error message.
// It is the rung where a data object finally reveals the QUERY FIELD NAMES to
// write; the fields themselves stay one call away (catalog-object_fields),
// because a wide object's field list is an order of magnitude more context
// than its call signature.
//
// Like every structural tool it runs in the caller's context, so the engine's
// permission filter is the only one: a name the caller may not see comes back
// as not_found, exactly as a misspelling would.

// Relations are the one unbounded list inside a description — a hub table can
// carry dozens — so they page like everything else, with relations_total
// reporting the full count. They cannot be delegated to catalog-object_fields:
// an edge that is not materialised as a field on THIS side has no field to
// list, so the description is the only place it appears.
const defaultRelationsLimit = 50

// DescribeResult is the answer for a batch of exact names.
type DescribeResult struct {
	Items    []CatalogDescription `json:"items"`
	NotFound []string             `json:"not_found,omitempty" jsonschema_description:"Names that do not exist OR that the caller may not see — the two are indistinguishable by design"`
}

// CatalogDescription is one described entity. The shape is kind-dependent;
// what does not apply to a kind is omitted.
type CatalogDescription struct {
	Kind            string `json:"kind"`
	Name            string `json:"name"`
	Description     string `json:"description,omitempty"`
	LongDescription string `json:"long_description,omitempty"`
	Module          string `json:"module,omitempty"`
	DataSource      string `json:"data_source,omitempty"`

	// Data object.
	Type            string              `json:"type,omitempty"        jsonschema_description:"TABLE or VIEW"`
	Queries         []DescribedQuery    `json:"queries,omitempty"     jsonschema_description:"The query field names to WRITE in GraphQL — copy verbatim, nested in the module path"`
	PrimaryKey      []string            `json:"primary_key,omitempty"`
	Properties      *ObjectProperties   `json:"properties,omitempty"`
	Args            []DescribedArg      `json:"args,omitempty"        jsonschema_description:"Parameterized-view arguments — pass them as the query's args:{...}"`
	Relations       []DescribedRelation `json:"relations,omitempty"`
	RelationsTotal  int                 `json:"relations_total,omitempty"`
	RelationsOffset int                 `json:"relations_offset,omitempty"`
	RelationsMore   bool                `json:"relations_has_more,omitempty" jsonschema_description:"Call again with a larger relations_offset for the rest"`
	FieldsCount     int                 `json:"fields_count,omitempty" jsonschema_description:"Call catalog-object_fields for the fields themselves"`
	DataSources     []string            `json:"data_sources,omitempty" jsonschema_description:"Every source contributing to this object — more than one means extensions added fields"`

	// Function.
	Returns string `json:"returns,omitempty"`
	IsTable *bool  `json:"is_table,omitempty" jsonschema_description:"true when the function returns a row set — select fields from it like a table"`

	// Module.
	DataObjectsCount *int `json:"data_objects_count,omitempty"`
	FunctionsCount   *int `json:"functions_count,omitempty"`
	SubmodulesCount  *int `json:"submodules_count,omitempty"`

	// Data source.
	Engine      string   `json:"engine,omitempty"`
	ReadOnly    *bool    `json:"read_only,omitempty"`
	AsModule    *bool    `json:"as_module,omitempty"`
	IsExtension *bool    `json:"is_extension,omitempty"`
	Modules     []string `json:"modules,omitempty"`
}

type DescribedQuery struct {
	Name         string         `json:"name"`
	Type         string         `json:"type" jsonschema_description:"SELECT | SELECT_ONE | AGGREGATION | BUCKET_AGGREGATION. An object may have SEVERAL SELECT_ONE queries — by primary key and by each unique key; the arguments say which"`
	RootTypeName string         `json:"root_type_name,omitempty"`
	Args         []DescribedArg `json:"args,omitempty"`
}

type DescribedArg struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

type ObjectProperties struct {
	IsCube       bool `json:"is_cube,omitempty"`
	IsM2M        bool `json:"is_m2m,omitempty"`
	IsHypertable bool `json:"is_hypertable,omitempty"`
	SoftDelete   bool `json:"soft_delete,omitempty"`
	HasVectors   bool `json:"has_vectors,omitempty" jsonschema_description:"The object has Vector fields — semantic search over its rows is possible"`
}

type DescribedRelation struct {
	Name       string `json:"name"`
	Direction  string `json:"direction"  jsonschema_description:"FORWARD (this object points out) or BACK (something points here)"`
	Kind       string `json:"kind"       jsonschema_description:"FK | M2M | JOIN"`
	FieldName  string `json:"field_name,omitempty" jsonschema_description:"The field on THIS object that navigates the edge — select it to traverse"`
	DataObject string `json:"data_object,omitempty" jsonschema_description:"The far data object"`
	Through    string `json:"through,omitempty"     jsonschema_description:"M2M junction object"`
}

// --- meta-query scan targets ---

// gqlTypeRef mirrors __Type's wrapper chain; String flattens it back to the
// SDL spelling an agent writes.
type gqlTypeRef struct {
	Kind   string      `json:"kind"`
	Name   string      `json:"name"`
	OfType *gqlTypeRef `json:"ofType"`
}

func (t *gqlTypeRef) String() string {
	if t == nil {
		return ""
	}
	switch t.Kind {
	case "NON_NULL":
		return t.OfType.String() + "!"
	case "LIST":
		return "[" + t.OfType.String() + "]"
	}
	return t.Name
}

// typeRefSelection nests ofType deep enough for every wrapper this schema
// produces ([String!]! is the widest shape at four nodes).
const typeRefSelection = `type { kind name ofType { kind name ofType { kind name ofType { kind name } } } }`

type metaInputValue struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Type        *gqlTypeRef `json:"type"`
}

func describedArgs(in []metaInputValue) []DescribedArg {
	out := make([]DescribedArg, 0, len(in))
	for _, a := range in {
		out = append(out, DescribedArg{Name: a.Name, Type: a.Type.String(), Description: a.Description})
	}
	return out
}

type metaDescribedObject struct {
	Name            string   `json:"name"`
	Type            string   `json:"type"`
	Description     string   `json:"description"`
	LongDescription string   `json:"longDescription"`
	ModuleName      string   `json:"moduleName"`
	DataSourceName  string   `json:"dataSourceName"`
	DataSources     []string `json:"dataSources"`
	PrimaryKey      []string `json:"primaryKey"`
	Properties      struct {
		IsCube       bool `json:"isCube"`
		IsM2M        bool `json:"isM2M"`
		IsHypertable bool `json:"isHypertable"`
		SoftDelete   bool `json:"softDelete"`
		HasVectors   bool `json:"hasVectors"`
	} `json:"properties"`
	Args    []metaInputValue `json:"args"`
	Queries []struct {
		Name         string           `json:"name"`
		Type         string           `json:"type"`
		RootTypeName string           `json:"rootTypeName"`
		Args         []metaInputValue `json:"args"`
	} `json:"queries"`
	Relations []struct {
		Name       string `json:"name"`
		Direction  string `json:"direction"`
		Kind       string `json:"kind"`
		FieldName  string `json:"fieldName"`
		DataObject *struct {
			Name string `json:"name"`
		} `json:"dataObject"`
		Through *struct {
			Name string `json:"name"`
		} `json:"through"`
	} `json:"relations"`
	Fields []struct {
		Name       string `json:"name"`
		McpExclude bool   `json:"mcp_exclude"`
	} `json:"fields"`
}

type metaDescribedFunction struct {
	Name            string           `json:"name"`
	Type            string           `json:"type"`
	Description     string           `json:"description"`
	LongDescription string           `json:"longDescription"`
	ModuleName      string           `json:"moduleName"`
	DataSourceName  string           `json:"dataSourceName"`
	IsTable         bool             `json:"isTable"`
	Args            []metaInputValue `json:"args"`
	Returns         *gqlTypeRef      `json:"returns"`
}

type metaDescribedModule struct {
	Name            string   `json:"name"`
	Description     string   `json:"description"`
	LongDescription string   `json:"longDescription"`
	DataSources     []string `json:"dataSources"`
	DataObjects     []struct {
		Name string `json:"name"`
	} `json:"dataObjects"`
	Functions []struct {
		Name string `json:"name"`
	} `json:"functions"`
	Modules []struct {
		Name string `json:"name"`
	} `json:"modules"`
}

type metaDescribedDataSource struct {
	metaDataSourceNode
	LongDescription string `json:"longDescription"`
}

// --- selections ---

const describeObjectSelection = `name type description longDescription moduleName dataSourceName dataSources
	primaryKey
	properties { isCube isM2M isHypertable softDelete hasVectors }
	args { name description ` + typeRefSelection + ` }
	queries { name type rootTypeName args { name ` + typeRefSelection + ` } }
	relations { name direction kind fieldName dataObject { name } through { name } }
	fields { name mcp_exclude }`

const describeFunctionSelection = `name type description longDescription moduleName dataSourceName isTable
	args { name description ` + typeRefSelection + ` }
	returns { kind name ofType { kind name ofType { kind name ofType { kind name } } } }`

const describeModuleSelection = `name description longDescription dataSources
	dataObjects { name } functions { name } modules { name }`

const describeDataSourceSelection = `name engine description longDescription readOnly asModule isExtension modules`

func (s *Server) catalogDescribe(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	kind := req.GetString("kind", "")
	if !slices.Contains(catalogKinds, kind) {
		return toolResultError(fmt.Sprintf("kind must be one of %s", strings.Join(catalogKinds, ", "))), nil
	}
	names := req.GetStringSlice("names", nil)
	if len(names) == 0 {
		return toolResultError("names is required — pass the exact names to describe"), nil
	}
	if len(names) > maxPageLimit {
		names = names[:maxPageLimit]
	}
	module := req.GetString("module", "")
	rel := pageArgs{
		limit:  clampLimit(req.GetInt("relations_limit", defaultRelationsLimit), defaultRelationsLimit),
		offset: max(0, req.GetInt("relations_offset", 0)),
	}

	res, err := s.describe(ctx, kind, module, names, rel)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	return toolResultJSON(res), nil
}

// describe batches every requested name into ONE aliased query. Aliases are
// positional (a0, a1, …) so a name never has to be a valid GraphQL alias, and
// every value travels as a variable rather than inline.
// pageArgs is a nested list's window inside a record.
type pageArgs struct {
	limit  int
	offset int
}

func (s *Server) describe(ctx context.Context, kind, module string, names []string, rel pageArgs) (*DescribeResult, error) {
	var (
		sel     string
		perName func(i int) string
	)
	vars := map[string]any{}
	switch kind {
	case kindDataObject:
		sel = describeObjectSelection
		perName = func(i int) string { return fmt.Sprintf("_dataObject(name: $n%d)", i) }
	case kindFunction:
		sel = describeFunctionSelection
		vars["m"] = module
		perName = func(i int) string { return fmt.Sprintf("_function(module: $m, name: $n%d)", i) }
	case kindModule:
		sel = describeModuleSelection
		perName = func(i int) string { return fmt.Sprintf("_module(name: $n%d)", i) }
	case kindDataSource:
		sel = describeDataSourceSelection
		perName = func(i int) string { return fmt.Sprintf("_dataSource(name: $n%d)", i) }
	}

	var sig, body strings.Builder
	for i, name := range names {
		vars[fmt.Sprintf("n%d", i)] = name
		if i > 0 {
			sig.WriteString(", ")
		}
		fmt.Fprintf(&sig, "$n%d: String!", i)
		fmt.Fprintf(&body, "a%d: %s { %s }\n", i, perName(i), sel)
	}
	if kind == kindFunction {
		sig.WriteString(", $m: String!")
	}
	query := "query(" + sig.String() + ") {\n" + body.String() + "}"

	out := &DescribeResult{Items: []CatalogDescription{}}
	switch kind {
	case kindDataObject:
		batch := map[string]*metaDescribedObject{}
		if err := s.queryScan(ctx, query, vars, "", &batch); err != nil {
			return nil, err
		}
		collectDescribed(names, batch, out, func(o *metaDescribedObject) CatalogDescription {
			return describeObject(o, rel)
		})
	case kindFunction:
		batch := map[string]*metaDescribedFunction{}
		if err := s.queryScan(ctx, query, vars, "", &batch); err != nil {
			return nil, err
		}
		collectDescribed(names, batch, out, describeFunction)
	case kindModule:
		batch := map[string]*metaDescribedModule{}
		if err := s.queryScan(ctx, query, vars, "", &batch); err != nil {
			return nil, err
		}
		collectDescribed(names, batch, out, describeModule)
	case kindDataSource:
		batch := map[string]*metaDescribedDataSource{}
		if err := s.queryScan(ctx, query, vars, "", &batch); err != nil {
			return nil, err
		}
		collectDescribed(names, batch, out, describeDataSource)
	}
	return out, nil
}

// collectDescribed walks the request order (not the map's) so the answer lines
// up with what was asked, and records every name the engine did not resolve.
func collectDescribed[T any](names []string, batch map[string]*T, out *DescribeResult,
	convert func(*T) CatalogDescription,
) {
	for i, name := range names {
		node, ok := batch[fmt.Sprintf("a%d", i)]
		if !ok || node == nil {
			out.NotFound = append(out.NotFound, name)
			continue
		}
		out.Items = append(out.Items, convert(node))
	}
}

func describeObject(o *metaDescribedObject, rel pageArgs) CatalogDescription {
	d := CatalogDescription{
		Kind: kindDataObject, Name: o.Name, Type: o.Type,
		Description: o.Description, LongDescription: o.LongDescription,
		Module: o.ModuleName, DataSource: o.DataSourceName, DataSources: o.DataSources,
		PrimaryKey: o.PrimaryKey, Args: describedArgs(o.Args),
		// Counted the way catalog-object_fields LISTS them: an @exclude_mcp
		// field and a synthetic placeholder are not fields an agent can ask
		// for, and a count the next call cannot reproduce is worse than none.
		FieldsCount: countListableFields(o),
		Properties: &ObjectProperties{
			IsCube: o.Properties.IsCube, IsM2M: o.Properties.IsM2M,
			IsHypertable: o.Properties.IsHypertable, SoftDelete: o.Properties.SoftDelete,
			HasVectors: o.Properties.HasVectors,
		},
	}
	for _, q := range o.Queries {
		d.Queries = append(d.Queries, DescribedQuery{
			Name: q.Name, Type: q.Type, RootTypeName: q.RootTypeName, Args: describedArgs(q.Args),
		})
	}
	d.RelationsTotal = len(o.Relations)
	start := min(rel.offset, d.RelationsTotal)
	end := min(start+rel.limit, d.RelationsTotal)
	d.RelationsOffset = start
	d.RelationsMore = end < d.RelationsTotal
	for _, r := range o.Relations[start:end] {
		entry := DescribedRelation{Name: r.Name, Direction: r.Direction, Kind: r.Kind, FieldName: r.FieldName}
		if r.DataObject != nil {
			entry.DataObject = r.DataObject.Name
		}
		if r.Through != nil {
			entry.Through = r.Through.Name
		}
		d.Relations = append(d.Relations, entry)
	}
	return d
}

func countListableFields(o *metaDescribedObject) int {
	n := 0
	for _, f := range o.Fields {
		if f.McpExclude || isPlaceholderField(f.Name) {
			continue
		}
		n++
	}
	return n
}

func describeFunction(f *metaDescribedFunction) CatalogDescription {
	isTable := f.IsTable
	return CatalogDescription{
		Kind: kindFunction, Name: f.Name, Type: f.Type,
		Description: f.Description, LongDescription: f.LongDescription,
		Module: f.ModuleName, DataSource: f.DataSourceName,
		Args: describedArgs(f.Args), Returns: f.Returns.String(), IsTable: &isTable,
	}
}

func describeModule(m *metaDescribedModule) CatalogDescription {
	objects, functions, submodules := len(m.DataObjects), len(m.Functions), len(m.Modules)
	return CatalogDescription{
		Kind: kindModule, Name: m.Name,
		Description: m.Description, LongDescription: m.LongDescription,
		DataSources:      m.DataSources,
		DataObjectsCount: &objects, FunctionsCount: &functions, SubmodulesCount: &submodules,
	}
}

func describeDataSource(ds *metaDescribedDataSource) CatalogDescription {
	return CatalogDescription{
		Kind: kindDataSource, Name: ds.Name, Engine: ds.Engine,
		Description: ds.Description, LongDescription: ds.LongDescription,
		ReadOnly: ds.ReadOnly, AsModule: ds.AsModule, IsExtension: ds.IsExtension,
		Modules: ds.Modules,
	}
}
