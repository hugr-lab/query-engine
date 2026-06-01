package mcp

// SearchResult is a generic paginated wrapper for tool responses.
type SearchResult[T any] struct {
	Total    int `json:"total"    jsonschema_description:"Total matching items in database"`
	Returned int `json:"returned" jsonschema_description:"Items returned in this page"`
	Items    []T `json:"items"`
}

// --- Discovery: Modules ---

type ModuleSearchItem struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Score       float64 `json:"score" jsonschema_description:"Relevance score 0-1, higher is better"`
}

// --- Discovery: Data Sources ---

type DataSourceSearchItem struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Type        string  `json:"type"      jsonschema_description:"Source type: duckdb, postgres, http, etc"`
	ReadOnly    bool    `json:"read_only"`
	AsModule    bool    `json:"as_module"`
	Score       float64 `json:"score"`
}

// --- Discovery: Data Objects ---

type DataObjectSearchItem struct {
	Name          string            `json:"name"          jsonschema_description:"Full type name (e.g. prefix_tablename) — call schema-type_fields on it for fields"`
	ObjectType    string            `json:"object_type"   jsonschema_description:"table or view"`
	Parameterized bool              `json:"parameterized" jsonschema_description:"true when this is a view that takes query parameters — describe_data_objects/describe_fields list them"`
	HasGeometry   bool              `json:"has_geometry"  jsonschema_description:"true when the object has at least one geometry field"`
	Module        string            `json:"module"        jsonschema_description:"Module path the object lives in — REQUIRED to nest the GraphQL query"`
	Catalog       string            `json:"catalog"       jsonschema_description:"Data source (catalog) name the object belongs to"`
	Description   string            `json:"description"`
	FieldsCount   int               `json:"fields_count"  jsonschema_description:"Number of fields on the object type"`
	Queries       []DataObjectQuery `json:"queries"       jsonschema_description:"Available query fields in the module namespace"`
	Score         float64           `json:"score"`
}

type DataObjectQuery struct {
	Name       string             `json:"name"        jsonschema_description:"Query field name in module (e.g. orders, orders_by_pk)"`
	QueryType  string             `json:"query_type"  jsonschema_description:"select, select_one, aggregate, bucket_agg"`
	ReturnType string             `json:"return_type" jsonschema_description:"GraphQL type this query returns — call schema-type_fields on it for the result fields"`
	QueryRoot  string             `json:"query_root,omitempty" jsonschema_description:"GraphQL type that hosts this query field; call schema-describe_fields(query_root,[name]) for its arguments. Set by describe_data_objects only."`
	Arguments  []FunctionArgument `json:"arguments,omitempty"   jsonschema_description:"Query field arguments (e.g. parameterized-view params). Set by describe_data_objects only."`
}

// --- Discovery: Functions ---

type FunctionSearchItem struct {
	Name        string `json:"name"        jsonschema_description:"Function field name in module"`
	Module      string `json:"module"      jsonschema_description:"Module the function lives in — REQUIRED to nest the GraphQL call"`
	Description string `json:"description,omitempty"`
	IsMutation  bool   `json:"is_mutation"`
	IsList      bool   `json:"is_list"     jsonschema_description:"Returns array"`
	// Lean search fields.
	ReturnType     string  `json:"return_type,omitempty"     jsonschema_description:"GraphQL type the function returns — call schema-type_fields on it for result fields"`
	ArgumentsCount int     `json:"arguments_count,omitempty" jsonschema_description:"Number of call arguments; use discovery-describe_functions for their names/types"`
	Score          float64 `json:"score,omitempty"`
	// Full detail — set by discovery-describe_functions only.
	Arguments []FunctionArgument  `json:"arguments,omitempty"`
	Returns   *FunctionReturnType `json:"returns,omitempty"`
}

type FunctionArgument struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Desc     string `json:"description,omitempty"`
	// Fields expands an input-object argument into its input fields.
	// Populated by describe_data_objects for the `args` argument of a
	// parameterized view (the view's parameters); empty otherwise.
	Fields []ArgInputField `json:"fields,omitempty" jsonschema_description:"Input fields of this argument's type (e.g. a parameterized view's parameters). Set only for input-object arguments like 'args'."`
}

type ArgInputField struct {
	Name string `json:"name"`
	Type string `json:"type" jsonschema_description:"GraphQL type (e.g. String!, Int)"`
}

type FunctionReturnType struct {
	TypeName string                `json:"type_name"`
	IsList   bool                  `json:"is_list"`
	Fields   []FunctionReturnField `json:"fields,omitempty" jsonschema_description:"Top fields of return type (up to 10)"`
}

type FunctionReturnField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// --- Discovery: Field Values ---

type FieldValuesResult struct {
	Stats  *FieldStats       `json:"stats,omitempty"`
	Values []FieldValueCount `json:"values,omitempty"`
}

type FieldStats struct {
	Min      any `json:"min,omitempty"`
	Max      any `json:"max,omitempty"`
	Avg      any `json:"avg,omitempty"`
	Distinct int `json:"distinct_count"`
}

type FieldValueCount struct {
	Value any `json:"value"`
	Count int `json:"count"`
}

// --- Schema: Type Info ---

type TypeInfo struct {
	Name             string `json:"name"`
	Kind             string `json:"kind"     jsonschema_description:"OBJECT, INPUT_OBJECT, ENUM, SCALAR"`
	Module           string `json:"module"`
	HugrType         string `json:"hugr_type" jsonschema_description:"table, view, module, filter, data_input, etc"`
	Catalog          string `json:"catalog"`
	FieldsTotal      int    `json:"fields_total"`
	HasGeometryField bool   `json:"has_geometry_field"`
	HasFieldWithArgs bool   `json:"has_field_with_arguments"`
	Description      string `json:"description,omitempty"`
	LongDescription  string `json:"long_description,omitempty"`
}

// --- Schema: Enum Values ---

type EnumValuesResult struct {
	Name        string          `json:"name"        jsonschema_description:"Enum type name"`
	Description string          `json:"description" jsonschema_description:"Enum type description"`
	Values      []EnumValueInfo `json:"values"      jsonschema_description:"List of enum values"`
}

type EnumValueInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// --- Schema: Type Fields ---

type TypeFieldInfo struct {
	Name        string              `json:"name"`
	FieldType   string              `json:"field_type"      jsonschema_description:"Full GraphQL type (e.g. String!, [Int])"`
	HugrType    string              `json:"hugr_type"       jsonschema_description:"select=relation, aggregate, bucket_agg, extra_field, function"`
	IsList      bool                `json:"is_list"`
	Description string              `json:"description,omitempty"`
	ArgsCount   int                 `json:"arguments_count"`
	Arguments   []FieldArgumentInfo `json:"arguments,omitempty"`
	Score       float64             `json:"score,omitempty"`
}

type FieldArgumentInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Desc     string `json:"description,omitempty"`
}

// distanceToScore converts embedding distance (0=identical) to a 0-1 score (1=best).
func distanceToScore(d float64) float64 {
	if d < 0 {
		return 0
	}
	return 1 - d
}
