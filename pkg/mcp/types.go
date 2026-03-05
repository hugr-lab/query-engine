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
	Name        string               `json:"name"        jsonschema_description:"Full type name (e.g. prefix_tablename)"`
	Module      string               `json:"module"      jsonschema_description:"Module path (e.g. sales.analytics)"`
	Description string               `json:"description"`
	ObjectType  string               `json:"object_type" jsonschema_description:"table or view"`
	Score       float64              `json:"score"`
	Queries     []DataObjectQuery    `json:"queries"     jsonschema_description:"Available query fields in module namespace"`
	Fields      []DataObjectFieldBrief `json:"fields,omitempty" jsonschema_description:"Top fields (scalars and relations)"`
}

type DataObjectQuery struct {
	Name      string `json:"name"       jsonschema_description:"Query field name in module (e.g. orders, orders_by_pk)"`
	QueryType string `json:"query_type" jsonschema_description:"select, select_one, aggregate, bucket_agg"`
}

type DataObjectFieldBrief struct {
	Name     string `json:"name"`
	Type     string `json:"type"      jsonschema_description:"GraphQL type (e.g. String!, [prefix_orders])"`
	HugrType string `json:"hugr_type" jsonschema_description:"empty=scalar, select=relation, aggregate, bucket_agg, extra_field, function"`
}

// --- Discovery: Functions ---

type FunctionSearchItem struct {
	Name        string             `json:"name"        jsonschema_description:"Function field name in module"`
	Module      string             `json:"module"`
	Description string             `json:"description"`
	IsMutation  bool               `json:"is_mutation"`
	IsList      bool               `json:"is_list"     jsonschema_description:"Returns array"`
	Score       float64            `json:"score"`
	Arguments   []FunctionArgument `json:"arguments"`
	Returns     FunctionReturnType `json:"returns"`
}

type FunctionArgument struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Desc     string `json:"description,omitempty"`
}

type FunctionReturnType struct {
	TypeName string              `json:"type_name"`
	IsList   bool               `json:"is_list"`
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
