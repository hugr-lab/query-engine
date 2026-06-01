package mcp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/types"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) searchModules(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 5)
	minScore := req.GetFloat("min_score", 0.3)
	if query == "" {
		return toolResultError("query is required"), nil
	}

	var items []struct {
		Name     string  `json:"name"`
		Desc     string  `json:"description"`
		Distance float64 `json:"_distance_to_query"`
	}
	err := s.queryScanAdmin(ctx, `query($query: String!) {
		core {
			catalog {
				modules(
					order_by: [{field: "_distance_to_query", direction: ASC}]
				) @cache(ttl: "10m") {
					name
					description
					_distance_to_query(query: $query)
				}
			}
		}
	}`, map[string]any{"query": query}, "core.catalog.modules", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	var result []ModuleSearchItem
	var total int
	for _, item := range items {
		if !filter.visibleModule(item.Name) {
			continue
		}
		total++
		if len(result) >= topK {
			continue
		}
		score := distanceToScore(item.Distance)
		if minScore > 0 && score < minScore {
			continue
		}
		result = append(result, ModuleSearchItem{
			Name:        item.Name,
			Description: item.Desc,
			Score:       score,
		})
	}

	return toolResultJSON(SearchResult[ModuleSearchItem]{
		Total:    total,
		Returned: len(result),
		Items:    result,
	}), nil
}

func (s *Server) searchDataSources(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 5)
	minScore := req.GetFloat("min_score", 0.3)
	if query == "" {
		return toolResultError("query is required"), nil
	}

	var items []struct {
		Name     string  `json:"name"`
		Desc     string  `json:"description"`
		Type     string  `json:"type"`
		ReadOnly bool    `json:"read_only"`
		AsModule bool    `json:"as_module"`
		Distance float64 `json:"_distance_to_query"`
	}
	err := s.queryScanAdmin(ctx, `query($query: String!) {
		core {
			catalog {
				schema_catalogs(
					order_by: [{field: "_distance_to_query", direction: ASC}]
				) @cache(ttl: "10m") {
					name
					description
					type
					read_only
					as_module
					_distance_to_query(query: $query)
				}
			}
		}
	}`, map[string]any{"query": query}, "core.catalog.schema_catalogs", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	var result []DataSourceSearchItem
	var total int
	for _, item := range items {
		if !filter.visibleDataSource(item.Name) {
			continue
		}
		total++
		if len(result) >= topK {
			continue
		}
		score := distanceToScore(item.Distance)
		if minScore > 0 && score < minScore {
			continue
		}
		result = append(result, DataSourceSearchItem{
			Name:        item.Name,
			Description: item.Desc,
			Type:        item.Type,
			ReadOnly:    item.ReadOnly,
			AsModule:    item.AsModule,
			Score:       score,
		})
	}

	return toolResultJSON(SearchResult[DataSourceSearchItem]{
		Total:    total,
		Returned: len(result),
		Items:    result,
	}), nil
}

func (s *Server) searchModuleDataObjects(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	module := req.GetString("module", "")
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 5)
	minScore := req.GetFloat("min_score", 0.3)
	if query == "" {
		return toolResultError("query is required"), nil
	}

	// Module filter resolution (4 cases — `module` is required-with-
	// empty-allowed at the protocol layer):
	//
	//   module=""  + include_sub_modules=false → root-only namespace
	//     (data objects whose `module` field is exactly "").
	//   module=""  + include_sub_modules=true  → no module filter at
	//     all; the lookup spans the entire catalogue including root.
	//   module="x" + include_sub_modules=false → strict eq match.
	//   module="x" + include_sub_modules=true  → prefix match (x and
	//     every sub-module rooted at x).
	includeSub := req.GetBool("include_sub_modules", true)
	typesFilter := map[string]any{
		"hugr_type": map[string]any{"in": []string{"table", "view"}},
	}
	switch {
	case module == "" && includeSub:
		// Whole catalogue — omit the module clause entirely.
	case module == "" && !includeSub:
		typesFilter["module"] = map[string]any{"eq": ""}
	case includeSub:
		typesFilter["module"] = map[string]any{"like": module + "%"}
	default:
		typesFilter["module"] = map[string]any{"eq": module}
	}

	var items []dataObjectScan
	err := s.queryScanAdmin(ctx, dataObjectGQL(false, true), map[string]any{
		"filter": typesFilter,
		"limit":  100,
		"query":  query,
	}, "core.catalog.types", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	var result []DataObjectSearchItem
	var total int
	for _, item := range items {
		if !filter.visibleType(item.Name) {
			continue
		}
		total++
		if len(result) >= topK {
			continue
		}
		score := distanceToScore(item.Distance)
		if minScore > 0 && score < minScore {
			continue
		}
		result = append(result, buildDataObjectItem(filter, item, score, false))
	}

	return toolResultJSON(SearchResult[DataObjectSearchItem]{
		Total:    total,
		Returned: len(result),
		Items:    result,
	}), nil
}

// dataObjectScan is the scanned `core.catalog.types` row shared by
// searchModuleDataObjects (lean) and describeDataObjects (with args).
// Everything a data-object item needs comes from ONE nested query:
// fields_aggregation gives the field count + the type list (geometry
// probe), and the data_object → queries → field nesting gives each
// query's return type and (describe only) its arguments. args_type_name
// on the data_object is the parameterized-view signal.
type dataObjectScan struct {
	Name      string  `json:"name"`
	Desc      string  `json:"description"`
	HugrType  string  `json:"hugr_type"`
	Module    string  `json:"module"`
	Catalog   string  `json:"catalog"`
	Distance  float64 `json:"_distance_to_query"`
	FieldsAgg struct {
		Count     int `json:"_rows_count"`
		FieldType struct {
			List []string `json:"list"`
		} `json:"field_type"`
	} `json:"fields_aggregation"`
	DataObject []struct {
		ArgsTypeName string `json:"args_type_name"`
		ArgsType     struct {
			Fields []struct {
				Name      string `json:"name"`
				FieldType string `json:"field_type"`
			} `json:"fields"`
		} `json:"args_type"`
		Queries []struct {
			Name      string `json:"name"`
			QueryType string `json:"query_type"`
			QueryRoot string `json:"query_root"`
			Field     struct {
				FieldTypeName string `json:"field_type_name"`
				Arguments     []struct {
					Name    string `json:"name"`
					ArgType string `json:"arg_type"`
					Desc    string `json:"description"`
				} `json:"arguments"`
			} `json:"field"`
		} `json:"queries"`
	} `json:"data_object"`
}

// dataObjectGQL builds the catalog query for data objects. withArgs
// (describe) additionally pulls each query field's arguments (the
// parameterized-view params + the standard relation args); the lean
// search variant omits them. withRelevance (search) ranks by embedding
// distance against $query; describe (exact names) skips the ranking and
// the $query variable.
func dataObjectGQL(withArgs, withRelevance bool) string {
	argSel, argsTypeSel := "", ""
	if withArgs {
		// describe surfaces ONLY the parameterized-view `args` argument
		// (its param fields expand below). The standard relation args
		// (filter/order_by/limit/offset/distinct_on) are identical
		// boilerplate on every data object and add nothing per-object,
		// so they stay out even in describe; a caller that truly needs
		// them reads query_root via schema-describe_fields.
		argSel = `arguments(filter: { name: { eq: "args" }, is_arg_default: { eq: false } }) { name arg_type description }`
		// args_type is the params bundle; expand its input fields once
		// (nested_limit — a plain limit null-outs on a nested relation)
		// and attach them to the `args` argument when building the result.
		argsTypeSel = `args_type { fields(nested_limit: 50) { name field_type } }`
	}
	sig, distSel, orderBy := "query($filter: core_types_filter, $limit: Int)", "", ""
	if withRelevance {
		sig = "query($filter: core_types_filter, $limit: Int, $query: String!)"
		distSel = "_distance_to_query(query: $query)"
		orderBy = `order_by: [{field: "_distance_to_query", direction: ASC}]`
	}
	return fmt.Sprintf(`%s {
		core {
			catalog {
				types(filter: $filter, %s limit: $limit) @cache(ttl: "10m") {
					name
					hugr_type
					description
					module
					catalog
					%s
					fields_aggregation { _rows_count field_type { list(distinct: true) } }
					data_object {
						args_type_name
						%s
						queries {
							name
							query_type
							query_root
							field {
								field_type_name
								%s
							}
						}
					}
				}
			}
		}
	}`, sig, orderBy, distSel, argsTypeSel, argSel)
}

// buildDataObjectItem assembles the public item from a scanned type
// row. withArgs (the describe path) fills each query's QueryRoot +
// Arguments; the lean search path leaves them empty so the candidate
// list stays small.
func buildDataObjectItem(filter *mcpFilter, item dataObjectScan, score float64, withArgs bool) DataObjectSearchItem {
	out := DataObjectSearchItem{
		Name:        item.Name,
		ObjectType:  item.HugrType,
		HasGeometry: hasGeometryField(item.FieldsAgg.FieldType.List),
		Module:      item.Module,
		Catalog:     item.Catalog,
		Description: item.Desc,
		FieldsCount: item.FieldsAgg.Count,
		Score:       score,
	}
	if len(item.DataObject) > 0 {
		do := item.DataObject[0]
		out.Parameterized = strings.TrimSpace(do.ArgsTypeName) != ""
		// The parameterized view's params (the `args` input type's
		// fields) — attached to each query's `args` argument below.
		var viewParams []ArgInputField
		for _, pf := range do.ArgsType.Fields {
			viewParams = append(viewParams, ArgInputField{Name: pf.Name, Type: pf.FieldType})
		}
		for _, q := range do.Queries {
			if !filter.visibleField(q.QueryRoot, q.Name) {
				continue
			}
			dq := DataObjectQuery{
				Name:       q.Name,
				QueryType:  q.QueryType,
				ReturnType: q.Field.FieldTypeName,
			}
			if withArgs {
				dq.QueryRoot = q.QueryRoot
				for _, a := range q.Field.Arguments {
					arg := FunctionArgument{
						Name:     a.Name,
						Type:     a.ArgType,
						Required: strings.HasSuffix(strings.TrimSuffix(a.ArgType, "]"), "!"),
						Desc:     a.Desc,
					}
					if a.Name == "args" {
						arg.Fields = viewParams
					}
					dq.Arguments = append(dq.Arguments, arg)
				}
			}
			out.Queries = append(out.Queries, dq)
		}
	}
	return out
}

// hasGeometryField reports whether any field type in the list is a
// geometry type (drives DataObjectSearchItem.HasGeometry).
func hasGeometryField(fieldTypes []string) bool {
	for _, ft := range fieldTypes {
		if isGeometryType(ft) {
			return true
		}
	}
	return false
}

// describeDataObjects returns the FULL catalogue record for the
// EXACT-name data objects in `names` — the describe half of the
// data-object surface. Deterministic name lookup (no semantic
// scoring), batched: the caller passes every type name it already
// knows (from a prior search, the user's literal input, a stored
// reference) and gets them all in one round-trip. Type names are
// globally unique across the catalogue, so no module hint is needed.
//
// Beyond the lean search shape each item adds, per query, the
// `query_root` (the GraphQL type hosting the query field) and the
// query's `arguments` — including a parameterized view's params
// (`args: <view>_args`) — so the model can build the call without a
// further round-trip. Score is 1.0. Names that don't resolve to a
// visible table/view are silently skipped; an all-miss request
// returns a structured not_found error.
func (s *Server) describeDataObjects(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var names []string
	for _, n := range req.GetStringSlice("names", nil) {
		if t := strings.TrimSpace(n); t != "" {
			names = append(names, t)
		}
	}
	if len(names) == 0 {
		return toolResultError("names is required — pass one or more type names (e.g. from discovery-search_module_data_objects)"), nil
	}

	var items []dataObjectScan
	err := s.queryScanAdmin(ctx, dataObjectGQL(true, false), map[string]any{
		"filter": map[string]any{
			"name":      map[string]any{"in": names},
			"hugr_type": map[string]any{"in": []string{"table", "view"}},
		},
		"limit": len(names),
	}, "core.catalog.types", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	result := make([]DataObjectSearchItem, 0, len(items))
	for _, item := range items {
		if !filter.visibleType(item.Name) {
			continue
		}
		result = append(result, buildDataObjectItem(filter, item, 1.0, true))
	}
	if len(result) == 0 {
		return toolResultError(fmt.Sprintf("no data object found for %v", names)), nil
	}

	return toolResultJSON(SearchResult[DataObjectSearchItem]{
		Total:    len(result),
		Returned: len(result),
		Items:    result,
	}), nil
}

func (s *Server) searchModuleFunctions(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	module := req.GetString("module", "")
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 10)
	includeMutations := req.GetBool("include_mutations", false)
	includeSubModules := req.GetBool("include_sub_modules", true)
	if module == "" || query == "" {
		return toolResultError("module and query are required"), nil
	}

	// Step 1: Get function root type names for the module(s).
	moduleFilter := map[string]any{"eq": module}
	if includeSubModules {
		moduleFilter = map[string]any{"like": module + "%"}
	}

	var modules []struct {
		Name            string `json:"name"`
		FunctionRoot    string `json:"function_root"`
		MutFunctionRoot string `json:"mut_function_root"`
	}
	err := s.queryScanAdmin(ctx, `query($filter: core_modules_filter) {
		core {
			catalog {
				modules(filter: $filter) {
					name
					function_root
					mut_function_root
				}
			}
		}
	}`, map[string]any{
		"filter": map[string]any{"name": moduleFilter},
	}, "core.catalog.modules", &modules)
	if err != nil {
		return toolResultError(fmt.Sprintf("module lookup: %v", err)), nil
	}

	// Collect root type names.
	type rootInfo struct {
		typeName   string
		module     string
		isMutation bool
	}
	var roots []rootInfo
	for _, m := range modules {
		if m.FunctionRoot != "" {
			roots = append(roots, rootInfo{typeName: m.FunctionRoot, module: m.Name, isMutation: false})
		}
		if includeMutations && m.MutFunctionRoot != "" {
			roots = append(roots, rootInfo{typeName: m.MutFunctionRoot, module: m.Name, isMutation: true})
		}
	}
	if len(roots) == 0 {
		return toolResultJSON(SearchResult[FunctionSearchItem]{Items: []FunctionSearchItem{}}), nil
	}

	// Build type_name filter.
	typeNames := make([]string, len(roots))
	for i, r := range roots {
		typeNames[i] = r.typeName
	}

	// Build module→isMutation lookup.
	rootMap := make(map[string]rootInfo)
	for _, r := range roots {
		rootMap[r.typeName] = r
	}

	// Step 2: Query the candidate function fields — LEAN. No
	// per-candidate argument trees: just the count (arguments_count)
	// + the return type name. discovery-describe_functions returns
	// the full signature for the few the caller will actually call.
	var fields []struct {
		Name          string  `json:"name"`
		Description   string  `json:"description"`
		FieldType     string  `json:"field_type"`
		FieldTypeName string  `json:"field_type_name"`
		TypeName      string  `json:"type_name"`
		Distance      float64 `json:"_distance_to_query"`
		ArgsAgg       struct {
			Count int `json:"_rows_count"`
		} `json:"arguments_aggregation"`
	}
	err = s.queryScanAdmin(ctx, `query($filter: core_fields_filter, $limit: Int, $query: String!) {
		core {
			catalog {
				fields(
					filter: $filter
					order_by: [{field: "_distance_to_query", direction: ASC}]
					limit: $limit
				) {
					name
					description
					field_type
					field_type_name
					type_name
					_distance_to_query(query: $query)
					arguments_aggregation(filter: { is_arg_default: { eq: false } }) { _rows_count }
				}
			}
		}
	}`, map[string]any{
		"filter": map[string]any{
			"type_name": map[string]any{"in": typeNames},
		},
		"limit": topK,
		"query": query,
	}, "core.catalog.fields", &fields)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	var result []FunctionSearchItem
	for _, f := range fields {
		ri := rootMap[f.TypeName]
		if ri.isMutation {
			if !filter.visibleMutationFunction(ri.module, f.Name) {
				continue
			}
		} else if !filter.visibleFunction(ri.module, f.Name) {
			continue
		}
		result = append(result, FunctionSearchItem{
			Name:           f.Name,
			Module:         ri.module,
			Description:    f.Description,
			IsMutation:     ri.isMutation,
			IsList:         strings.HasPrefix(f.FieldType, "["),
			ReturnType:     f.FieldTypeName,
			ArgumentsCount: f.ArgsAgg.Count,
			Score:          distanceToScore(f.Distance),
		})
	}

	return toolResultJSON(SearchResult[FunctionSearchItem]{
		Total:    len(fields),
		Returned: len(result),
		Items:    result,
	}), nil
}

// describeFunctions returns the FULL signature — arguments + the
// return type (with its top fields) — for the named functions in a
// module. The describe half of discovery-search_module_functions:
// batched (pass every name you know), exact-name, no scoring. Function
// field names are NOT globally unique, so a `module` is required;
// sub-modules are searched too. Both query functions and mutation
// functions are matched. Returns a structured not_found error when no
// name resolves.
func (s *Server) describeFunctions(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	module := req.GetString("module", "")
	var names []string
	for _, n := range req.GetStringSlice("names", nil) {
		if t := strings.TrimSpace(n); t != "" {
			names = append(names, t)
		}
	}
	if module == "" || len(names) == 0 {
		return toolResultError("module and names are required"), nil
	}

	// Resolve the module(s) function + mutation-function root types.
	var modules []struct {
		Name            string `json:"name"`
		FunctionRoot    string `json:"function_root"`
		MutFunctionRoot string `json:"mut_function_root"`
	}
	err := s.queryScanAdmin(ctx, `query($filter: core_modules_filter) {
		core { catalog { modules(filter: $filter) { name function_root mut_function_root } } }
	}`, map[string]any{
		"filter": map[string]any{"name": map[string]any{"like": module + "%"}},
	}, "core.catalog.modules", &modules)
	if err != nil {
		return toolResultError(fmt.Sprintf("module lookup: %v", err)), nil
	}

	type rootInfo struct {
		module     string
		isMutation bool
	}
	rootMap := make(map[string]rootInfo)
	var typeNames []string
	for _, m := range modules {
		if m.FunctionRoot != "" {
			rootMap[m.FunctionRoot] = rootInfo{module: m.Name, isMutation: false}
			typeNames = append(typeNames, m.FunctionRoot)
		}
		if m.MutFunctionRoot != "" {
			rootMap[m.MutFunctionRoot] = rootInfo{module: m.Name, isMutation: true}
			typeNames = append(typeNames, m.MutFunctionRoot)
		}
	}
	if len(typeNames) == 0 {
		return toolResultError(fmt.Sprintf("module %q has no functions", module)), nil
	}

	// One query: the named function fields with full arguments + the
	// return type's top fields (nested_limit — a plain limit on a
	// nested relation null-outs per the engine's per-parent semantics).
	var fields []struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		FieldType     string `json:"field_type"`
		FieldTypeName string `json:"field_type_name"`
		TypeName      string `json:"type_name"`
		Arguments     []struct {
			Name    string `json:"name"`
			ArgType string `json:"arg_type"`
			Desc    string `json:"description"`
		} `json:"arguments"`
		Type struct {
			Fields []struct {
				Name      string `json:"name"`
				FieldType string `json:"field_type"`
			} `json:"fields"`
		} `json:"type"`
	}
	err = s.queryScanAdmin(ctx, `query($filter: core_fields_filter, $limit: Int) {
		core {
			catalog {
				fields(filter: $filter, limit: $limit) {
					name
					description
					field_type
					field_type_name
					type_name
					arguments(filter: { is_arg_default: { eq: false } }) { name arg_type description }
					type { fields(nested_limit: 20) { name field_type } }
				}
			}
		}
	}`, map[string]any{
		"filter": map[string]any{
			"type_name": map[string]any{"in": typeNames},
			"name":      map[string]any{"in": names},
		},
		"limit": len(names) * len(typeNames),
	}, "core.catalog.fields", &fields)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	result := make([]FunctionSearchItem, 0, len(fields))
	for _, f := range fields {
		ri := rootMap[f.TypeName]
		if ri.isMutation {
			if !filter.visibleMutationFunction(ri.module, f.Name) {
				continue
			}
		} else if !filter.visibleFunction(ri.module, f.Name) {
			continue
		}
		isList := strings.HasPrefix(f.FieldType, "[")
		item := FunctionSearchItem{
			Name:        f.Name,
			Module:      ri.module,
			Description: f.Description,
			IsMutation:  ri.isMutation,
			IsList:      isList,
			Returns:     &FunctionReturnType{TypeName: f.FieldTypeName, IsList: isList},
		}
		for _, a := range f.Arguments {
			item.Arguments = append(item.Arguments, FunctionArgument{
				Name:     a.Name,
				Type:     a.ArgType,
				Required: strings.HasSuffix(strings.TrimSuffix(a.ArgType, "]"), "!"),
				Desc:     a.Desc,
			})
		}
		for _, rf := range f.Type.Fields {
			item.Returns.Fields = append(item.Returns.Fields, FunctionReturnField{Name: rf.Name, Type: rf.FieldType})
		}
		result = append(result, item)
	}
	if len(result) == 0 {
		return toolResultError(fmt.Sprintf("no function found in module %q for %v", module, names)), nil
	}

	return toolResultJSON(SearchResult[FunctionSearchItem]{
		Total:    len(result),
		Returned: len(result),
		Items:    result,
	}), nil
}

func (s *Server) fieldValues(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	objectName := req.GetString("object_name", "")
	fieldName := req.GetString("field_name", "")
	limit := req.GetInt("limit", 10)
	withStats := req.GetBool("calculate_stats", false)

	if objectName == "" || fieldName == "" {
		return toolResultError("object_name and field_name are required"), nil
	}
	if limit < 1 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	// Get filter param if provided.
	var filterArg string
	args := req.GetArguments()
	if f, ok := args["filter"]; ok && f != nil {
		// Pass filter as a GraphQL variable.
		filterArg = "filter_var"
	}

	// Check type visibility before querying data.
	filter := newMCPFilter(ctx)
	if !filter.visibleType(objectName) {
		return toolResultError(fmt.Sprintf("type %q not found or not accessible", objectName)), nil
	}

	// Look up type info to get module path.
	var typeInfo struct {
		Module string `json:"module"`
	}
	err := s.queryScanAdmin(ctx, `query($name: String!) {
		core { catalog { types_by_pk(name: $name) { module } } }
	}`, map[string]any{"name": objectName}, "core.catalog.types_by_pk", &typeInfo)
	if errors.Is(err, types.ErrNoData) {
		return toolResultError(fmt.Sprintf("type %q not found", objectName)), nil
	}
	if err != nil {
		return toolResultError(fmt.Sprintf("type lookup: %v", err)), nil
	}

	// Build module nesting.
	modulePath := typeInfo.Module
	var pre, post string
	if modulePath != "" {
		for _, p := range strings.Split(modulePath, ".") {
			pre += p + " { "
			post += " }"
		}
	}

	// Find aggregation and bucket_aggregation query names.
	var queryInfo []struct {
		Name string `json:"name"`
		Type string `json:"query_type"`
	}
	err = s.queryScanAdmin(ctx, `query($filter: core_data_object_queries_filter) {
		core { catalog { data_object_queries(filter: $filter) { name query_type } } }
	}`, map[string]any{
		"filter": map[string]any{"object_name": map[string]any{"eq": objectName}},
	}, "core.catalog.data_object_queries", &queryInfo)
	if err != nil {
		return toolResultError(fmt.Sprintf("query info lookup: %v", err)), nil
	}

	var aggName, bucketAggName string
	for _, q := range queryInfo {
		switch q.Type {
		case "aggregate":
			aggName = q.Name
		case "bucket_agg":
			bucketAggName = q.Name
		}
	}

	// Build query parts.
	var parts []string

	// Build filter string for the query.
	filterStr := ""
	if filterArg != "" {
		filterStr = fmt.Sprintf("filter: $%s", filterArg)
	}

	// Stats via aggregation — determine available stats based on field type.
	if withStats && aggName != "" {
		statsFields := aggStatsFields(s, ctx, objectName, fieldName)
		if statsFields != "" {
			if filterStr != "" {
				parts = append(parts, fmt.Sprintf(`stats: %s(%s) { field: %s { %s } }`, aggName, filterStr, fieldName, statsFields))
			} else {
				parts = append(parts, fmt.Sprintf(`stats: %s { field: %s { %s } }`, aggName, fieldName, statsFields))
			}
		}
	}

	// Top distinct values via bucket aggregation.
	if bucketAggName != "" {
		bucketArgs := fmt.Sprintf(`limit: %d`, limit)
		if filterStr != "" {
			parts = append(parts, fmt.Sprintf(`values: %s(%s %s) { key { value: %s } aggregations { _rows_count } }`,
				bucketAggName, bucketArgs, filterStr, fieldName))
		} else {
			parts = append(parts, fmt.Sprintf(`values: %s(%s) { key { value: %s } aggregations { _rows_count } }`,
				bucketAggName, bucketArgs, fieldName))
		}
	}

	if len(parts) == 0 {
		return toolResultError("no aggregation queries available for this object"), nil
	}

	// Build variables.
	vars := make(map[string]any)
	varDecl := ""
	if filterArg != "" {
		if f, ok := args["filter"]; ok {
			vars[filterArg] = f
			// We need to know the filter type name. It's typically the type_name + "_filter".
			// But we can find it from the data_object.
			varDecl = fmt.Sprintf("($%s: %s_filter)", filterArg, objectName)
		}
	}

	gql := fmt.Sprintf("query%s { %s %s %s }", varDecl, pre, strings.Join(parts, "\n"), post)

	res, err := s.querier.Query(ctx, gql, vars)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	defer res.Close()
	if res.Err() != nil {
		return toolResultError(fmt.Sprintf("query error: %v", res.Err())), nil
	}

	// Parse results.
	var fvResult FieldValuesResult
	basePath := modulePath
	if basePath != "" {
		basePath += "."
	}

	if withStats && aggName != "" {
		var stats struct {
			Field struct {
				Min      any `json:"min"`
				Max      any `json:"max"`
				Avg      any `json:"avg"`
				Distinct int `json:"distinct"`
			} `json:"field"`
		}
		if err := res.ScanData(basePath+"stats", &stats); err == nil {
			fvResult.Stats = &FieldStats{
				Min:      stats.Field.Min,
				Max:      stats.Field.Max,
				Avg:      stats.Field.Avg,
				Distinct: stats.Field.Distinct,
			}
		}
	}

	if bucketAggName != "" {
		var buckets []struct {
			Key struct {
				Value any `json:"value"`
			} `json:"key"`
			Agg struct {
				Count int `json:"_rows_count"`
			} `json:"aggregations"`
		}
		err := res.ScanData(basePath+"values", &buckets)
		switch {
		case err == nil:
			fvResult.Values = make([]FieldValueCount, 0, len(buckets))
			for _, b := range buckets {
				fvResult.Values = append(fvResult.Values, FieldValueCount{
					Value: b.Key.Value,
					Count: b.Agg.Count,
				})
			}
		case errors.Is(err, types.ErrNoData), errors.Is(err, types.ErrWrongDataPath):
			// Field genuinely has no bucketable values — leave Values empty.
		default:
			// A real scan error (not "no data") must surface as a diagnostic
			// instead of an empty {}: an opaque empty result reads as "no
			// values" and the model retries the same call; an error tells it
			// to switch tactics.
			return toolResultError(fmt.Sprintf("internal tool error: discovery-field_values could not decode the result for %s.%s. This is a tool-side failure, NOT a problem with your arguments — do not retry the same call; proceed without these field values. (detail: %v)", objectName, fieldName, err)), nil
		}
	}

	return toolResultJSON(fvResult), nil
}

// aggStatsFields returns the aggregation stats fields to query for a given field.
// It checks the field's scalar type and returns appropriate stats (e.g. no min/max/avg for strings).
func aggStatsFields(s *Server, ctx context.Context, objectName, fieldName string) string {
	// Look up the field's scalar type.
	var fields []struct {
		FieldType string `json:"field_type"`
	}
	_ = s.queryScanAdmin(ctx, `query($filter: core_fields_filter) {
		core { catalog { fields(filter: $filter, limit: 1) { field_type } } }
	}`, map[string]any{
		"filter": map[string]any{
			"type_name": map[string]any{"eq": objectName},
			"name":      map[string]any{"eq": fieldName},
		},
	}, "core.catalog.fields", &fields)

	if len(fields) == 0 {
		return "distinct: count"
	}

	ft := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(fields[0].FieldType, "!"), "["), "]")
	ft = strings.TrimSuffix(ft, "!")

	switch ft {
	case "Float", "Int", "BigInt":
		return "min max avg distinct: count"
	case "Timestamp", "Date", "DateTime":
		return "min max distinct: count"
	default:
		// String, Boolean, Geometry, etc. — only count is safe.
		return "distinct: count"
	}
}
