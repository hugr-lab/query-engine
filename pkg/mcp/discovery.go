package mcp

import (
	"context"
	"fmt"
	"strings"

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
	err := s.queryScanAdmin(ctx, `query($query: String!, $limit: Int) {
		core {
			catalog {
				modules(
					order_by: [{field: "_distance_to_query", direction: ASC}]
					limit: $limit
				) {
					name
					description
					_distance_to_query(query: $query)
				}
			}
		}
	}`, map[string]any{"query": query, "limit": topK}, "core.catalog.modules", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	var result []ModuleSearchItem
	for _, item := range items {
		if !filter.visibleModule(item.Name) {
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
		Total:    len(items),
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
	err := s.queryScanAdmin(ctx, `query($query: String!, $limit: Int) {
		core {
			catalog {
				schema_catalogs(
					order_by: [{field: "_distance_to_query", direction: ASC}]
					limit: $limit
				) {
					name
					description
					type
					read_only
					as_module
					_distance_to_query(query: $query)
				}
			}
		}
	}`, map[string]any{"query": query, "limit": topK}, "core.catalog.schema_catalogs", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	var result []DataSourceSearchItem
	for _, item := range items {
		if !filter.visibleDataSource(item.Name) {
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
		Total:    len(items),
		Returned: len(result),
		Items:    result,
	}), nil
}

func (s *Server) searchModuleDataObjects(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	module := req.GetString("module", "")
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 5)
	minScore := req.GetFloat("min_score", 0.3)
	if module == "" || query == "" {
		return toolResultError("module and query are required"), nil
	}

	moduleFilter := map[string]any{"eq": module}
	if req.GetBool("include_sub_modules", true) {
		moduleFilter = map[string]any{"like": module + "%"}
	}

	var items []struct {
		Name       string  `json:"name"`
		Desc       string  `json:"description"`
		HugrType   string  `json:"hugr_type"`
		Module     string  `json:"module"`
		Distance   float64 `json:"_distance_to_query"`
		DataObject []struct {
			Queries []struct {
				Name      string `json:"name"`
				QueryType string `json:"query_type"`
				QueryRoot string `json:"query_root"`
			} `json:"queries"`
		} `json:"data_object"`
	}
	err := s.queryScanAdmin(ctx, `query($filter: core_types_filter, $limit: Int, $query: String!) {
		core {
			catalog {
				types(
					filter: $filter
					order_by: [{field: "_distance_to_query", direction: ASC}]
					limit: $limit
				) {
					name
					hugr_type
					description
					module
					_distance_to_query(query: $query)
					data_object {
						queries {
							name
							query_type
							query_root
						}
					}
				}
			}
		}
	}`, map[string]any{
		"filter": map[string]any{
			"hugr_type": map[string]any{"in": []string{"table", "view"}},
			"module":    moduleFilter,
		},
		"limit": topK,
		"query": query,
	}, "core.catalog.types", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	var result []DataObjectSearchItem
	for _, item := range items {
		if !filter.visibleType(item.Name) {
			continue
		}
		score := distanceToScore(item.Distance)
		if minScore > 0 && score < minScore {
			continue
		}

		var queries []DataObjectQuery
		if len(item.DataObject) > 0 {
			for _, q := range item.DataObject[0].Queries {
				if !filter.visibleField(q.QueryRoot, q.Name) {
					continue
				}
				queries = append(queries, DataObjectQuery{Name: q.Name, QueryType: q.QueryType})
			}
		}

		result = append(result, DataObjectSearchItem{
			Name:        item.Name,
			Module:      item.Module,
			Description: item.Desc,
			ObjectType:  item.HugrType,
			Score:       score,
			Queries:     queries,
		})
	}

	return toolResultJSON(SearchResult[DataObjectSearchItem]{
		Total:    len(items),
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

	// Step 2: Query fields with arguments and distance.
	var fields []struct {
		Name          string  `json:"name"`
		Description   string  `json:"description"`
		FieldType     string  `json:"field_type"`
		FieldTypeName string  `json:"field_type_name"`
		TypeName      string  `json:"type_name"`
		Distance      float64 `json:"_distance_to_query"`
		Arguments     []struct {
			Name         string `json:"name"`
			ArgType      string `json:"arg_type"`
			Desc         string `json:"description"`
			IsArgDefault bool   `json:"is_arg_default"`
		} `json:"arguments"`
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
					arguments(filter: { is_arg_default: { eq: false } }) {
						name
						arg_type
						description
					}
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
		} else {
			if !filter.visibleFunction(ri.module, f.Name) {
				continue
			}
		}

		score := distanceToScore(f.Distance)

		var args []FunctionArgument
		for _, a := range f.Arguments {
			args = append(args, FunctionArgument{
				Name:     a.Name,
				Type:     a.ArgType,
				Required: strings.HasSuffix(strings.TrimSuffix(a.ArgType, "]"), "!"),
				Desc:     a.Desc,
			})
		}

		isList := strings.HasPrefix(f.FieldType, "[")

		result = append(result, FunctionSearchItem{
			Name:        f.Name,
			Module:      ri.module,
			Description: f.Description,
			IsMutation:  ri.isMutation,
			IsList:      isList,
			Score:       score,
			Arguments:   args,
			Returns: FunctionReturnType{
				TypeName: f.FieldTypeName,
				IsList:   isList,
			},
		})
	}

	return toolResultJSON(SearchResult[FunctionSearchItem]{
		Total:    len(fields),
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
		if err := res.ScanData(basePath+"values", &buckets); err == nil {
			fvResult.Values = make([]FieldValueCount, 0, len(buckets))
			for _, b := range buckets {
				fvResult.Values = append(fvResult.Values, FieldValueCount{
					Value: b.Key.Value,
					Count: b.Agg.Count,
				})
			}
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
