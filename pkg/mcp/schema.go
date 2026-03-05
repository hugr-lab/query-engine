package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) typeInfo(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	withDesc := req.GetBool("with_description", true)
	withLongDesc := req.GetBool("with_long_description", false)

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	var raw struct {
		Name     string `json:"name"`
		Kind     string `json:"kind"`
		HugrType string `json:"hugr_type"`
		Module   string `json:"module"`
		Catalog  string `json:"catalog"`
		Desc     string `json:"description"`
		LongDesc string `json:"long_description"`
		FieldsAgg struct {
			Count     int `json:"_rows_count"`
			HugrTypes struct {
				List []string `json:"list"`
			} `json:"hugr_type"`
			FieldTypes struct {
				List []string `json:"list"`
			} `json:"field_type"`
		} `json:"fields_aggregation"`
	}

	err := s.queryScan(ctx, `query($name: String!) {
		core {
			catalog {
				types_by_pk(name: $name) {
					name
					kind
					hugr_type
					module
					catalog
					description
					long_description
					fields_aggregation {
						_rows_count
						hugr_type { list(distinct: true) }
						field_type { list(distinct: true) }
					}
				}
			}
		}
	}`, map[string]any{"name": typeName}, "core.catalog.types_by_pk", &raw)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	if raw.Name == "" {
		return toolResultError(fmt.Sprintf("type %q not found", typeName)), nil
	}

	// Detect geometry fields from field_type list.
	hasGeo := false
	for _, ft := range raw.FieldsAgg.FieldTypes.List {
		if isGeometryType(ft) {
			hasGeo = true
			break
		}
	}

	// Detect fields with arguments from hugr_type list.
	hasArgs := false
	for _, ht := range raw.FieldsAgg.HugrTypes.List {
		if ht == "extra_field" || ht == "function" || ht == "mutation_function" {
			hasArgs = true
			break
		}
	}
	// Also check for arguments via catalog if not yet detected.
	if !hasArgs {
		var argsAgg struct {
			Count int `json:"_rows_count"`
		}
		argErr := s.queryScan(ctx, `query($filter: core_catalog_arguments_filter) {
			core { catalog { arguments_aggregation(filter: $filter) { _rows_count } } }
		}`, map[string]any{
			"filter": map[string]any{"type_name": map[string]any{"eq": typeName}},
		}, "core.catalog.arguments_aggregation", &argsAgg)
		if argErr == nil && argsAgg.Count > 0 {
			hasArgs = true
		}
	}

	result := TypeInfo{
		Name:             raw.Name,
		Kind:             raw.Kind,
		Module:           raw.Module,
		HugrType:         raw.HugrType,
		Catalog:          raw.Catalog,
		FieldsTotal:      raw.FieldsAgg.Count,
		HasGeometryField: hasGeo,
		HasFieldWithArgs: hasArgs,
	}
	if withDesc {
		result.Description = raw.Desc
	}
	if withLongDesc {
		result.LongDescription = raw.LongDesc
	}

	return toolResultJSON(result), nil
}

func (s *Server) typeFields(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	relevanceQuery := req.GetString("relevance_query", "")
	limit := req.GetInt("limit", 50)
	offset := req.GetInt("offset", 0)
	includeDesc := req.GetBool("include_description", false)
	includeArgs := req.GetBool("include_arguments", false)

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	type rawArg struct {
		Name    string `json:"name"`
		ArgType string `json:"arg_type"`
		Desc    string `json:"description"`
	}
	type rawField struct {
		Name          string   `json:"name"`
		FieldType     string   `json:"field_type"`
		FieldTypeName string   `json:"field_type_name"`
		Description   string   `json:"description"`
		HugrType      string   `json:"hugr_type"`
		Distance      float64  `json:"_distance_to_query,omitempty"`
		Arguments     []rawArg `json:"arguments,omitempty"`
		ArgsAgg       struct {
			Count int `json:"_rows_count"`
		} `json:"arguments_aggregation"`
	}

	vars := map[string]any{
		"filter": map[string]any{
			"type_name": map[string]any{"eq": typeName},
		},
		"limit":  limit,
		"offset": offset,
	}

	// Build field selection based on flags.
	argSelection := ""
	if includeArgs {
		argSelection = `arguments { name arg_type description }`
	}

	var fields []rawField
	var totalCount int

	if relevanceQuery != "" {
		// With relevance ranking.
		type aggResult struct {
			Total int `json:"_rows_count"`
		}
		var agg aggResult

		gql := fmt.Sprintf(`query($filter: core_catalog_fields_filter, $limit: Int, $offset: Int) {
			core {
				catalog {
					fields(
						filter: $filter
						order_by: [{field: "_distance_to_query", direction: ASC}]
						limit: $limit
						offset: $offset
					) {
						name
						field_type
						field_type_name
						description
						hugr_type
						_distance_to_query(query: %q)
						arguments_aggregation { _rows_count }
						%s
					}
					fields_aggregation(filter: $filter) { _rows_count }
				}
			}
		}`, relevanceQuery, argSelection)

		res, err := s.querier.Query(ctx, gql, vars)
		if err != nil {
			return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
		}
		defer res.Close()
		if res.Err() != nil {
			return toolResultError(fmt.Sprintf("query error: %v", res.Err())), nil
		}
		if err := res.ScanData("core.catalog.fields", &fields); err != nil {
			return toolResultError(fmt.Sprintf("scan fields: %v", err)), nil
		}
		if err := res.ScanData("core.catalog.fields_aggregation", &agg); err == nil {
			totalCount = agg.Total
		}
	} else {
		// Without relevance ranking.
		gql := fmt.Sprintf(`query($filter: core_catalog_fields_filter, $limit: Int, $offset: Int) {
			core {
				catalog {
					fields(
						filter: $filter
						order_by: [{field: "name", direction: ASC}]
						limit: $limit
						offset: $offset
					) {
						name
						field_type
						field_type_name
						description
						hugr_type
						arguments_aggregation { _rows_count }
						%s
					}
					fields_aggregation(filter: $filter) { _rows_count }
				}
			}
		}`, argSelection)

		type aggResult struct {
			Total int `json:"_rows_count"`
		}
		var agg aggResult

		res, err := s.querier.Query(ctx, gql, vars)
		if err != nil {
			return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
		}
		defer res.Close()
		if res.Err() != nil {
			return toolResultError(fmt.Sprintf("query error: %v", res.Err())), nil
		}
		if err := res.ScanData("core.catalog.fields", &fields); err != nil {
			return toolResultError(fmt.Sprintf("scan fields: %v", err)), nil
		}
		if err := res.ScanData("core.catalog.fields_aggregation", &agg); err == nil {
			totalCount = agg.Total
		}
	}

	// Convert to typed result.
	result := make([]TypeFieldInfo, 0, len(fields))
	for _, f := range fields {
		item := TypeFieldInfo{
			Name:      f.Name,
			FieldType: f.FieldType,
			HugrType:  f.HugrType,
			IsList:    strings.HasPrefix(f.FieldType, "["),
			ArgsCount: f.ArgsAgg.Count,
		}

		if includeDesc {
			item.Description = f.Description
		}

		if f.Distance > 0 {
			item.Score = distanceToScore(f.Distance)
		}

		if includeArgs && len(f.Arguments) > 0 {
			item.Arguments = make([]FieldArgumentInfo, 0, len(f.Arguments))
			for _, a := range f.Arguments {
				item.Arguments = append(item.Arguments, FieldArgumentInfo{
					Name:     a.Name,
					Type:     a.ArgType,
					Required: strings.HasSuffix(strings.TrimSuffix(a.ArgType, "]"), "!"),
					Desc:     a.Desc,
				})
			}
		}

		result = append(result, item)
	}

	return toolResultJSON(SearchResult[TypeFieldInfo]{
		Total:    totalCount,
		Returned: len(result),
		Items:    result,
	}), nil
}

func (s *Server) enumValues(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	// Use GraphQL introspection to get enum values — they are not stored in catalog fields.
	var result struct {
		Name       string `json:"name"`
		Kind       string `json:"kind"`
		EnumValues []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"enumValues"`
	}

	err := s.queryScan(ctx, `query($name: String!) {
		__type(name: $name) {
			name
			kind
			enumValues {
				name
				description
			}
		}
	}`, map[string]any{"name": typeName}, "__type", &result)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	if result.Name == "" {
		return toolResultError(fmt.Sprintf("type %q not found", typeName)), nil
	}
	if result.Kind != "ENUM" {
		return toolResultError(fmt.Sprintf("type %q is %s, not ENUM", typeName, result.Kind)), nil
	}

	return toolResultJSON(result.EnumValues), nil
}

// isGeometryType checks if a GraphQL type string is a geometry type.
func isGeometryType(fieldType string) bool {
	t := strings.TrimSuffix(strings.TrimPrefix(fieldType, "["), "]")
	t = strings.TrimSuffix(t, "!")
	return t == "Geometry" || t == "GeoJSON" || t == "GeometryCollection"
}
