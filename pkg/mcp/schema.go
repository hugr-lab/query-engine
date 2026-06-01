package mcp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/types"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) typeInfo(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	withDesc := req.GetBool("with_description", true)
	withLongDesc := req.GetBool("with_long_description", false)

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	filter := newMCPFilter(ctx)
	if !filter.visibleType(typeName) {
		return toolResultError(fmt.Sprintf("type %q not found or not accessible", typeName)), nil
	}

	var raw struct {
		Name      string `json:"name"`
		Kind      string `json:"kind"`
		HugrType  string `json:"hugr_type"`
		Module    string `json:"module"`
		Catalog   string `json:"catalog"`
		Desc      string `json:"description"`
		LongDesc  string `json:"long_description"`
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

	err := s.queryScanAdmin(ctx, `query($name: String!) {
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
	if errors.Is(err, types.ErrNoData) {
		return toolResultError(fmt.Sprintf("type %q not found", typeName)), nil
	}
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
		argErr := s.queryScanAdmin(ctx, `query($filter: core_arguments_filter) {
			core { catalog { arguments_aggregation(filter: $filter) { _rows_count } } }
		}`, map[string]any{
			"filter": map[string]any{
				"type_name":      map[string]any{"eq": typeName},
				"is_arg_default": map[string]any{"eq": false},
			},
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

	// type_fields is the LEAN list surface: it never emits per-field
	// argument trees. hugr_type already classifies the argument
	// profile and arguments_count flags which fields carry args; the
	// caller follows up with schema-describe_fields for the exact
	// arguments of the specific fields it will use. Keeping the
	// selection empty is what makes this ~80% smaller than the old
	// include_arguments dump (the _join/_spatial operator types
	// especially). The %s slot in the queries below stays blank.
	argSelection := ""

	var fields []rawField
	var totalCount int

	if relevanceQuery != "" {
		// With relevance ranking.
		type aggResult struct {
			Total int `json:"_rows_count"`
		}
		var agg aggResult

		vars["relevance_query"] = relevanceQuery

		gql := fmt.Sprintf(`query($filter: core_fields_filter, $limit: Int, $offset: Int, $relevance_query: String!) {
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
						_distance_to_query(query: $relevance_query)
						arguments_aggregation(filter: { is_arg_default: { eq: false } }) { _rows_count }
						%s
					}
					fields_aggregation(filter: $filter) { _rows_count }
				}
			}
		}`, argSelection)

		res, err := s.querier.Query(auth.ContextWithFullAccess(ctx), gql, vars)
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
		gql := fmt.Sprintf(`query($filter: core_fields_filter, $limit: Int, $offset: Int) {
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
						arguments_aggregation(filter: { is_arg_default: { eq: false } }) { _rows_count }
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

		res, err := s.querier.Query(auth.ContextWithFullAccess(ctx), gql, vars)
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

	// Convert to typed result, filtering by visibility.
	filter := newMCPFilter(ctx)
	result := make([]TypeFieldInfo, 0, len(fields))
	for _, f := range fields {
		if !filter.visibleField(typeName, f.Name) {
			continue
		}
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

		result = append(result, item)
	}

	return toolResultJSON(SearchResult[TypeFieldInfo]{
		Total:    totalCount,
		Returned: len(result),
		Items:    result,
	}), nil
}

// describeFields is the DESCRIBE half of the type_fields/describe_fields
// split: it returns the FULL per-field detail — arguments (name, type,
// required, description) + description — for ONLY the fields named in
// `fields`. The caller reaches for it after schema-type_fields listed
// the fields and their hugr_type, once it knows which field(s) it will
// actually use and needs the exact argument names/types (filter inputs,
// bucket args, function params, parameterized-view query params).
// Scoping to named fields keeps the payload tiny even on the wide
// operator types (_join / _spatial) whose full argument dump used to
// dominate context.
func (s *Server) describeFields(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}
	var names []string
	for _, n := range req.GetStringSlice("fields", nil) {
		if t := strings.TrimSpace(n); t != "" {
			names = append(names, t)
		}
	}
	if len(names) == 0 {
		return toolResultError("fields is required — pass one or more field names from schema-type_fields"), nil
	}

	type rawArg struct {
		Name    string `json:"name"`
		ArgType string `json:"arg_type"`
		Desc    string `json:"description"`
	}
	type rawField struct {
		Name        string   `json:"name"`
		FieldType   string   `json:"field_type"`
		Description string   `json:"description"`
		HugrType    string   `json:"hugr_type"`
		Arguments   []rawArg `json:"arguments,omitempty"`
		ArgsAgg     struct {
			Count int `json:"_rows_count"`
		} `json:"arguments_aggregation"`
	}

	var fields []rawField
	err := s.queryScanAdmin(ctx, `query($filter: core_fields_filter, $limit: Int) {
		core {
			catalog {
				fields(filter: $filter, order_by: [{field: "name", direction: ASC}], limit: $limit) {
					name
					field_type
					description
					hugr_type
					arguments_aggregation(filter: { is_arg_default: { eq: false } }) { _rows_count }
					arguments(filter: { is_arg_default: { eq: false } }) { name arg_type description }
				}
			}
		}
	}`, map[string]any{
		"filter": map[string]any{
			"type_name": map[string]any{"eq": typeName},
			"name":      map[string]any{"in": names},
		},
		"limit": len(names),
	}, "core.catalog.fields", &fields)
	if errors.Is(err, types.ErrNoData) {
		return toolResultError(fmt.Sprintf("no fields named %v on type %q", names, typeName)), nil
	}
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	filter := newMCPFilter(ctx)
	result := make([]TypeFieldInfo, 0, len(fields))
	for _, f := range fields {
		if !filter.visibleField(typeName, f.Name) {
			continue
		}
		item := TypeFieldInfo{
			Name:        f.Name,
			FieldType:   f.FieldType,
			HugrType:    f.HugrType,
			IsList:      strings.HasPrefix(f.FieldType, "["),
			ArgsCount:   f.ArgsAgg.Count,
			Description: f.Description,
		}
		for _, a := range f.Arguments {
			item.Arguments = append(item.Arguments, FieldArgumentInfo{
				Name:     a.Name,
				Type:     a.ArgType,
				Required: strings.HasSuffix(strings.TrimSuffix(a.ArgType, "]"), "!"),
				Desc:     a.Desc,
			})
		}
		result = append(result, item)
	}
	if len(result) == 0 {
		return toolResultError(fmt.Sprintf("no matching visible fields on %q for %v", typeName, names)), nil
	}

	return toolResultJSON(SearchResult[TypeFieldInfo]{
		Total:    len(result),
		Returned: len(result),
		Items:    result,
	}), nil
}

func (s *Server) enumValues(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	var raw struct {
		Name        string          `json:"name"`
		Kind        string          `json:"kind"`
		Description string          `json:"description"`
		EnumValues  []EnumValueInfo `json:"enum_values"`
	}

	err := s.queryScanAdmin(ctx, `query($name: String!) {
		core {
			catalog {
				types_by_pk(name: $name) {
					name
					kind
					description
					enum_values {
						name
						description
					}
				}
			}
		}
	}`, map[string]any{"name": typeName}, "core.catalog.types_by_pk", &raw)
	if errors.Is(err, types.ErrNoData) {
		return toolResultError(fmt.Sprintf("type %q not found", typeName)), nil
	}
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	if raw.Name == "" {
		return toolResultError(fmt.Sprintf("type %q not found", typeName)), nil
	}
	if raw.Kind != "ENUM" {
		return toolResultError(fmt.Sprintf("type %q is %s, not ENUM", typeName, raw.Kind)), nil
	}

	return toolResultJSON(EnumValuesResult{
		Name:        raw.Name,
		Description: raw.Description,
		Values:      raw.EnumValues,
	}), nil
}

// isGeometryType checks if a GraphQL type string is a geometry type.
func isGeometryType(fieldType string) bool {
	t := strings.TrimSuffix(strings.TrimPrefix(fieldType, "["), "]")
	t = strings.TrimSuffix(t, "!")
	return t == "Geometry" || t == "GeoJSON" || t == "GeometryCollection"
}
