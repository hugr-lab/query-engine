package mcp

// schema.go holds what is left of the legacy compiled-schema tools:
// schema-type_info, superseded by schema-describe_types and removed with the
// rest of the discovery-* family. Everything it reads lives in the
// compiled-schema views, which the entity storage does not expose.

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// isGeometryType checks if a GraphQL type string is a geometry type.
func isGeometryType(fieldType string) bool {
	t := strings.TrimSuffix(strings.TrimPrefix(fieldType, "["), "]")
	t = strings.TrimSuffix(t, "!")
	return t == "Geometry" || t == "GeoJSON" || t == "GeometryCollection"
}
