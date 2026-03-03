package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) typeInfo(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	withDesc := req.GetBool("with_description", true)

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	_ = withDesc // all fields always returned; description is always present in DB

	var result struct {
		Name     string `json:"name"`
		Kind     string `json:"kind"`
		HugrType string `json:"hugr_type"`
		Module   string `json:"module"`
		Catalog  string `json:"catalog"`
		Desc     string `json:"description"`
		LongDesc string `json:"long_description"`
	}

	err := s.queryScan(ctx, `query($name: String!) {
		core {
			catalog {
				_schema_types_by_pk(name: $name) {
					name
					kind
					hugr_type
					module
					catalog
					description
					long_description
				}
			}
		}
	}`, map[string]any{"name": typeName}, "core.catalog._schema_types_by_pk", &result)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	if result.Name == "" {
		return toolResultError(fmt.Sprintf("type %q not found", typeName)), nil
	}

	return toolResultJSON(result), nil
}

func (s *Server) typeFields(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	limit := req.GetInt("limit", 50)
	offset := req.GetInt("offset", 0)

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	var fields []struct {
		Name          string `json:"name"`
		FieldType     string `json:"field_type"`
		FieldTypeName string `json:"field_type_name"`
		Description   string `json:"description"`
		HugrType      string `json:"hugr_type"`
		Catalog       string `json:"catalog"`
	}

	err := s.queryScan(ctx, `query($filter: _schema_fields_filter, $limit: Int, $offset: Int) {
		core {
			catalog {
				_schema_fields(
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
					catalog
				}
			}
		}
	}`, map[string]any{
		"filter": map[string]any{
			"type_name": map[string]any{"eq": typeName},
		},
		"limit":  limit,
		"offset": offset,
	}, "core.catalog._schema_fields", &fields)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	if fields == nil {
		fields = make([]struct {
			Name          string `json:"name"`
			FieldType     string `json:"field_type"`
			FieldTypeName string `json:"field_type_name"`
			Description   string `json:"description"`
			HugrType      string `json:"hugr_type"`
			Catalog       string `json:"catalog"`
		}, 0)
	}

	return toolResultJSON(fields), nil
}

func (s *Server) enumValues(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")

	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}

	var result struct {
		Name   string `json:"name"`
		Kind   string `json:"kind"`
		Fields []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"fields"`
	}

	err := s.queryScan(ctx, `query($name: String!) {
		core {
			catalog {
				_schema_types_by_pk(name: $name) {
					name
					kind
					fields {
						name
						description
					}
				}
			}
		}
	}`, map[string]any{"name": typeName}, "core.catalog._schema_types_by_pk", &result)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	if result.Name == "" {
		return toolResultError(fmt.Sprintf("type %q not found", typeName)), nil
	}
	if result.Kind != "ENUM" {
		return toolResultError(fmt.Sprintf("type %q is %s, not ENUM", typeName, result.Kind)), nil
	}

	return toolResultJSON(result.Fields), nil
}
