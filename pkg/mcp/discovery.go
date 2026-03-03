package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

type searchItem struct {
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	HugrType    string  `json:"hugr_type,omitempty"`
	Module      string  `json:"module,omitempty"`
	Catalog     string  `json:"catalog,omitempty"`
	Score       float64 `json:"score,omitempty"`
	Distance    float64 `json:"_distance_to_query,omitempty"`
}

func (s *Server) searchModules(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 5)
	minScore := req.GetFloat("min_score", 0.3)
	if query == "" {
		return toolResultError("query is required"), nil
	}

	var items []searchItem
	err := s.queryScan(ctx, fmt.Sprintf(`query {
		core {
			catalog {
				modules(
					order_by: [{field: "_distance_to_query", direction: ASC, args: {query: %q}}]
					limit: %d
				) {
					name
					description
					_distance_to_query(query: %q)
				}
			}
		}
	}`, query, topK, query), nil, "core.catalog.modules", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	return toolResultJSON(filterByScore(items, minScore)), nil
}

func (s *Server) searchDataSources(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 5)
	minScore := req.GetFloat("min_score", 0.3)
	if query == "" {
		return toolResultError("query is required"), nil
	}

	var items []searchItem
	err := s.queryScan(ctx, fmt.Sprintf(`query {
		core {
			catalog {
				catalogs(
					order_by: [{field: "_distance_to_query", direction: ASC, args: {query: %q}}]
					limit: %d
				) {
					name
					description
					_distance_to_query(query: %q)
				}
			}
		}
	}`, query, topK, query), nil, "core.catalog.catalogs", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	return toolResultJSON(filterByScore(items, minScore)), nil
}

func (s *Server) searchModuleDataObjects(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	module := req.GetString("module", "")
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 10)
	if module == "" || query == "" {
		return toolResultError("module and query are required"), nil
	}

	moduleFilter := fmt.Sprintf(`{eq: %q}`, module)
	if req.GetBool("include_sub_modules", true) {
		moduleFilter = fmt.Sprintf(`{like: %q}`, module+"%%")
	}

	var items []searchItem
	err := s.queryScan(ctx, fmt.Sprintf(`query {
		core {
			catalog {
				types(
					filter: {
						hugr_type: {in: ["table", "view"]}
						module: %s
					}
					order_by: [{field: "_distance_to_query", direction: ASC, args: {query: %q}}]
					limit: %d
				) {
					name
					hugr_type
					description
					module
					catalog
					_distance_to_query(query: %q)
				}
			}
		}
	}`, moduleFilter, query, topK, query), nil, "core.catalog.types", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	return toolResultJSON(filterByScore(items, 0)), nil
}

func (s *Server) searchModuleFunctions(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	module := req.GetString("module", "")
	query := req.GetString("query", "")
	topK := req.GetInt("top_k", 10)
	includeMutations := req.GetBool("include_mutations", false)
	if module == "" || query == "" {
		return toolResultError("module and query are required"), nil
	}

	typeTypes := `["function"]`
	if includeMutations {
		typeTypes = `["function", "mutation_function"]`
	}

	var items []struct {
		FieldName string `json:"field_name"`
		FieldDesc string `json:"field_description"`
		HugrType  string `json:"hugr_type"`
		Catalog   string `json:"catalog"`
		TypeType  string `json:"type_type"`
	}
	err := s.queryScan(ctx, fmt.Sprintf(`query {
		core {
			catalog {
				module_intro(
					filter: {
						module: {eq: %q}
						type_type: {in: %s}
					}
					limit: %d
				) {
					field_name
					field_description
					hugr_type
					catalog
					type_type
				}
			}
		}
	}`, module, typeTypes, topK), nil, "core.catalog.module_intro", &items)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}

	return toolResultJSON(map[string]any{
		"total":    len(items),
		"returned": len(items),
		"items":    items,
	}), nil
}

// filterByScore computes score from distance and filters by minScore.
func filterByScore(items []searchItem, minScore float64) map[string]any {
	var filtered []searchItem
	for _, item := range items {
		if item.Distance > 0 {
			item.Score = 1 - item.Distance
		}
		if minScore > 0 && item.Score < minScore {
			continue
		}
		filtered = append(filtered, item)
	}
	return map[string]any{
		"total":    len(items),
		"returned": len(filtered),
		"items":    filtered,
	}
}
