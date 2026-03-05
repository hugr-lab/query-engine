package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) inlineGraphQLResult(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")
	jqTransform := req.GetString("jq_transform", "")
	maxResultSize := req.GetInt("max_result_size", 1000)

	if query == "" {
		return toolResultError("query is required"), nil
	}
	if maxResultSize < 100 {
		maxResultSize = 100
	}
	if maxResultSize > 5000 {
		maxResultSize = 5000
	}

	vars := make(map[string]any)
	args := req.GetArguments()
	if v, ok := args["variables"]; ok {
		if m, ok := v.(map[string]any); ok {
			vars = m
		}
	}

	// Compile JQ transformer before executing query (fail fast on bad expression).
	var transformer *jq.Transformer
	if jqTransform != "" {
		var err error
		transformer, err = jq.NewTransformer(ctx, jqTransform, jq.WithVariables(vars), jq.WithQuerier(s.querier))
		if err != nil {
			return toolResultError(fmt.Sprintf("jq compile: %v", err)), nil
		}
	}

	res, err := s.querier.Query(ctx, query, vars)
	if err != nil {
		return toolResultError(fmt.Sprintf("query failed: %v", err)), nil
	}
	defer res.Close()
	if res.Err() != nil {
		return toolResultError(fmt.Sprintf("query error: %v", res.Err())), nil
	}

	var data any = res.Data
	if transformer != nil {
		data, err = transformer.Transform(ctx, res, nil)
		if err != nil {
			return toolResultError(fmt.Sprintf("jq transform: %v", err)), nil
		}
	}

	b, err := json.Marshal(data)
	if err != nil {
		return toolResultError(fmt.Sprintf("marshal result: %v", err)), nil
	}

	originalSize := len(b)
	isTruncated := originalSize > maxResultSize

	result := map[string]any{
		"is_truncated":  isTruncated,
		"original_size": originalSize,
	}
	if isTruncated {
		result["data"] = string(b[:maxResultSize]) + "..."
	} else {
		result["data"] = json.RawMessage(b)
	}

	return toolResultJSON(result), nil
}

func (s *Server) validateGraphQLQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")

	if query == "" {
		return toolResultError("query is required"), nil
	}

	vars := make(map[string]any)
	args := req.GetArguments()
	if v, ok := args["variables"]; ok {
		if m, ok := v.(map[string]any); ok {
			vars = m
		}
	}

	ctx = types.ContextWithValidateOnly(ctx)
	res, err := s.querier.Query(ctx, query, vars)
	if err != nil {
		return toolResultJSON(map[string]any{
			"ok":    false,
			"error": err.Error(),
		}), nil
	}
	defer res.Close()
	if res.Err() != nil {
		return toolResultJSON(map[string]any{
			"ok":    false,
			"error": res.Err().Error(),
		}), nil
	}

	return toolResultJSON(map[string]any{"ok": true}), nil
}
