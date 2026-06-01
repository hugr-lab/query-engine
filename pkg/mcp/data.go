package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/hugr-lab/query-engine/types"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) inlineGraphQLResult(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Reject mutation operations — this tool is read-only.
	ctx = types.ContextWithQueryHint(ctx, types.NoMutationHint())
	return s.runGraphQLResult(ctx, req)
}

// executeMutation runs a GraphQL mutation (insert/update/delete or mutation
// function). Unlike inlineGraphQLResult it does NOT set NoMutationHint, so
// mutation operations are permitted. The caller's permissions still apply —
// operations the user is not allowed to perform are rejected by the engine.
func (s *Server) executeMutation(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.runGraphQLResult(ctx, req)
}

// runGraphQLResult executes a GraphQL operation from the tool request and
// returns a truncatable JSON result, optionally shaped by a jq transform.
// The caller sets any query hints (e.g. NoMutationHint) on ctx beforehand.
func (s *Server) runGraphQLResult(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")
	jqTransform := req.GetString("jq_transform", "")
	maxResultSize := req.GetInt("max_result_size", 2000)

	if query == "" {
		return toolResultError("query is required"), nil
	}

	if maxResultSize < 100 {
		maxResultSize = 100
	}
	if maxResultSize > 10000 {
		maxResultSize = 10000
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
		result["data"] = fmt.Sprintf("[truncated: result is %d bytes, max is %d. Increase max_result_size or use jq_transform to reduce output]", originalSize, maxResultSize)
		// Raw byte-window preview; may split a UTF-8 rune at the boundary.
		result["preview"] = string(b[:min(originalSize, 2000)])
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

	ctx = types.ContextWithQueryHint(ctx, types.ValidateOnlyHint())
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
