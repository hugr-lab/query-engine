//go:build duckdb_arrow

package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	mcpserver "github.com/hugr-lab/query-engine/pkg/mcp"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
)

// setupEngine bootstraps a full in-memory engine for MCP integration testing.
// MCPEnabled is false — we create the MCP server separately to avoid the embedder requirement.
func setupEngine(t *testing.T) *hugr.Service {
	t.Helper()
	ctx := context.Background()

	service, err := hugr.New(hugr.Config{
		DB: db.Config{
			Path: "",
		},
		CoreDB: coredb.New(coredb.Config{}),
		Auth:   &auth.Config{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { service.Close() })

	err = service.Init(ctx)
	require.NoError(t, err)

	return service
}

// setupMCP creates an MCP handler backed by the engine.
func setupMCP(t *testing.T, service *hugr.Service) http.Handler {
	t.Helper()
	srv := mcpserver.New(service, nil, true)
	return srv.Handler()
}

// jsonRPC sends a JSON-RPC 2.0 request to the MCP handler and returns the response body.
func jsonRPC(t *testing.T, handler http.Handler, method string, params any) map[string]any {
	t.Helper()

	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
	}
	if params != nil {
		body["params"] = params
	}

	b, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// MCP may respond with SSE or JSON. Parse the JSON-RPC response.
	// For StreamableHTTP, the response may be SSE with data: lines.
	content := string(respBody)

	// Try direct JSON first.
	var result map[string]any
	if err := json.Unmarshal([]byte(content), &result); err == nil {
		return result
	}

	// Parse SSE: look for "data: " lines with JSON.
	for _, line := range bytes.Split(respBody, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("data: ")) {
			jsonData := bytes.TrimPrefix(line, []byte("data: "))
			if err := json.Unmarshal(jsonData, &result); err == nil {
				return result
			}
		}
	}

	t.Fatalf("failed to parse MCP response: %s", content)
	return nil
}

// mcpInit sends the initialize handshake required before any tool calls.
func mcpInit(t *testing.T, handler http.Handler) {
	t.Helper()
	resp := jsonRPC(t, handler, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    "integration-test",
			"version": "1.0.0",
		},
	})
	// Should have a result (server capabilities).
	require.Contains(t, resp, "result", "initialize should return result: %v", resp)
}

// --- Tests ---

func TestMCP_Initialize(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)

	resp := jsonRPC(t, handler, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    "test",
			"version": "1.0",
		},
	})

	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	assert.Contains(t, result, "capabilities")
	assert.Contains(t, result, "serverInfo")

	serverInfo := result["serverInfo"].(map[string]any)
	assert.Equal(t, "Hugr Schema Explorer", serverInfo["name"])
}

func TestMCP_ToolsList(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "tools/list", nil)
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	tools := result["tools"].([]any)

	// Should have 10 tools (5 discovery + 3 schema + 2 data).
	assert.Len(t, tools, 10, "expected 10 MCP tools")

	// Verify tool names.
	toolNames := make(map[string]bool)
	for _, tool := range tools {
		tm := tool.(map[string]any)
		toolNames[tm["name"].(string)] = true
	}
	expectedTools := []string{
		"discovery-search_modules",
		"discovery-search_data_sources",
		"discovery-search_module_data_objects",
		"discovery-search_module_functions",
		"discovery-field_values",
		"schema-type_info",
		"schema-type_fields",
		"schema-enum_values",
		"data-inline_graphql_result",
		"data-validate_graphql_query",
	}
	for _, name := range expectedTools {
		assert.True(t, toolNames[name], "missing tool: %s", name)
	}
}

func TestMCP_ResourcesList(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "resources/list", nil)
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	resources := result["resources"].([]any)

	// Should have 4 resources (overview, query-patterns, filter-guide, aggregations).
	assert.Len(t, resources, 4, "expected 4 MCP resources")
}

func TestMCP_PromptsList(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "prompts/list", nil)
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	prompts := result["prompts"].([]any)

	// Should have 4 prompts (start, analyze, query, dashboard).
	assert.Len(t, prompts, 4, "expected 4 MCP prompts")
}

func TestMCP_ValidateGraphQLQuery(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	// Valid query.
	resp := jsonRPC(t, handler, "tools/call", map[string]any{
		"name": "data-validate_graphql_query",
		"arguments": map[string]any{
			"query": `{ core { data_sources { name } } }`,
		},
	})
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	require.NotEmpty(t, content)

	textContent := content[0].(map[string]any)["text"].(string)
	var validResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(textContent), &validResult))
	assert.Equal(t, true, validResult["ok"])
}

func TestMCP_InlineGraphQLResult(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "tools/call", map[string]any{
		"name": "data-inline_graphql_result",
		"arguments": map[string]any{
			"query": `{ core { data_sources { name } } }`,
		},
	})
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	require.NotEmpty(t, content)

	textContent := content[0].(map[string]any)["text"].(string)
	var queryResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(textContent), &queryResult))
	assert.Contains(t, queryResult, "data")
	assert.Contains(t, queryResult, "is_truncated")
	assert.Contains(t, queryResult, "original_size")
}

func TestMCP_SchemaTypeInfo(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	// Query a known type from the core catalog.
	resp := jsonRPC(t, handler, "tools/call", map[string]any{
		"name": "schema-type_info",
		"arguments": map[string]any{
			"type_name": "core_data_sources",
		},
	})
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	require.NotEmpty(t, content)

	textContent := content[0].(map[string]any)["text"].(string)
	t.Logf("type_info response: %.500s", textContent)
	var typeInfo map[string]any
	require.NoError(t, json.Unmarshal([]byte(textContent), &typeInfo))
	assert.Equal(t, "core_data_sources", typeInfo["name"])
	assert.Contains(t, typeInfo, "fields_total")
	assert.Contains(t, typeInfo, "has_geometry_field")
	assert.Contains(t, typeInfo, "has_field_with_arguments")
}

func TestMCP_SchemaTypeFields(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "tools/call", map[string]any{
		"name": "schema-type_fields",
		"arguments": map[string]any{
			"type_name": "core_data_sources",
		},
	})
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	require.NotEmpty(t, content)

	textContent := content[0].(map[string]any)["text"].(string)
	t.Logf("type_fields response text: %.500s", textContent)
	var fieldsResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(textContent), &fieldsResult))
	assert.Contains(t, fieldsResult, "total")
	assert.Contains(t, fieldsResult, "returned")
	assert.Contains(t, fieldsResult, "items")
	items := fieldsResult["items"].([]any)
	assert.Greater(t, len(items), 0, "core_data_sources should have fields")
	// Check that each field has required fields.
	first := items[0].(map[string]any)
	assert.Contains(t, first, "name")
	assert.Contains(t, first, "field_type")
	assert.Contains(t, first, "hugr_type")
	assert.Contains(t, first, "is_list")
	assert.Contains(t, first, "arguments_count")
}

func TestMCP_PromptGet(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "prompts/get", map[string]any{
		"name": "start",
	})
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	assert.Contains(t, result, "messages")
	messages := result["messages"].([]any)
	assert.NotEmpty(t, messages)
}

func TestMCP_ResourceRead(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "resources/read", map[string]any{
		"uri": "hugr://overview",
	})
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	contents := result["contents"].([]any)
	require.NotEmpty(t, contents)

	first := contents[0].(map[string]any)
	assert.Contains(t, first["text"].(string), "Hugr")
}

func TestMCP_ValidateInvalidQuery(t *testing.T) {
	service := setupEngine(t)
	handler := setupMCP(t, service)
	mcpInit(t, handler)

	resp := jsonRPC(t, handler, "tools/call", map[string]any{
		"name": "data-validate_graphql_query",
		"arguments": map[string]any{
			"query": `{ nonexistent_field { name } }`,
		},
	})
	require.Contains(t, resp, "result")
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	require.NotEmpty(t, content)

	textContent := content[0].(map[string]any)["text"].(string)
	var validResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(textContent), &validResult))
	assert.Equal(t, false, validResult["ok"])
	assert.Contains(t, validResult, "error")
}
