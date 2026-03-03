package mcp

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

//go:embed resources
var resourcesFS embed.FS

//go:embed prompts
var promptsFS embed.FS

// Server wraps the MCP server and its dependencies.
type Server struct {
	querier types.Querier
	mcp     *server.MCPServer
	http    *server.StreamableHTTPServer
}

// New creates a new MCP server backed by the given query engine.
func New(querier types.Querier) *Server {
	s := &Server{querier: querier}

	mcpServer := server.NewMCPServer(
		"Hugr Schema Explorer",
		"1.0.0",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true),
		server.WithPromptCapabilities(true),
	)

	// Discovery tools.
	mcpServer.AddTool(mcp.NewTool("discovery-search_modules",
		mcp.WithDescription("Search modules by natural language query using semantic embeddings"),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language search query")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(5)),
		mcp.WithNumber("min_score", mcp.Description("Minimum relevance score (0-1)"), mcp.DefaultNumber(0.3)),
	), s.searchModules)

	mcpServer.AddTool(mcp.NewTool("discovery-search_data_sources",
		mcp.WithDescription("Search data sources by natural language query using semantic embeddings"),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language search query")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(5)),
		mcp.WithNumber("min_score", mcp.Description("Minimum relevance score (0-1)"), mcp.DefaultNumber(0.3)),
	), s.searchDataSources)

	mcpServer.AddTool(mcp.NewTool("discovery-search_module_data_objects",
		mcp.WithDescription("Search tables/views within a module by natural language query"),
		mcp.WithString("module", mcp.Required(), mcp.Description("Module name")),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language search query")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(10)),
		mcp.WithBoolean("include_sub_modules", mcp.Description("Include sub-module data objects"), mcp.DefaultBool(true)),
		mcp.WithString("fields_query", mcp.Description("Additional fields relevance query")),
	), s.searchModuleDataObjects)

	mcpServer.AddTool(mcp.NewTool("discovery-search_module_functions",
		mcp.WithDescription("Search functions within a module by natural language query"),
		mcp.WithString("module", mcp.Required(), mcp.Description("Module name")),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language search query")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(10)),
		mcp.WithBoolean("include_mutations", mcp.Description("Include mutation functions"), mcp.DefaultBool(false)),
	), s.searchModuleFunctions)

	// Schema introspection tools.
	mcpServer.AddTool(mcp.NewTool("schema-type_info",
		mcp.WithDescription("Get type metadata: kind, field count, description"),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Full type name")),
		mcp.WithBoolean("with_description", mcp.Description("Include description"), mcp.DefaultBool(true)),
	), s.typeInfo)

	mcpServer.AddTool(mcp.NewTool("schema-type_fields",
		mcp.WithDescription("List type fields with optional relevance ranking"),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Full type name")),
		mcp.WithString("relevance_query", mcp.Description("Rank by relevance to query")),
		mcp.WithNumber("limit", mcp.Description("Page size (1-200)"), mcp.DefaultNumber(50)),
		mcp.WithNumber("offset", mcp.Description("Pagination offset"), mcp.DefaultNumber(0)),
	), s.typeFields)

	mcpServer.AddTool(mcp.NewTool("schema-enum_values",
		mcp.WithDescription("Get enum type values"),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Enum type name")),
	), s.enumValues)

	// Data tools.
	mcpServer.AddTool(mcp.NewTool("data-inline_graphql_result",
		mcp.WithDescription("Execute a GraphQL query and return JSON result (with optional jq transform)"),
		mcp.WithString("query", mcp.Required(), mcp.Description("GraphQL query")),
		mcp.WithObject("variables", mcp.Description("Query variables")),
		mcp.WithString("operation_name", mcp.Description("Operation name")),
		mcp.WithString("jq_transform", mcp.Description("JQ expression to apply")),
		mcp.WithNumber("max_result_size", mcp.Description("Max result bytes (100-5000)"), mcp.DefaultNumber(1000)),
	), s.inlineGraphQLResult)

	mcpServer.AddTool(mcp.NewTool("data-validate_graphql_query",
		mcp.WithDescription("Validate a GraphQL query without executing it"),
		mcp.WithString("query", mcp.Required(), mcp.Description("GraphQL query")),
		mcp.WithObject("variables", mcp.Description("Query variables")),
		mcp.WithString("operation_name", mcp.Description("Operation name")),
	), s.validateGraphQLQuery)

	// Resources.
	addResources(mcpServer)

	// Prompts.
	addPrompts(mcpServer)

	s.mcp = mcpServer
	s.http = server.NewStreamableHTTPServer(mcpServer,
		server.WithStateLess(true),
	)
	return s
}

// Handler returns an http.Handler for mounting at /mcp.
func (s *Server) Handler() http.Handler {
	return s.http
}

// --- Helper functions ---

// queryScan executes a GraphQL query and scans the result at the given path into target.
func (s *Server) queryScan(ctx context.Context, gql string, vars map[string]any, path string, target any) error {
	res, err := s.querier.Query(ctx, gql, vars)
	if err != nil {
		return err
	}
	defer res.Close()
	if res.Err() != nil {
		return res.Err()
	}
	return res.ScanData(path, target)
}

func toolResultJSON(v any) *mcp.CallToolResult {
	b, _ := json.MarshalIndent(v, "", "  ")
	return mcp.NewToolResultText(string(b))
}

func toolResultError(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{mcp.NewTextContent(msg)},
		IsError: true,
	}
}

// addResources registers embedded markdown resources.
func addResources(s *server.MCPServer) {
	resources := []struct {
		URI  string
		Name string
		File string
	}{
		{"hugr://ai-instructions", "AI Instructions", "resources/ai-instructions.md"},
		{"hugr://schema-structure", "Schema Structure", "resources/schema-structure.md"},
		{"hugr://data-types", "Data Types", "resources/data-types.md"},
		{"hugr://query-patterns", "Query Patterns", "resources/query-patterns.md"},
		{"hugr://filter-guide", "Filter Guide", "resources/filter-guide.md"},
		{"hugr://aggregations", "Aggregations", "resources/aggregations.md"},
		{"hugr://overview", "Overview", "resources/overview.md"},
	}

	for _, r := range resources {
		content, err := resourcesFS.ReadFile(r.File)
		if err != nil {
			// Resource file missing — skip silently.
			continue
		}
		s.AddResource(
			mcp.NewResource(r.URI, r.Name,
				mcp.WithResourceDescription(r.Name+" for Hugr query engine"),
				mcp.WithMIMEType("text/markdown"),
			),
			func(_ context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
				return []mcp.ResourceContents{
					mcp.TextResourceContents{
						URI:      req.Params.URI,
						MIMEType: "text/markdown",
						Text:     string(content),
					},
				}, nil
			},
		)
	}
}

// addPrompts registers embedded prompt templates.
func addPrompts(s *server.MCPServer) {
	s.AddPrompt(
		mcp.NewPrompt("start",
			mcp.WithPromptDescription("Initial system prompt for Hugr schema exploration"),
		),
		func(_ context.Context, _ mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			content, err := promptsFS.ReadFile("prompts/start.md")
			if err != nil {
				return nil, fmt.Errorf("read start prompt: %w", err)
			}
			return &mcp.GetPromptResult{
				Description: "Initial system prompt for Hugr schema exploration",
				Messages: []mcp.PromptMessage{
					{Role: mcp.RoleUser, Content: mcp.NewTextContent(string(content))},
				},
			}, nil
		},
	)

	s.AddPrompt(
		mcp.NewPrompt("discovery",
			mcp.WithPromptDescription("Discovery workflow guide"),
			mcp.WithArgument("module", mcp.ArgumentDescription("Module to explore")),
			mcp.WithArgument("query", mcp.ArgumentDescription("Search query")),
		),
		func(_ context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			content, err := promptsFS.ReadFile("prompts/discovery.md")
			if err != nil {
				return nil, fmt.Errorf("read discovery prompt: %w", err)
			}
			text := string(content)
			if args := req.Params.Arguments; len(args) > 0 {
				if m := args["module"]; m != "" {
					text += fmt.Sprintf("\n\nFocus on module: %s", m)
				}
				if q := args["query"]; q != "" {
					text += fmt.Sprintf("\n\nSearch for: %s", q)
				}
			}
			return &mcp.GetPromptResult{
				Description: "Discovery workflow guide",
				Messages: []mcp.PromptMessage{
					{Role: mcp.RoleUser, Content: mcp.NewTextContent(text)},
				},
			}, nil
		},
	)

	s.AddPrompt(
		mcp.NewPrompt("query-building",
			mcp.WithPromptDescription("Query construction guide"),
			mcp.WithArgument("type_name", mcp.ArgumentDescription("Type to query")),
			mcp.WithArgument("module", mcp.ArgumentDescription("Module context")),
		),
		func(_ context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			content, err := promptsFS.ReadFile("prompts/query-building.md")
			if err != nil {
				return nil, fmt.Errorf("read query-building prompt: %w", err)
			}
			text := string(content)
			if args := req.Params.Arguments; len(args) > 0 {
				if t := args["type_name"]; t != "" {
					text += fmt.Sprintf("\n\nTarget type: %s", t)
				}
				if m := args["module"]; m != "" {
					text += fmt.Sprintf("\n\nModule: %s", m)
				}
			}
			return &mcp.GetPromptResult{
				Description: "Query construction guide",
				Messages: []mcp.PromptMessage{
					{Role: mcp.RoleUser, Content: mcp.NewTextContent(text)},
				},
			}, nil
		},
	)

	s.AddPrompt(
		mcp.NewPrompt("analysis",
			mcp.WithPromptDescription("Data analysis patterns"),
			mcp.WithArgument("data_source", mcp.ArgumentDescription("Data source to analyze")),
		),
		func(_ context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			content, err := promptsFS.ReadFile("prompts/analysis.md")
			if err != nil {
				return nil, fmt.Errorf("read analysis prompt: %w", err)
			}
			text := string(content)
			if args := req.Params.Arguments; len(args) > 0 {
				if ds := args["data_source"]; ds != "" {
					text += fmt.Sprintf("\n\nData source: %s", ds)
				}
			}
			return &mcp.GetPromptResult{
				Description: "Data analysis patterns",
				Messages: []mcp.PromptMessage{
					{Role: mcp.RoleUser, Content: mcp.NewTextContent(text)},
				},
			}, nil
		},
	)
}
