package mcp

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

//go:embed resources
var resourcesFS embed.FS

//go:embed prompts
var promptsFS embed.FS

//go:embed instructions.md
var instructions string

// Server wraps the MCP server and its dependencies.
type Server struct {
	querier types.Querier
	debug   bool
	mcp     *server.MCPServer
	http    *server.StreamableHTTPServer
}

// New creates a new MCP server backed by the given query engine.
func New(querier types.Querier, debug bool) *Server {
	s := &Server{querier: querier, debug: debug}

	mcpServer := server.NewMCPServer(
		"Hugr Schema Explorer",
		"1.0.0",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true),
		server.WithPromptCapabilities(true),
		server.WithInstructions(instructions),
		server.WithToolHandlerMiddleware(toolLoggingMiddleware(debug)),
	)

	// Discovery tools.
	mcpServer.AddTool(mcp.NewTool("discovery-search_modules",
		mcp.WithDescription(`Search modules by natural language. Returns top-K modules ranked by semantic relevance.
Use as FIRST STEP to find which module contains the data you need.
Each module is a namespace: query { module { submodule { ... } } }.`),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language search query describing what data you need")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(5)),
		mcp.WithNumber("min_score", mcp.Description("Minimum relevance score (0-1)"), mcp.DefaultNumber(0.3)),
		mcp.WithOutputSchema[SearchResult[ModuleSearchItem]](),
	), s.searchModules)

	mcpServer.AddTool(mcp.NewTool("discovery-search_data_sources",
		mcp.WithDescription(`Search data sources by natural language. Returns sources with type (duckdb/postgres/http) and read-only info.`),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language search query describing what data you need")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(5)),
		mcp.WithNumber("min_score", mcp.Description("Minimum relevance score (0-1)"), mcp.DefaultNumber(0.3)),
		mcp.WithOutputSchema[SearchResult[DataSourceSearchItem]](),
	), s.searchDataSources)

	mcpServer.AddTool(mcp.NewTool("discovery-search_module_data_objects",
		mcp.WithDescription(`Search tables/views in a module. Returns type name (for schema-type_fields) and query field names (for GraphQL queries).
Each data object has 4 query fields: <name>, <name>_by_pk, <name>_aggregation, <name>_bucket_aggregation.
IMPORTANT: use query field names from 'queries' array to build GraphQL, not the type name.
NOTE: aggregation and bucket_aggregation are data object queries, NOT functions — do not use discovery-search_module_functions for them.`),
		mcp.WithString("module", mcp.Required(), mcp.Description("Module name to search within")),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language query describing what tables/views you need")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(5)),
		mcp.WithNumber("min_score", mcp.Description("Minimum relevance score (0-1)"), mcp.DefaultNumber(0.3)),
		mcp.WithBoolean("include_sub_modules", mcp.Description("Include sub-module data objects"), mcp.DefaultBool(true)),
		mcp.WithOutputSchema[SearchResult[DataObjectSearchItem]](),
	), s.searchModuleDataObjects)

	mcpServer.AddTool(mcp.NewTool("discovery-search_module_functions",
		mcp.WithDescription(`Search custom functions in a module. Functions are separate from data objects — they are custom computed endpoints.
Functions are called via: query { function { module { func_name(args) { fields } } }
NOTE: do NOT search here for aggregation/bucket_aggregation — those are data object queries found via discovery-search_module_data_objects.`),
		mcp.WithString("module", mcp.Required(), mcp.Description("Module name to search within")),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language query describing what functions you need")),
		mcp.WithNumber("top_k", mcp.Description("Number of results (1-50)"), mcp.DefaultNumber(10)),
		mcp.WithBoolean("include_mutations", mcp.Description("Include mutation functions"), mcp.DefaultBool(false)),
		mcp.WithBoolean("include_sub_modules", mcp.Description("Include sub-module functions"), mcp.DefaultBool(true)),
		mcp.WithOutputSchema[SearchResult[FunctionSearchItem]](),
	), s.searchModuleFunctions)

	mcpServer.AddTool(mcp.NewTool("discovery-field_values",
		mcp.WithDescription(`Return top distinct values and optional stats for a scalar field. Use to understand data distribution before building filters.
Stats (min/max/avg) are only available for numeric and timestamp fields, not for strings.
ALWAYS check field values before building filters with specific values (categories, statuses, etc.).`),
		mcp.WithString("object_name", mcp.Required(), mcp.Description("Data object type name (e.g. prefix_tablename)")),
		mcp.WithString("field_name", mcp.Required(), mcp.Description("Field name to analyze")),
		mcp.WithNumber("limit", mcp.Description("Number of top values (1-100)"), mcp.DefaultNumber(10)),
		mcp.WithBoolean("calculate_stats", mcp.Description("Include min/max/avg/distinct_count (only for numeric/timestamp fields)"), mcp.DefaultBool(false)),
		mcp.WithObject("filter", mcp.Description("Optional filter object to narrow data before aggregation")),
		mcp.WithOutputSchema[FieldValuesResult](),
	), s.fieldValues)

	// Schema introspection tools.
	mcpServer.AddTool(mcp.NewTool("schema-type_info",
		mcp.WithDescription(`Return high-level metadata for a type: kind, module, catalog, field count, geometry/argument presence.
Use type_name (e.g. "prefix_tablename"), NOT the module name.`),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Full type name (e.g. prefix_tablename, NOT the module name)")),
		mcp.WithBoolean("with_description", mcp.Description("Include short description"), mcp.DefaultBool(true)),
		mcp.WithBoolean("with_long_description", mcp.Description("Include long description (verbose, uses more context)"), mcp.DefaultBool(false)),
		mcp.WithOutputSchema[TypeInfo](),
	), s.typeInfo)

	mcpServer.AddTool(mcp.NewTool("schema-type_fields",
		mcp.WithDescription(`Return fields of a type. MUST call before building any query — field names cannot be guessed.
Use type_name (e.g. "synthea_patients"), NOT the module name (e.g. NOT "synthea").
Returns: hugr_type (empty=scalar, select=relation, aggregate, bucket_agg, extra_field, function), arguments_count, is_list.
Use include_arguments=true to see field arguments (filter types, bucket args, etc.).
Rely on field descriptions to understand semantics — names are often auto-generated.`),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Full type name (e.g. prefix_tablename, NOT the module name)")),
		mcp.WithString("relevance_query", mcp.Description("Rank fields by semantic relevance to this query")),
		mcp.WithNumber("limit", mcp.Description("Max fields to return (1-200)"), mcp.DefaultNumber(50)),
		mcp.WithNumber("offset", mcp.Description("Pagination offset"), mcp.DefaultNumber(0)),
		mcp.WithBoolean("include_description", mcp.Description("Include field descriptions (default false to save context)"), mcp.DefaultBool(false)),
		mcp.WithBoolean("include_arguments", mcp.Description("Include full argument details for fields with arguments"), mcp.DefaultBool(false)),
		mcp.WithOutputSchema[SearchResult[TypeFieldInfo]](),
	), s.typeFields)

	mcpServer.AddTool(mcp.NewTool("schema-enum_values",
		mcp.WithDescription(`Return enum values for a GraphQL enum type. Use to discover valid enum values before building queries.
Common enums: OrderDirection (ASC, DESC), TimeExtract (year, month, day, hour, ...), TimeBucket (minute, hour, day, week, month, quarter, year).`),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Enum type name")),
		mcp.WithOutputSchema[EnumValuesResult](),
	), s.enumValues)

	// Data tools.
	mcpServer.AddTool(mcp.NewTool("data-inline_graphql_result",
		mcp.WithDescription(`Execute a GraphQL query and return JSON result with optional jq transform.
If result is truncated (is_truncated=true), increase max_result_size (up to 5000) or use jq_transform to reduce output.
Query rules:
- Modules are nested fields: query { module { submodule { table(limit:10) { fields } } } }
- order_by: [{field: "name", direction: ASC}] — direction is UPPERCASE (ASC/DESC), fields MUST be in selection set
- Bucket aggregation order_by uses dot-paths: "key.fieldname", "aggregations._rows_count", "aggregations.amount.sum" — NEVER bare field names, NEVER field(args) in path
- Do NOT use distinct_on with bucket_aggregation — grouping is defined by key { ... } fields
- Relation filters: one-to-one direct nesting, one-to-many use any_of/all_of/none_of
- Aggregation functions by type: numeric (sum/avg/min/max/count), string (count/any/first/last/list — NO min/max/avg), timestamp (min/max/count)`),
		mcp.WithString("query", mcp.Required(), mcp.Description("GraphQL query")),
		mcp.WithObject("variables", mcp.Description("Query variables")),
		mcp.WithString("jq_transform", mcp.Description("JQ expression to apply to result")),
		mcp.WithNumber("max_result_size", mcp.Description("Max result bytes (100-5000). Increase if result is truncated."), mcp.DefaultNumber(1000)),
	), s.inlineGraphQLResult)

	mcpServer.AddTool(mcp.NewTool("data-validate_graphql_query",
		mcp.WithDescription(`Validate a GraphQL query without executing. Use before execution to catch errors early.
Validates field names, argument types, filter structure, and order_by paths.`),
		mcp.WithString("query", mcp.Required(), mcp.Description("GraphQL query")),
		mcp.WithObject("variables", mcp.Description("Query variables")),
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

// --- Middleware ---

func toolLoggingMiddleware(debug bool) server.ToolHandlerMiddleware {
	return func(next server.ToolHandlerFunc) server.ToolHandlerFunc {
		return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if !debug {
				return next(ctx, req)
			}

			tool := req.Params.Name
			args := req.GetArguments()

			argsJSON, _ := json.Marshal(args)
			slog.Info("MCP tool call",
				"tool", tool,
				"args", string(argsJSON),
			)

			start := time.Now()
			result, err := next(ctx, req)
			dur := time.Since(start)

			if err != nil {
				slog.Error("MCP tool error",
					"tool", tool,
					"duration", dur,
					"error", err,
				)
				return result, err
			}

			if result != nil && result.IsError {
				// Extract error text from content.
				var errText string
				for _, c := range result.Content {
					if tc, ok := c.(mcp.TextContent); ok {
						errText = tc.Text
						break
					}
				}
				slog.Warn("MCP tool returned error",
					"tool", tool,
					"duration", dur,
					"error", errText,
				)
				return result, nil
			}

			// Log success with truncated response preview.
			var preview string
			if result != nil {
				for _, c := range result.Content {
					if tc, ok := c.(mcp.TextContent); ok {
						preview = tc.Text
						break
					}
				}
			}
			responseLen := len(preview)
			if len(preview) > 500 {
				preview = preview[:500] + "..."
			}

			slog.Info("MCP tool result",
				"tool", tool,
				"duration", dur,
				"response_len", responseLen,
				"preview", preview,
			)

			return result, nil
		}
	}
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
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		// Fallback to text-only if structured marshaling would fail (e.g. truncated json.RawMessage).
		return mcp.NewToolResultText(fmt.Sprintf("%v", v))
	}
	return mcp.NewToolResultStructured(v, string(b))
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
		{"hugr://overview", "Overview", "resources/overview.md"},
		{"hugr://query-patterns", "Query Patterns", "resources/query-patterns.md"},
		{"hugr://filter-guide", "Filter Guide", "resources/filter-guide.md"},
		{"hugr://aggregations", "Aggregations", "resources/aggregations.md"},
	}

	for _, r := range resources {
		content, err := resourcesFS.ReadFile(r.File)
		if err != nil {
			continue
		}
		s.AddResource(
			mcp.NewResource(r.URI, r.Name,
				mcp.WithResourceDescription(r.Name+" reference for Hugr query engine"),
				mcp.WithMIMEType("text/markdown"),
			),
			func(_ context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
				slog.Info("MCP resource read", "uri", req.Params.URI)
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
	prompts := []struct {
		Name string
		Desc string
		File string
	}{
		{
			"start",
			"Use at the beginning of a conversation to set up Hugr schema exploration context. " +
				"Loads tool workflow, query syntax rules, and key conventions. " +
				"Choose this when the user wants to explore data or ask questions about available datasets.",
			"prompts/start.md",
		},
		{
			"analyze",
			"Use when the user asks to analyze data, find patterns, compute statistics, or answer analytical questions. " +
				"Guides through data exploration, aggregation queries, and presents findings with tables and insights. " +
				"Examples: 'analyze patient demographics', 'what are the trends in sales', 'compare categories'.",
			"prompts/analyze.md",
		},
		{
			"query",
			"Use when the user needs help building a specific Hugr GraphQL query. " +
				"Guides through schema discovery, field inspection, and step-by-step query construction with validation. " +
				"Examples: 'build a query for top orders', 'how to filter by date range', 'join two tables'.",
			"prompts/query.md",
		},
		{
			"dashboard",
			"Use when the user asks to create a dashboard, overview, or visual report of a dataset. " +
				"Generates a React component with KPIs, breakdowns, time trends, and rankings in Hugr brand style. " +
				"Examples: 'create a dashboard for patient data', 'build an overview of sales', 'visualize key metrics'.",
			"prompts/dashboard.md",
		},
	}

	for _, p := range prompts {
		s.AddPrompt(
			mcp.NewPrompt(p.Name,
				mcp.WithPromptDescription(p.Desc),
			),
			func(_ context.Context, _ mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
				slog.Info("MCP prompt get", "name", p.Name)
				content, err := promptsFS.ReadFile(p.File)
				if err != nil {
					return nil, fmt.Errorf("read %s prompt: %w", p.Name, err)
				}
				return &mcp.GetPromptResult{
					Description: p.Desc,
					Messages: []mcp.PromptMessage{
						{Role: mcp.RoleUser, Content: mcp.NewTextContent(string(content))},
					},
				}, nil
			},
		)
	}
}
