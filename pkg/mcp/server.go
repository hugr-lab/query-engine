package mcp

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/types"
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

// New creates a new MCP server backed by the given query engine. Everything —
// including the permission filter on the search path — goes through the
// querier, so MCP holds no copy of the engine's access policy.
func New(querier types.Querier, mcpServer *server.MCPServer, debug bool) *Server {
	if mcpServer == nil {
		mcpServer = server.NewMCPServer(
			"Hugr Schema Explorer",
			"1.0.0",
			server.WithToolCapabilities(true),
			server.WithResourceCapabilities(false, true),
			server.WithPromptCapabilities(true),
			server.WithInstructions(instructions),
			server.WithToolHandlerMiddleware(toolLoggingMiddleware(debug)),
		)
	}
	s := &Server{querier: querier, debug: debug}

	// Catalog tools — the logical model (design-035).
	mcpServer.AddTool(mcp.NewTool("catalog-list",
		mcp.WithDescription(`Enumerate what EXISTS in the logical model — the map to start from when you do not yet know the vocabulary of this deployment.
Pick a kind:
- module — the namespaces, as a FLAT list of dotted paths ("sales", "sales.reports"); dots are GraphQL nesting: query { sales { reports { ... } } }. "" is the root module. Each entry carries how many data objects / functions / submodules it holds.
- data_source — the attached sources, with the modules each contributes to. read_only=null means the storage does not record it, NOT that mutations are allowed.
- data_object — the tables and views. 'name' is the GraphQL TYPE name; 'module' is where to nest the query. Use catalog-describe for the query field names to actually write.
- function — the callables. type is FUNCTION (query), MUTATION or SUBSCRIPTION.
Scope with 'module' (walks that subtree, including submodules) and 'prefix' (case-insensitive match on the name). Everything is paginated and permission-filtered: 'total' counts only what you may see.
Use catalog-search instead when you know WHAT you want but not what it is called.`),
		mcp.WithString("kind", mcp.Required(), mcp.Description("module | data_source | data_object | function"),
			mcp.Enum(catalogKinds...)),
		mcp.WithString("module", mcp.Description(`Restrict to this module's subtree; "" (default) walks everything. Not accepted for kind=data_source — a source contributes to several modules rather than belonging to one`)),
		mcp.WithString("prefix", mcp.Description("Case-insensitive name prefix filter")),
		mcp.WithNumber("limit", mcp.Description("Page size (1-200)"), mcp.DefaultNumber(defaultPageLimit)),
		mcp.WithNumber("offset", mcp.Description("Items to skip"), mcp.DefaultNumber(0)),
		mcp.WithOutputSchema[Page[CatalogItem]](),
	), s.catalogList)

	mcpServer.AddTool(mcp.NewTool("catalog-describe",
		mcp.WithDescription(`Describe EXACT names you already have (from catalog-list, catalog-search, or an error message) — this is where you learn how to CALL them. Batched: pass every name you care about in one call.
- data_object — returns queries[], the query field names to WRITE, with their arguments: <object> (the row set), <object>_by_pk and one more per unique key, <object>_aggregation, <object>_bucket_aggregation. Also the primary key, properties, parameterized-view args, relations (how to traverse to other objects) and fields_count. The FIELDS are deliberately not here: call catalog-object_fields for them.
- function — arguments, return type, and is_table (true means it returns a row set you select fields from).
- module — description and how much it holds.
- data_source — engine, flags and the modules it contributes to.
Nest the call in the module path from 'module': query { <module> { <query_name>(...) { ... } } }.
Relations page: relations_total is the full count, and relations_has_more means there are more — call again with a larger relations_offset. The same edges also appear as navigation FIELDS, so catalog-object_fields and schema-type_fields on the object type are two other ways to walk them.
Names that do not exist, and names the caller may not see, both come back in not_found — the tool cannot tell you which.`),
		mcp.WithString("kind", mcp.Required(), mcp.Description("module | data_source | data_object | function"),
			mcp.Enum(catalogKinds...)),
		mcp.WithArray("names", mcp.Required(), mcp.Description("Exact names, copied verbatim from a previous result"),
			mcp.Items(map[string]any{"type": "string"})),
		mcp.WithString("module", mcp.Description("Owning module — REQUIRED for kind=function, since a function's identity is (module, name)")),
		mcp.WithNumber("relations_limit", mcp.Description("Relations per described object (1-200)"), mcp.DefaultNumber(defaultRelationsLimit)),
		mcp.WithNumber("relations_offset", mcp.Description("Relations to skip, per described object"), mcp.DefaultNumber(0)),
		mcp.WithOutputSchema[DescribeResult](),
	), s.catalogDescribe)

	mcpServer.AddTool(mcp.NewTool("catalog-search",
		mcp.WithDescription(`Find things by MEANING when you know what you want but not what this deployment calls it. Describe the data in your own words ("customer orders with payment status", "where is the diagnosis code").
Searches modules, data sources, data objects, functions and FIELDS at once; narrow with 'kinds'. Every hit carries the module to nest it in and a next_call naming the exact tool to run next.
Field hits are the reason to search rather than list: a data object's fields are not all columns. field_kind says which — 'column' is a stored value, 'extra' is computed, and 'relation' is a PATH: ref_object names the object it navigates to, so selecting that field is how you get there. Narrow with 'field_kinds' if navigation hits are noise.
'lexical': true means this deployment has no vector index, so ranking fell back to substring matching — prefer exact terms and expect cruder results.
Results are filtered to what YOU may see; filtered_out counts what was dropped, which distinguishes "nothing matches" from "nothing you may see matches".
Use catalog-list when you want the complete map instead of the relevant few.`),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural-language description of the data you are looking for")),
		mcp.WithArray("kinds", mcp.Description("Restrict to these kinds (default: all)"),
			mcp.Items(map[string]any{"type": "string", "enum": searchKinds})),
		mcp.WithArray("field_kinds", mcp.Description("For field hits: column | relation | extra (default: all)"),
			mcp.Items(map[string]any{"type": "string", "enum": fieldKinds})),
		mcp.WithString("module", mcp.Description("Restrict to this module's subtree")),
		mcp.WithNumber("limit", mcp.Description("Page size (1-200)"), mcp.DefaultNumber(defaultPageLimit)),
		mcp.WithNumber("offset", mcp.Description("Hits to skip"), mcp.DefaultNumber(0)),
		mcp.WithNumber("min_score", mcp.Description("Drop hits below this score (0-1)"), mcp.DefaultNumber(0)),
		mcp.WithOutputSchema[SearchResultPage](),
	), s.catalogSearch)

	mcpServer.AddTool(mcp.NewTool("schema-describe_types",
		mcp.WithDescription(`Identify bare TYPE names — the ones that turn up in an error message, in a field's type, or in an argument like shop_items_filter — when you do not yet know what they are.
Answers what the name IS: kind (OBJECT / INPUT_OBJECT / ENUM / SCALAR / …), description and member COUNTS. Not the members: call schema-type_fields or schema-enum_values for those.
It is also the router between the two tool families:
- the name is a data object → logical_kind=data_object, and next_call points at catalog-describe.
- the name was GENERATED from one (a filter, an aggregation, an insert/update input) → derived_from names the base object and 'role' says what the type is for, so you learn which object you are actually filtering.
- the name is a module root type (_module_<mod>_query and friends) → logical_kind=module_root.
Batched: pass every unknown name at once. Names that do not exist, and names you may not see, both come back in not_found.`),
		mcp.WithArray("names", mcp.Required(), mcp.Description("Type names, copied verbatim"),
			mcp.Items(map[string]any{"type": "string"})),
		mcp.WithOutputSchema[TypeDescriptions](),
	), s.schemaDescribeTypes)

	mcpServer.AddTool(mcp.NewTool("catalog-object_fields",
		mcp.WithDescription(`List the fields of a DATA OBJECT — what you can actually select. Call this after catalog-describe told you the query name; describe gives the call signature, this gives the columns.
field_kind says what each field IS, and they are not all columns: 'column' is a stored value, 'extra' is computed, and 'relation' is a PATH — ref_object names the object it leads to, so selecting that field is how you traverse. is_pk marks the primary key.
relevance_query ranks the fields by MEANING instead of schema order — use it on a wide table when you know what you want ("diagnosis code", "when it was paid") but not what it is called.
args_count flags fields that take arguments; call schema-field_args for the ones you will parameterize.
Fields marked @exclude_mcp by the operator are never listed.`),
		mcp.WithString("object", mcp.Required(), mcp.Description("The data object's GraphQL type name")),
		mcp.WithString("relevance_query", mcp.Description("Rank fields by relevance to this description instead of schema order")),
		mcp.WithString("prefix", mcp.Description("Case-insensitive name prefix filter")),
		mcp.WithBoolean("include_description", mcp.Description("Include field descriptions"), mcp.DefaultBool(true)),
		mcp.WithNumber("limit", mcp.Description("Page size (1-200)"), mcp.DefaultNumber(defaultPageLimit)),
		mcp.WithNumber("offset", mcp.Description("Fields to skip"), mcp.DefaultNumber(0)),
		mcp.WithOutputSchema[Page[TypeField]](),
	), s.catalogObjectFields)

	mcpServer.AddTool(mcp.NewTool("schema-type_fields",
		mcp.WithDescription(`List the members of ANY generated type — a filter input, an aggregation, a mutation input, a module root. This is the tool for a name you got from an error or from a field's type; for a DATA OBJECT use catalog-object_fields, which also ranks by meaning.
Returns fields for an OBJECT and input fields for an INPUT_OBJECT — the same question either way. No argument trees: args_count flags which fields take arguments, and schema-field_args returns them for the few you name.
Paginated and deterministic. Use schema-describe_types first if you do not yet know what the name IS.`),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Full type name (not a module name)")),
		mcp.WithString("prefix", mcp.Description("Case-insensitive name prefix filter")),
		mcp.WithBoolean("include_description", mcp.Description("Include descriptions"), mcp.DefaultBool(true)),
		mcp.WithNumber("limit", mcp.Description("Page size (1-200)"), mcp.DefaultNumber(defaultPageLimit)),
		mcp.WithNumber("offset", mcp.Description("Members to skip"), mcp.DefaultNumber(0)),
		mcp.WithOutputSchema[Page[TypeField]](),
	), s.schemaTypeFields)

	mcpServer.AddTool(mcp.NewTool("schema-field_args",
		mcp.WithDescription(`Return the ARGUMENT trees of the few named fields of a type — per argument: name, type and description.
Kept separate from the field lists on purpose: a field's own line is about ten tokens, its argument tree one to two orders of magnitude more. A relation field carries a whole filter input for the far object plus order_by/limit/offset; the _join field carries the widest argument set in the schema. Name only the fields you will actually parameterize.`),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Full type name")),
		mcp.WithArray("fields", mcp.Required(), mcp.Description("Field names, copied from a field listing"),
			mcp.Items(map[string]any{"type": "string"})),
		mcp.WithOutputSchema[FieldArgsResult](),
	), s.schemaFieldArgs)

	mcpServer.AddTool(mcp.NewTool("schema-enum_values",
		mcp.WithDescription(`Return the values of a GraphQL ENUM — call before writing one into a query, since invalid values fail validation.
Common enums: OrderDirection (ASC, DESC), TimeExtract, TimeBucket, GeometryType.`),
		mcp.WithString("type_name", mcp.Required(), mcp.Description("Enum type name")),
		mcp.WithNumber("limit", mcp.Description("Page size (1-200)"), mcp.DefaultNumber(defaultPageLimit)),
		mcp.WithNumber("offset", mcp.Description("Values to skip"), mcp.DefaultNumber(0)),
		mcp.WithOutputSchema[Page[EnumValue]](),
	), s.schemaEnumValues)

	mcpServer.AddTool(mcp.NewTool("data-field_values",
		mcp.WithDescription(`Show what is actually IN a field: its most common values with row counts, and optionally min/max/avg. Use it before writing a filter, so you match values that exist instead of guessing their spelling or range.
This RUNS A QUERY over the data under your own permissions — it is not schema introspection. Pass 'filter' to scope it to a subset.`),
		mcp.WithString("object_name", mcp.Required(), mcp.Description("The data object's GraphQL type name")),
		mcp.WithString("field_name", mcp.Required(), mcp.Description("Field to summarise")),
		mcp.WithNumber("limit", mcp.Description("Distinct values to return (1-100)"), mcp.DefaultNumber(10)),
		mcp.WithBoolean("calculate_stats", mcp.Description("Also compute min/max/avg where the type allows"), mcp.DefaultBool(false)),
		mcp.WithObject("filter", mcp.Description("Scope the summary, same shape as the object's query filter")),
		mcp.WithOutputSchema[FieldValues](),
	), s.dataFieldValues)

	mcpServer.AddTool(mcp.NewTool("data-inline_graphql_result",
		mcp.WithDescription(`Execute a GraphQL query and return JSON result with optional jq transform.
If result is truncated (is_truncated=true), increase max_result_size (up to 10000) or use jq_transform to reduce output.
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
		mcp.WithNumber("max_result_size", mcp.Description("Max result bytes (100-10000). Increase if result is truncated."), mcp.DefaultNumber(1000)),
	), s.inlineGraphQLResult)

	mcpServer.AddTool(mcp.NewTool("data-execute_mutation",
		mcp.WithDescription(`Execute a GraphQL MUTATION (insert/update/delete a data object, or call a mutation function) and return the JSON result.
THIS MODIFIES DATA — only call when the user explicitly asked to create, update, or delete data. Runs with the caller's permissions; operations the user is not allowed to perform are rejected by the engine.
Mutations mirror queries — modules are nested fields, the operation MUST start with 'mutation':
- insert returns the new row (select its fields directly):
    mutation { module { insert_<Object>(data: {field: value}) { id ... } } }
- update/delete return OperationResult { affected_rows }; filter is REQUIRED to scope rows:
    mutation { module { update_<Object>(filter: {...}, data: {...}) { affected_rows } } }
    mutation { module { delete_<Object>(filter: {...}) { affected_rows } } }
- mutation functions are nested under 'function':
    mutation { function { module { <mutation_func>(args) { ... } } } }
Field names are <Object> with the catalog prefix. BEFORE calling: catalog-describe names the object and its module, schema-type_fields on the module's mutation root type gives the exact insert_/update_/delete_ field, and schema-type_fields on the data input and filter types gives their shape (schema-describe_types identifies a type name you do not recognise). Prefer the 'variables' argument for the data payload. If the result is truncated, raise max_result_size or use jq_transform.`),
		mcp.WithString("query", mcp.Required(), mcp.Description("GraphQL mutation (operation must start with 'mutation')")),
		mcp.WithObject("variables", mcp.Description("Mutation variables (use for the data payload / filter)")),
		mcp.WithString("jq_transform", mcp.Description("JQ expression to apply to result")),
		mcp.WithNumber("max_result_size", mcp.Description("Max result bytes (100-10000). Increase if result is truncated."), mcp.DefaultNumber(1000)),
	), s.executeMutation)

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
// Uses the caller's context (with user permissions). Used for user data queries.
func (s *Server) queryScan(ctx context.Context, gql string, vars map[string]any, path string, target any) error {
	res, err := s.querier.Query(ctx, gql, vars)
	if err != nil {
		return err
	}
	// Check res.Err() BEFORE registering Close: when the engine
	// reports an error the Response can carry a half-built Data
	// tree (typed-nil ArrowTableStream inside the nested map) and
	// running Close() on it panics. Return early without touching
	// the broken response.
	if rerr := res.Err(); rerr != nil {
		return rerr
	}
	defer res.Close()
	return res.ScanData(path, target)
}

// queryScanLookup executes a SINGLE-lookup query — one aliased root field that
// the engine may resolve to null — and reports whether it was served at all.
//
// The alias is read off the ROOT rather than scanned by path, because a null
// AT a path is not something ScanData can report: it walks into the value and
// answers ErrNoData, the same error a broken response gives. Here the two must
// stay apart — "no such name, or not yours" is an answer the agent can act on,
// and an engine failure is not.
//
// (Until the null-response fix this also had to tolerate the path scan failing
// outright, because a resolved-to-null field never reached the response at
// all. It does now, so a scan error is once again just an error.)
func (s *Server) queryScanLookup(ctx context.Context, gql string, vars map[string]any, alias string, target any) (bool, error) {
	root := map[string]json.RawMessage{}
	if err := s.queryScan(ctx, gql, vars, "", &root); err != nil {
		return false, err
	}
	raw, ok := root[alias]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return false, nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return false, err
	}
	return true, nil
}

// queryScanAdmin executes a catalog query with full access (no permission
// filtering). Used by the search path to READ THE INDEX: the catalog views are
// ordinary data objects, and a role may well be denied them while still being
// entitled to use MCP. Nothing it returns reaches the client without passing
// filterHits, which re-asks the engine in the caller's own context.
func (s *Server) queryScanAdmin(ctx context.Context, gql string, vars map[string]any, path string, target any) error {
	return s.queryScan(auth.ContextWithFullAccess(ctx), gql, vars, path, target)
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
