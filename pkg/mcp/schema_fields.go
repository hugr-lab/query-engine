package mcp

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/mark3labs/mcp-go/mcp"
)

// The field surface is split in two because the two questions have different
// mechanics, and one tool answering both would carry a ranking argument that
// is inert for most of its inputs:
//
//   - catalog-object_fields walks a DATA OBJECT. Its fields have curated
//     descriptions and vectors, so they can be ranked by MEANING, and they are
//     not all columns — a relation field is a path, an extra field is computed.
//     This is the field tool an exploration path needs.
//   - schema-type_fields walks ANY generated type — a filter input, an
//     aggregation, a module root — deterministically, with a cursor. This is
//     what a composition path needs when it lands on shop_items_filter.
//
// Both read through the caller's context, so nothing needs re-filtering, and
// both leave ARGUMENT trees to schema-field_args: a field's own line is ~10
// tokens while its arguments are one to two orders of magnitude bigger, and a
// query parameterizes two or three fields, not all of them.

// TypeField is one member of a type.
type TypeField struct {
	Name        string `json:"name"`
	Type        string `json:"type"        jsonschema_description:"GraphQL type, SDL spelling"`
	ArgsCount   int    `json:"args_count,omitempty" jsonschema_description:"Call schema-field_args for the argument tree of the fields you will parameterize"`
	Description string `json:"description,omitempty"`

	// Data-object fields only.
	FieldKind string `json:"field_kind,omitempty" jsonschema_description:"column | relation | extra"`
	RefObject string `json:"ref_object,omitempty" jsonschema_description:"For a relation field: the data object it navigates to — select this field to get there"`
	IsPK      bool   `json:"is_pk,omitempty"`
}

// FieldArgs is one field's argument tree.
type FieldArgs struct {
	Field string         `json:"field"`
	Args  []DescribedArg `json:"args"`
}

type FieldArgsResult struct {
	TypeName string      `json:"type_name"`
	Items    []FieldArgs `json:"items"`
	NotFound []string    `json:"not_found,omitempty"`
}

// EnumValue is one enum member.
type EnumValue struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Deprecated  bool   `json:"deprecated,omitempty"`
}

// --- scan targets ---

type metaFieldNode struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	McpExclude  bool             `json:"mcp_exclude"`
	Type        *gqlTypeRef      `json:"type"`
	Args        []metaInputValue `json:"args"`
}

type metaTypeFieldsNode struct {
	Name        string          `json:"name"`
	Kind        string          `json:"kind"`
	Fields      []metaFieldNode `json:"fields"`
	InputFields []struct {
		Name        string      `json:"name"`
		Description string      `json:"description"`
		Type        *gqlTypeRef `json:"type"`
	} `json:"inputFields"`
}

type metaObjectFieldsNode struct {
	Name   string `json:"name"`
	Fields []struct {
		metaFieldNode
	} `json:"fields"`
	PrimaryKey []string `json:"primaryKey"`
	Relations  []struct {
		FieldName  string `json:"fieldName"`
		DataObject *struct {
			Name string `json:"name"`
		} `json:"dataObject"`
	} `json:"relations"`
}

// --- catalog-object_fields ---

func (s *Server) catalogObjectFields(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	object := req.GetString("object", "")
	if object == "" {
		return toolResultError("object is required — the data object's GraphQL type name"), nil
	}
	relevance := strings.TrimSpace(req.GetString("relevance_query", ""))
	prefix := strings.ToLower(req.GetString("prefix", ""))
	includeDesc := req.GetBool("include_description", true)
	limit, offset := pageArgsOf(req)

	var node metaObjectFieldsNode
	found, err := s.queryScanLookup(ctx, `query($n: String!) { obj: _dataObject(name: $n) {
		name primaryKey
		fields { name description mcp_exclude `+typeRefSelection+` args { name } }
		relations { fieldName dataObject { name } }
	} }`, map[string]any{"n": object}, "obj", &node)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	if !found {
		return toolResultError(fmt.Sprintf("data object %q not found, or not visible to you", object)), nil
	}

	refByField := map[string]string{}
	for _, r := range node.Relations {
		if r.FieldName != "" && r.DataObject != nil {
			refByField[r.FieldName] = r.DataObject.Name
		}
	}

	items := make([]TypeField, 0, len(node.Fields))
	for _, f := range node.Fields {
		if f.McpExclude || isPlaceholderField(f.Name) {
			continue
		}
		if prefix != "" && !strings.HasPrefix(strings.ToLower(f.Name), prefix) {
			continue
		}
		item := TypeField{
			Name: f.Name, Type: f.Type.String(), ArgsCount: len(f.Args),
			IsPK: slices.Contains(node.PrimaryKey, f.Name),
		}
		if includeDesc {
			item.Description = f.Description
		}
		item.RefObject = refByField[f.Name]
		item.FieldKind = classifyFieldKind(item.Type, len(f.Args), item.RefObject != "")
		items = append(items, item)
	}

	if relevance != "" {
		s.orderFieldsByRelevance(ctx, object, relevance, items)
	}
	return toolResultJSON(paginate(items, limit, offset, false)), nil
}

// orderFieldsByRelevance reorders a list the caller can ALREADY see. The
// ranking read is privileged — the index lives in the catalog views — but it
// can leak nothing here: it only supplies an order over names that the
// caller's own visible field list already contains. Anything the index knows
// about and the caller may not see simply has no row to move.
func (s *Server) orderFieldsByRelevance(ctx context.Context, object, query string, items []TypeField) {
	var rows []vectorRow
	err := s.queryScanAdmin(ctx, `query($q: String!, $obj: String!) { core {
		entity_fields(filter: {type_name: {eq: $obj}},
			order_by: [{field: "_distance_to_query", direction: ASC}], limit: 500) {
			name _distance_to_query(query: $q)
		}
	} }`, map[string]any{"q": query, "obj": object}, "core.entity_fields", &rows)
	if err != nil || len(rows) == 0 {
		// No index (no embedder, or the compiled storage): keep the schema
		// order rather than pretending to rank.
		return
	}
	rank := make(map[string]int, len(rows))
	for i, r := range rows {
		rank[r.Name] = i
	}
	unranked := len(rank)
	slices.SortStableFunc(items, func(a, b TypeField) int {
		ra, ok := rank[a.Name]
		if !ok {
			ra = unranked
		}
		rb, ok := rank[b.Name]
		if !ok {
			rb = unranked
		}
		return ra - rb
	})
}

// --- schema-type_fields ---

func (s *Server) schemaTypeFields(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}
	prefix := strings.ToLower(req.GetString("prefix", ""))
	includeDesc := req.GetBool("include_description", true)
	limit, offset := pageArgsOf(req)

	var node metaTypeFieldsNode
	found, err := s.queryScanLookup(ctx, `query($n: String!) { t: __type(name: $n) {
		name kind
		fields { name description mcp_exclude `+typeRefSelection+` args { name } }
		inputFields { name description `+typeRefSelection+` }
	} }`, map[string]any{"n": typeName}, "t", &node)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	if !found {
		return toolResultError(fmt.Sprintf("type %q not found, or not visible to you", typeName)), nil
	}

	items := []TypeField{}
	add := func(name, desc, typ string, args int) {
		if prefix != "" && !strings.HasPrefix(strings.ToLower(name), prefix) {
			return
		}
		if isPlaceholderField(name) {
			return
		}
		f := TypeField{Name: name, Type: typ, ArgsCount: args}
		if includeDesc {
			f.Description = desc
		}
		items = append(items, f)
	}
	for _, f := range node.Fields {
		if f.McpExclude {
			continue
		}
		add(f.Name, f.Description, f.Type.String(), len(f.Args))
	}
	// An INPUT_OBJECT carries inputFields instead — the same question, and an
	// agent holding a filter or a mutation input asks it the same way.
	for _, f := range node.InputFields {
		add(f.Name, f.Description, f.Type.String(), 0)
	}
	return toolResultJSON(paginate(items, limit, offset, false)), nil
}

// --- schema-field_args ---

func (s *Server) schemaFieldArgs(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}
	names := req.GetStringSlice("fields", nil)
	if len(names) == 0 {
		return toolResultError("fields is required — name the few fields you will parameterize"), nil
	}

	var node struct {
		Name   string `json:"name"`
		Fields []struct {
			Name string           `json:"name"`
			Args []metaInputValue `json:"args"`
		} `json:"fields"`
	}
	found, err := s.queryScanLookup(ctx, `query($n: String!) { t: __type(name: $n) {
		name fields { name args { name description `+typeRefSelection+` } }
	} }`, map[string]any{"n": typeName}, "t", &node)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	if !found {
		return toolResultError(fmt.Sprintf("type %q not found, or not visible to you", typeName)), nil
	}

	byName := map[string][]metaInputValue{}
	for _, f := range node.Fields {
		byName[f.Name] = f.Args
	}
	out := &FieldArgsResult{TypeName: node.Name, Items: []FieldArgs{}}
	for _, name := range names {
		args, ok := byName[name]
		if !ok {
			out.NotFound = append(out.NotFound, name)
			continue
		}
		out.Items = append(out.Items, FieldArgs{Field: name, Args: describedArgs(args)})
	}
	return toolResultJSON(out), nil
}

// --- schema-enum_values ---

func (s *Server) schemaEnumValues(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	typeName := req.GetString("type_name", "")
	if typeName == "" {
		return toolResultError("type_name is required"), nil
	}
	limit, offset := pageArgsOf(req)

	var node struct {
		Name       string `json:"name"`
		Kind       string `json:"kind"`
		EnumValues []struct {
			Name              string `json:"name"`
			Description       string `json:"description"`
			IsDeprecated      bool   `json:"isDeprecated"`
			DeprecationReason string `json:"deprecationReason"`
		} `json:"enumValues"`
	}
	found, err := s.queryScanLookup(ctx, `query($n: String!) { t: __type(name: $n) {
		name kind enumValues { name description isDeprecated }
	} }`, map[string]any{"n": typeName}, "t", &node)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	if !found {
		return toolResultError(fmt.Sprintf("type %q not found, or not visible to you", typeName)), nil
	}
	if node.Kind != "ENUM" {
		return toolResultError(fmt.Sprintf("type %q is %s, not ENUM — call schema-type_fields for its members",
			typeName, node.Kind)), nil
	}

	items := make([]EnumValue, 0, len(node.EnumValues))
	for _, v := range node.EnumValues {
		items = append(items, EnumValue{
			Name: v.Name, Description: v.Description, Deprecated: v.IsDeprecated,
		})
	}
	return toolResultJSON(paginate(items, limit, offset, false)), nil
}

// isPlaceholderField reports the synthetic members introspection injects to
// keep a type from having zero fields (pkg/metadata). A type whose every real
// field the caller may not see comes back carrying only those — listing them
// would offer an agent a field that cannot be selected.
func isPlaceholderField(name string) bool {
	return name == base.StubFieldName || name == base.PlaceholderFieldName
}

// pageArgsOf reads the shared pagination arguments, clamped to the hard cap.
func pageArgsOf(req mcp.CallToolRequest) (limit, offset int) {
	return clampLimit(req.GetInt("limit", defaultPageLimit), defaultPageLimit),
		max(0, req.GetInt("offset", 0))
}

// clampLimit bounds a requested page size. A non-positive limit reads as
// "unspecified" and takes the default: clamping it to 1 would answer a
// one-item page to a caller who passed 0 meaning "no opinion".
func clampLimit(n, def int) int {
	switch {
	case n <= 0:
		return def
	case n > maxPageLimit:
		return maxPageLimit
	}
	return n
}
