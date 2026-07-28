package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

// data-field_values is a DATA tool, not a schema one — it runs a real
// aggregation and returns rows. It carried a discovery- prefix, which made
// agents reach for it while still exploring the schema and spend a query on
// something they had no use for yet.
//
// Everything it needs about the object — the module to nest the call in and
// the names of the aggregation queries — comes from the meta-query family in
// the caller's context, so an object the caller may not see is simply not
// found, and the aggregation itself runs under the caller's own rights.

// FieldValues is the distribution of one field.
type FieldValues struct {
	Object string             `json:"object"`
	Field  string             `json:"field"`
	Values []FieldValueBucket `json:"values,omitempty"`
	Stats  map[string]any     `json:"stats,omitempty" jsonschema_description:"min/max/avg where the field's type allows, plus the distinct count"`
}

type FieldValueBucket struct {
	Value any `json:"value"`
	Rows  int `json:"rows"`
}

func (s *Server) dataFieldValues(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	object := req.GetString("object_name", "")
	field := req.GetString("field_name", "")
	if object == "" || field == "" {
		return toolResultError("object_name and field_name are required"), nil
	}
	limit := req.GetInt("limit", 10)
	limit = min(100, max(1, limit))
	withStats := req.GetBool("calculate_stats", false)

	var node struct {
		Name       string `json:"name"`
		ModuleName string `json:"moduleName"`
		Queries    []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"queries"`
		Fields []struct {
			Name string      `json:"name"`
			Type *gqlTypeRef `json:"type"`
		} `json:"fields"`
	}
	// Scanned from the ROOT into a raw envelope: an object the caller may not
	// see resolves to null, and a null at a named path is a scan error whose
	// message ("wrong data path") says nothing an agent can act on.
	root := map[string]json.RawMessage{}
	err := s.queryScan(ctx, `query($n: String!) { obj: _dataObject(name: $n) {
		name moduleName
		queries { name type }
		fields { name `+typeRefSelection+` }
	} }`, map[string]any{"n": object}, "", &root)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	found, ok := root["obj"]
	if !ok || len(found) == 0 || string(found) == "null" {
		return toolResultError(fmt.Sprintf("data object %q not found, or not visible to you", object)), nil
	}
	if err := json.Unmarshal(found, &node); err != nil {
		return toolResultError(err.Error()), nil
	}

	var fieldType string
	var known bool
	for _, f := range node.Fields {
		if f.Name == field {
			fieldType, known = f.Type.String(), true
			break
		}
	}
	if !known {
		return toolResultError(fmt.Sprintf("%q has no field %q — call catalog-object_fields for its fields", object, field)), nil
	}

	var aggName, bucketName string
	for _, q := range node.Queries {
		switch q.Type {
		case "AGGREGATION":
			aggName = q.Name
		case "BUCKET_AGGREGATION":
			bucketName = q.Name
		}
	}

	filterArg, hasFilter := req.GetArguments()["filter"]
	filterStr := ""
	vars := map[string]any{}
	varDecl := ""
	if hasFilter && filterArg != nil {
		vars["filter"] = filterArg
		varDecl = fmt.Sprintf("($filter: %s_filter)", object)
		filterStr = "filter: $filter"
	}

	var parts []string
	if withStats && aggName != "" {
		call := aggName
		if filterStr != "" {
			call = fmt.Sprintf("%s(%s)", aggName, filterStr)
		}
		parts = append(parts, fmt.Sprintf("stats: %s { field: %s { %s } }", call, field, statsFor(fieldType)))
	}
	if bucketName != "" {
		args := fmt.Sprintf("limit: %d", limit)
		if filterStr != "" {
			args += " " + filterStr
		}
		parts = append(parts, fmt.Sprintf("values: %s(%s) { key { value: %s } aggregations { _rows_count } }",
			bucketName, args, field))
	}
	if len(parts) == 0 {
		return toolResultError(fmt.Sprintf("%q has no aggregation queries — nothing to summarise", object)), nil
	}

	pre, post := modulePath(node.ModuleName)
	gql := fmt.Sprintf("query%s { %s %s %s }", varDecl, pre, strings.Join(parts, "\n"), post)

	var raw struct {
		Stats  map[string]any `json:"stats"`
		Values []struct {
			Key struct {
				Value any `json:"value"`
			} `json:"key"`
			Aggregations struct {
				Rows int `json:"_rows_count"`
			} `json:"aggregations"`
		} `json:"values"`
	}
	// The result nests exactly as the module path does, and ScanData takes the
	// same dotted spelling.
	if err := s.queryScan(ctx, gql, vars, node.ModuleName, &raw); err != nil {
		return toolResultError(err.Error()), nil
	}

	out := FieldValues{Object: object, Field: field}
	if stats, ok := raw.Stats["field"].(map[string]any); ok {
		out.Stats = stats
	}
	for _, v := range raw.Values {
		out.Values = append(out.Values, FieldValueBucket{Value: v.Key.Value, Rows: v.Aggregations.Rows})
	}
	return toolResultJSON(out), nil
}

// statsFor picks the aggregations the field's type actually supports —
// asking for avg on a string is a validation error, not an empty answer.
func statsFor(fieldType string) string {
	t := strings.Trim(fieldType, "[]!")
	t = strings.TrimSuffix(t, "!")
	switch t {
	case "Float", "Int", "BigInt":
		return "min max avg distinct: count"
	case "Timestamp", "Date", "DateTime", "Time":
		return "min max distinct: count"
	default:
		return "distinct: count"
	}
}

// modulePath renders a dotted module name as the GraphQL nesting a query
// needs, and the matching closing braces.
func modulePath(module string) (pre, post string) {
	if module == "" {
		return "", ""
	}
	for _, p := range strings.Split(module, ".") {
		pre += p + " { "
		post += " }"
	}
	return pre, post
}
