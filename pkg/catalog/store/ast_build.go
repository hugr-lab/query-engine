package store

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

// AST builders for the read side: the emit half of the directive↔bag pairs
// rebuilds directives from typed members with these helpers. Values carry the
// kind the SDL form would have (enum/int/list/object) so a formatted definition
// stays valid SDL, though the planner reads only Raw.

var reconPos = &ast.Position{Src: &ast.Source{Name: "catalog:reconstruct"}}

func directive(name string, args ...*ast.Argument) *ast.Directive {
	return &ast.Directive{Name: name, Arguments: args, Position: reconPos}
}

func strArg(name, val string) *ast.Argument {
	return &ast.Argument{Name: name, Value: &ast.Value{Raw: val, Kind: ast.StringValue, Position: reconPos}, Position: reconPos}
}

func boolArg(name string, val bool) *ast.Argument {
	return &ast.Argument{Name: name, Value: &ast.Value{Raw: strconv.FormatBool(val), Kind: ast.BooleanValue, Position: reconPos}, Position: reconPos}
}

func enumArg(name, val string) *ast.Argument {
	return &ast.Argument{Name: name, Value: &ast.Value{Raw: val, Kind: ast.EnumValue, Position: reconPos}, Position: reconPos}
}

// numArg renders a numeric argument from a decoded bag member (int64 straight
// from a directive, float64 after a JSON round-trip).
func numArg(name string, val any) *ast.Argument {
	return &ast.Argument{Name: name, Value: &ast.Value{Raw: numRaw(val), Kind: ast.IntValue, Position: reconPos}, Position: reconPos}
}

func numRaw(val any) string {
	switch v := val.(type) {
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return numRaw(float64(v))
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

// strListArg renders a [String!] argument ("fields", "source_fields", "tags").
func strListArg(name string, vals []string) *ast.Argument {
	return listArg(name, vals, ast.StringValue)
}

// enumListArg renders a list-of-enum argument ("operations").
func enumListArg(name string, vals []string) *ast.Argument {
	return listArg(name, vals, ast.EnumValue)
}

func listArg(name string, vals []string, kind ast.ValueKind) *ast.Argument {
	list := &ast.Value{Kind: ast.ListValue, Position: reconPos}
	for _, v := range vals {
		list.Children = append(list.Children, &ast.ChildValue{
			Value: &ast.Value{Raw: v, Kind: kind, Position: reconPos}, Position: reconPos,
		})
	}
	return &ast.Argument{Name: name, Value: list, Position: reconPos}
}

// jsonArg renders a decoded JSON member (map / list / scalar — @default(value:),
// @function_call(args:)) back into an ast.Value tree.
func jsonArg(name string, val any) *ast.Argument {
	return &ast.Argument{Name: name, Value: jsonValue(val), Position: reconPos}
}

func jsonValue(val any) *ast.Value {
	switch v := val.(type) {
	case nil:
		return &ast.Value{Raw: "null", Kind: ast.NullValue, Position: reconPos}
	case bool:
		return &ast.Value{Raw: strconv.FormatBool(v), Kind: ast.BooleanValue, Position: reconPos}
	case string:
		return &ast.Value{Raw: v, Kind: ast.StringValue, Position: reconPos}
	case float64, float32, int, int64:
		raw := numRaw(v)
		kind := ast.IntValue
		if strings.ContainsAny(raw, ".eE") {
			kind = ast.FloatValue
		}
		return &ast.Value{Raw: raw, Kind: kind, Position: reconPos}
	case []any:
		list := &ast.Value{Kind: ast.ListValue, Position: reconPos}
		for _, item := range v {
			list.Children = append(list.Children, &ast.ChildValue{Value: jsonValue(item), Position: reconPos})
		}
		return list
	case map[string]any:
		obj := &ast.Value{Kind: ast.ObjectValue, Position: reconPos}
		for _, k := range sortedKeys(v) {
			obj.Children = append(obj.Children, &ast.ChildValue{Name: k, Value: jsonValue(v[k]), Position: reconPos})
		}
		return obj
	default:
		return &ast.Value{Raw: fmt.Sprint(v), Kind: ast.StringValue, Position: reconPos}
	}
}

// parseFieldType turns a stored type string (Int!, [tags], [Int!]!) into ast.Type.
func parseFieldType(s string) *ast.Type {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "!") {
		t := parseFieldType(s[:len(s)-1])
		t.NonNull = true
		return t
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return &ast.Type{Elem: parseFieldType(s[1 : len(s)-1]), Position: reconPos}
	}
	return &ast.Type{NamedType: s, Position: reconPos}
}

// parseValueRaw turns a stored SDL value text (function argument defaults kept
// via Value.String()) back into an ast.Value with a best-effort kind.
func parseValueRaw(s string) *ast.Value {
	switch {
	case s == "null":
		return &ast.Value{Raw: "null", Kind: ast.NullValue, Position: reconPos}
	case s == "true" || s == "false":
		return &ast.Value{Raw: s, Kind: ast.BooleanValue, Position: reconPos}
	case len(s) >= 2 && strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`):
		if unq, err := strconv.Unquote(s); err == nil {
			return &ast.Value{Raw: unq, Kind: ast.StringValue, Position: reconPos}
		}
		return &ast.Value{Raw: s, Kind: ast.StringValue, Position: reconPos}
	default:
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return &ast.Value{Raw: s, Kind: ast.IntValue, Position: reconPos}
		}
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return &ast.Value{Raw: s, Kind: ast.FloatValue, Position: reconPos}
		}
		return &ast.Value{Raw: s, Kind: ast.EnumValue, Position: reconPos}
	}
}
