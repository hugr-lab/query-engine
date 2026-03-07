package schema

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"
)

// directiveJSON is the JSON representation of a single directive.
type directiveJSON struct {
	Name string                  `json:"name"`
	Args map[string]any          `json:"args,omitempty"`
}

// MarshalDirectives serializes an ast.DirectiveList to a stable, deterministic
// JSON format. The output is a JSON array of objects:
//
//	[{"name": "table", "args": {"name": "users"}}, {"name": "pk"}]
//
// Directives without arguments omit the "args" key.
// The same input always produces byte-identical output (deterministic).
func MarshalDirectives(dirs ast.DirectiveList) ([]byte, error) {
	result := make([]directiveJSON, 0, len(dirs))
	for _, d := range dirs {
		dj := directiveJSON{Name: d.Name}
		if len(d.Arguments) > 0 {
			dj.Args = make(map[string]any, len(d.Arguments))
			for _, arg := range d.Arguments {
				dj.Args[arg.Name] = marshalValue(arg.Value)
			}
		}
		result = append(result, dj)
	}
	// Go's json.Marshal sorts map keys alphabetically, so the output is
	// deterministic. Note: argument order within each directive is alphabetical
	// after round-trip, not the original declaration order.
	return json.Marshal(result)
}

// marshalValue converts an ast.Value to a Go value suitable for JSON marshaling.
func marshalValue(v *ast.Value) any {
	if v == nil {
		return nil
	}
	switch v.Kind {
	case ast.NullValue:
		return nil
	case ast.IntValue:
		if n, err := strconv.ParseInt(v.Raw, 10, 64); err == nil {
			return n
		}
		return v.Raw
	case ast.FloatValue:
		if f, err := strconv.ParseFloat(v.Raw, 64); err == nil {
			return f
		}
		return v.Raw
	case ast.BooleanValue:
		return v.Raw == "true"
	case ast.StringValue:
		return v.Raw
	case ast.EnumValue:
		// Wrap enum values to distinguish from plain strings in JSON.
		// Known limitation: an object value with a single key "$enum" would be
		// incorrectly deserialized as an enum. This is acceptable because "$enum"
		// is not a valid GraphQL directive argument name.
		return map[string]any{"$enum": v.Raw}
	case ast.ListValue:
		items := make([]any, 0, len(v.Children))
		for _, child := range v.Children {
			items = append(items, marshalValue(child.Value))
		}
		return items
	case ast.ObjectValue:
		obj := make(map[string]any, len(v.Children))
		for _, child := range v.Children {
			obj[child.Name] = marshalValue(child.Value)
		}
		return obj
	default:
		return v.Raw
	}
}

// UnmarshalDirectives deserializes JSON (produced by MarshalDirectives) back
// into an ast.DirectiveList with full structural fidelity.
func UnmarshalDirectives(data []byte) (ast.DirectiveList, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal directives: %w", err)
	}

	dirs := make(ast.DirectiveList, 0, len(raw))
	for i, r := range raw {
		var dj struct {
			Name string                    `json:"name"`
			Args map[string]json.RawMessage `json:"args"`
		}
		if err := json.Unmarshal(r, &dj); err != nil {
			return nil, fmt.Errorf("unmarshal directive %d: %w", i, err)
		}
		d := &ast.Directive{Name: dj.Name}
		if len(dj.Args) > 0 {
			// Sort keys for deterministic argument order.
			keys := make([]string, 0, len(dj.Args))
			for k := range dj.Args {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			d.Arguments = make(ast.ArgumentList, 0, len(dj.Args))
			for _, key := range keys {
				val, err := unmarshalValue(dj.Args[key])
				if err != nil {
					return nil, fmt.Errorf("unmarshal directive %q arg %q: %w", dj.Name, key, err)
				}
				d.Arguments = append(d.Arguments, &ast.Argument{
					Name:  key,
					Value: val,
				})
			}
		}
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// unmarshalValue converts a JSON value back to an *ast.Value.
func unmarshalValue(data json.RawMessage) (*ast.Value, error) {
	// Check for null.
	if string(data) == "null" {
		return &ast.Value{Kind: ast.NullValue, Raw: "null"}, nil
	}

	// Try boolean.
	if string(data) == "true" || string(data) == "false" {
		return &ast.Value{Kind: ast.BooleanValue, Raw: string(data)}, nil
	}

	// Try number (int or float).
	var num json.Number
	if err := json.Unmarshal(data, &num); err == nil {
		raw := num.String()
		// If it contains a dot or exponent, it's a float.
		isFloat := false
		for _, c := range raw {
			if c == '.' || c == 'e' || c == 'E' {
				isFloat = true
				break
			}
		}
		if isFloat {
			return &ast.Value{Kind: ast.FloatValue, Raw: raw}, nil
		}
		return &ast.Value{Kind: ast.IntValue, Raw: raw}, nil
	}

	// Try string.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		return &ast.Value{Kind: ast.StringValue, Raw: s}, nil
	}

	// Try array (list).
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err == nil {
		children := make(ast.ChildValueList, 0, len(arr))
		for _, item := range arr {
			val, err := unmarshalValue(item)
			if err != nil {
				return nil, err
			}
			children = append(children, &ast.ChildValue{Value: val})
		}
		return &ast.Value{Kind: ast.ListValue, Children: children}, nil
	}

	// Try object — could be an enum wrapper {"$enum": "..."} or an object value.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err == nil {
		// Check for enum wrapper.
		if enumRaw, ok := obj["$enum"]; ok && len(obj) == 1 {
			var enumVal string
			if err := json.Unmarshal(enumRaw, &enumVal); err == nil {
				return &ast.Value{Kind: ast.EnumValue, Raw: enumVal}, nil
			}
		}

		// Regular object value.
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		children := make(ast.ChildValueList, 0, len(obj))
		for _, key := range keys {
			val, err := unmarshalValue(obj[key])
			if err != nil {
				return nil, err
			}
			children = append(children, &ast.ChildValue{Name: key, Value: val})
		}
		return &ast.Value{Kind: ast.ObjectValue, Children: children}, nil
	}

	return nil, fmt.Errorf("cannot unmarshal value: %s", string(data))
}

// argDefJSON is the JSON representation of a directive argument definition.
type argDefJSON struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Description  string `json:"description,omitempty"`
	DefaultValue any    `json:"default,omitempty"`
}

// MarshalArgumentDefinitions serializes an ast.ArgumentDefinitionList to JSON.
// Used for storing directive argument schemas in _schema_directives.
func MarshalArgumentDefinitions(args ast.ArgumentDefinitionList) ([]byte, error) {
	if len(args) == 0 {
		return []byte("[]"), nil
	}
	result := make([]argDefJSON, 0, len(args))
	for _, arg := range args {
		ad := argDefJSON{
			Name:        arg.Name,
			Type:        MarshalType(arg.Type),
			Description: arg.Description,
		}
		if arg.DefaultValue != nil {
			ad.DefaultValue = marshalValue(arg.DefaultValue)
		}
		result = append(result, ad)
	}
	return json.Marshal(result)
}

// UnmarshalArgumentDefinitions deserializes JSON back into an ast.ArgumentDefinitionList.
func UnmarshalArgumentDefinitions(data []byte) (ast.ArgumentDefinitionList, error) {
	if len(data) == 0 || string(data) == "[]" || string(data) == "" {
		return nil, nil
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal argument definitions: %w", err)
	}

	args := make(ast.ArgumentDefinitionList, 0, len(raw))
	for i, r := range raw {
		var adj struct {
			Name         string          `json:"name"`
			Type         string          `json:"type"`
			Description  string          `json:"description"`
			DefaultValue json.RawMessage `json:"default"`
		}
		if err := json.Unmarshal(r, &adj); err != nil {
			return nil, fmt.Errorf("unmarshal argument definition %d: %w", i, err)
		}

		argType, err := UnmarshalType(adj.Type)
		if err != nil {
			return nil, fmt.Errorf("unmarshal argument %q type: %w", adj.Name, err)
		}

		arg := &ast.ArgumentDefinition{
			Name:        adj.Name,
			Type:        argType,
			Description: adj.Description,
		}

		if len(adj.DefaultValue) > 0 && string(adj.DefaultValue) != "null" {
			val, err := unmarshalValue(adj.DefaultValue)
			if err != nil {
				return nil, fmt.Errorf("unmarshal argument %q default: %w", adj.Name, err)
			}
			arg.DefaultValue = val
		}

		args = append(args, arg)
	}
	return args, nil
}

// MarshalValue serializes an *ast.Value to a JSON-compatible Go value.
// Exported wrapper for the internal marshalValue function.
func MarshalValue(v *ast.Value) any {
	return marshalValue(v)
}

// UnmarshalValue deserializes a JSON value back into an *ast.Value.
// Exported wrapper for the internal unmarshalValue function.
func UnmarshalValue(data json.RawMessage) (*ast.Value, error) {
	return unmarshalValue(data)
}
