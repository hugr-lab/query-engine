package sdl

import (
	"encoding/json"
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
)

func DirectiveArgValue(d *ast.Directive, name string, vars map[string]any) string {
	if d == nil {
		return ""
	}
	a := d.Arguments.ForName(name)
	if a == nil {
		return ""
	}
	if a.Value.Kind == ast.Variable {
		if v, ok := vars[a.Value.Raw]; ok {
			switch v := v.(type) {
			case map[string]any:
				b, err := json.Marshal(v)
				if err != nil {
					return ""
				}
				return string(b)
			default:
				return fmt.Sprint(v)
			}
		}
		return ""
	}
	if a.Value == nil || a.Value.Raw == "" {
		return ""
	}
	return a.Value.Raw
}

func DirectiveArgChildValues(d *ast.Directive, name string, vars map[string]any) []string {
	if d == nil {
		return nil
	}
	a := d.Arguments.ForName(name)
	if a == nil {
		return nil
	}
	if a.Value.Kind == ast.Variable {
		if v, ok := vars[a.Value.Raw]; ok {
			switch v := v.(type) {
			case []interface{}:
				out := make([]string, len(v))
				for i, vv := range v {
					switch vv := vv.(type) {
					case map[string]any:
						b, err := json.Marshal(vv)
						if err != nil {
							return nil
						}
						out[i] = string(b)
					default:
						out[i] = fmt.Sprint(vv)
					}
				}
				return out
			default:
				return []string{fmt.Sprint(v)}
			}
		}
		return nil
	}
	if a.Value == nil {
		return nil
	}
	var out []string
	for _, v := range a.Value.Children {
		if v.Value != nil {
			out = append(out, v.Value.Raw)
		}
	}
	return out
}
