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
	return argumentRawValue(a)
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
	return argumentChildRawValues(a)
}

// directiveArgValue reads a raw argument value from a directive (no variable resolution).
func directiveArgValue(d *ast.Directive, name string) string {
	if d == nil {
		return ""
	}
	return argumentRawValue(d.Arguments.ForName(name))
}

// directiveArgChildValues reads child values from a list argument (no variable resolution).
func directiveArgChildValues(d *ast.Directive, name string) []string {
	if d == nil {
		return nil
	}
	return argumentChildRawValues(d.Arguments.ForName(name))
}

func fieldDirectiveArgValue(field *ast.FieldDefinition, directiveName, argName string) string {
	if field == nil {
		return ""
	}
	return directiveArgValue(field.Directives.ForName(directiveName), argName)
}

func objectDirectiveArgValue(def *ast.Definition, directiveName, argName string) string {
	if def == nil {
		return ""
	}
	return directiveArgValue(def.Directives.ForName(directiveName), argName)
}

func argumentRawValue(a *ast.Argument) string {
	if a == nil {
		return ""
	}
	return a.Value.Raw
}

func argumentChildRawValues(a *ast.Argument) []string {
	if a == nil {
		return nil
	}
	var out []string
	for _, v := range a.Value.Children {
		out = append(out, v.Value.Raw)
	}
	return out
}
