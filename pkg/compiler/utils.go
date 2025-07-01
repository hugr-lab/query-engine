package compiler

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"

	_ "embed"
)

const (
	catalogSystemVariableName = "$catalog"
)

var systemTypeByName = map[string]struct{}{
	"Query":        {},
	"Mutation":     {},
	"Subscription": {},
}

func IsSystemType(def *ast.Definition) bool {
	if def.Kind == ast.Scalar || strings.HasPrefix(def.Name, "__") {
		return true
	}
	if _, ok := systemTypeByName[def.Name]; ok {
		return true
	}

	return HasSystemDirective(def.Directives)
}

func HasSystemDirective(def ast.DirectiveList) bool {
	return def.ForName(base.SystemDirective.Name) != nil
}

func IsDataObject(def *ast.Definition) bool {
	if def.Kind != ast.Object {
		return false
	}
	if IsSystemType(def) {
		return false
	}
	if d := def.Directives.ForName(objectTableDirectiveName); d != nil {
		return true
	}
	if d := def.Directives.ForName(objectViewDirectiveName); d != nil {
		return true
	}
	return false
}

func IsHyperTable(def *ast.Definition) bool {
	if def.Kind != ast.Object {
		return false
	}
	if IsSystemType(def) {
		return false
	}
	if d := def.Directives.ForName(objectHyperTableDirectiveName); d != nil {
		return true
	}
	return false
}

const (
	Table = "table"
	View  = "view"
)

func DataObjectType(def *ast.Definition) string {
	if d := def.Directives.ForName(Table); d != nil {
		return Table
	}
	if d := def.Directives.ForName(View); d != nil {
		return View
	}
	return ""
}

func IsEqualTypes(a, b *ast.Type) bool {
	if a.Name() != b.Name() {
		return false
	}
	if (a.Elem == nil) != (b.Elem == nil) {
		return false
	}
	if a.Elem != nil && b.Elem != nil {
		return IsEqualTypes(a.Elem, b.Elem)
	}
	return true
}

func compiledPos() *ast.Position {
	return base.CompiledPos("")
}

func CompiledPosName(name string) *ast.Position {
	return base.CompiledPos(name)
}

type Deprecated struct {
	IsDeprecated bool
	Reason       string
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

func directiveArgChildValues(d *ast.Directive, name string) []string {
	if d == nil {
		return nil
	}
	return argumentChildRawValues(d.Arguments.ForName(name))
}

func directiveArgValue(d *ast.Directive, name string) string {
	if d == nil {
		return ""
	}
	return argumentRawValue(d.Arguments.ForName(name))
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

func RemoveFieldsDuplicates(fields []string) []string {
	unique := make(map[string]struct{}, len(fields))
	var uniqueFields []string
	for _, f := range fields {
		if _, ok := unique[f]; ok {
			continue
		}
		unique[f] = struct{}{}
		uniqueFields = append(uniqueFields, f)
	}
	return uniqueFields
}

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

	return argumentRawValue(d.Arguments.ForName(name))
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

	return argumentChildRawValues(d.Arguments.ForName(name))
}
