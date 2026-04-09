package sdl

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// varPattern matches $VarName references inside string values.
var varPattern = regexp.MustCompile(`\$([A-Za-z_][A-Za-z0-9_]*)`)

// InterpolateVars replaces $VarName references inside string values
// of an argument map with corresponding values from the vars map.
// Non-string values and unresolved references are left as-is.
func InterpolateVars(args map[string]any, vars map[string]any) map[string]any {
	if vars == nil || args == nil {
		return args
	}
	out := make(map[string]any, len(args))
	for k, v := range args {
		if s, ok := v.(string); ok && strings.Contains(s, "$") {
			out[k] = interpolateVarsInString(s, vars)
		} else {
			out[k] = v
		}
	}
	return out
}

// FieldArgumentMap resolves a field's arguments and interpolates
// $VarName references in string values from query variables.
func FieldArgumentMap(field *ast.Field, vars map[string]any) map[string]any {
	return InterpolateVars(field.ArgumentMap(vars), vars)
}

func interpolateVarsInString(s string, vars map[string]any) string {
	return varPattern.ReplaceAllStringFunc(s, func(match string) string {
		name := match[1:] // strip leading '$'
		v, ok := vars[name]
		if !ok {
			return match
		}
		return fmt.Sprint(v)
	})
}

// IsScalarType returns true if typeName is a registered scalar type.
func IsScalarType(typeName string) bool {
	return types.IsScalar(typeName)
}

// IsJSONType returns true if typeName is JSON.
func IsJSONType(typeName string) bool {
	return typeName == base.JSONTypeName
}

// SpecifiedByURL returns the specifiedBy URL from a scalar definition.
func SpecifiedByURL(def *ast.Definition) string {
	for _, d := range def.Directives {
		if d.Name == "specifiedBy" {
			return d.Arguments.ForName("url").Value.Raw
		}
	}
	return ""
}

// ParseScalarValue dispatches value parsing to the scalar type's ValueParser interface.
var ParseScalarValue = types.ParseValue

// ParseScalarArray dispatches array parsing to the scalar type's ArrayParser interface.
var ParseScalarArray = types.ParseArray

// ToOutputSQL applies output SQL transformation for a scalar type.
func ToOutputSQL(typeName, sql string, raw bool) string {
	s := types.Lookup(typeName)
	if s == nil {
		return sql
	}
	if t, ok := s.(types.SQLOutputTransformer); ok {
		return t.ToOutputSQL(sql, raw)
	}
	return sql
}

// ToStructFieldSQL applies struct field SQL transformation for a scalar type.
func ToStructFieldSQL(typeName, sql string) string {
	s := types.Lookup(typeName)
	if s == nil {
		return sql
	}
	if t, ok := s.(types.SQLOutputTransformer); ok {
		return t.ToStructFieldSQL(sql)
	}
	return sql
}

// ParseArgumentValue resolves and parses a GraphQL argument value,
// handling scalars, input objects, enums, lists, and variables.
func ParseArgumentValue(ctx context.Context, defs base.DefinitionsSource, arg *ast.ArgumentDefinition, value *ast.Value, vars map[string]any, checkRequired bool) (any, error) {
	if arg.Type.NonNull {
		if value == nil {
			return nil, ErrorPosf(arg.Position, "argument %s is required", arg.Name)
		}
	}
	if value == nil {
		return nil, nil
	}
	if !IsScalarType(arg.Type.Name()) {
		def := defs.ForName(ctx, arg.Type.Name())
		if def == nil || (def.Kind != ast.InputObject && def.Kind != ast.Enum) {
			return nil, ErrorPosf(arg.Position, "unsupported argument type %s", arg.Type.Name())
		}
		if value.Kind == ast.Variable {
			return ParseDataAsInputObject(ctx, defs, arg.Type, vars[value.Raw], checkRequired)
		}
		if value.Kind == ast.EnumValue {
			return value.Raw, nil
		}
		if value.Kind == ast.ListValue {
			var vv []any
			for _, v := range value.Children {
				v, err := ParseArgumentValue(ctx, defs, &ast.ArgumentDefinition{
					Name:     arg.Name,
					Type:     v.Value.ExpectedType,
					Position: v.Position,
				}, v.Value, vars, checkRequired)
				if err != nil {
					return nil, err
				}
				if v != nil {
					vv = append(vv, v)
				}
			}
			return vv, nil
		}
		vv := map[string]any{}
		for _, f := range def.Fields {
			v, err := ParseArgumentValue(ctx, defs, &ast.ArgumentDefinition{
				Name:     f.Name,
				Type:     f.Type,
				Position: f.Position,
			}, value.Children.ForName(f.Name), vars, checkRequired)
			if err != nil {
				return nil, err
			}
			if v != nil {
				vv[f.Name] = v
			}
		}
		return vv, nil
	}
	val, err := value.Value(vars)
	if err != nil {
		return nil, err
	}
	if arg.Type.NamedType != "" {
		val, err = ParseScalarValue(arg.Type.Name(), val)
		if err != nil {
			return nil, err
		}
		if err := checkDim(val, arg.Directives.ForName(base.FieldDimDirectiveName)); err != nil {
			return nil, err
		}
		return val, nil
	}
	return ParseScalarArray(arg.Type.Name(), val)
}

// ParseDataAsInputObject recursively parses map data into a validated input object,
// resolving scalar types along the way.
func ParseDataAsInputObject(ctx context.Context, defs base.DefinitionsSource, inputType *ast.Type, data any, checkRequired bool) (any, error) {
	if data == nil {
		return nil, nil
	}
	if inputType.NamedType == "" {
		vv, ok := data.([]any)
		if !ok {
			return nil, ErrorPosf(inputType.Position, "expected array of objects")
		}
		var out []any
		for _, v := range vv {
			o, err := ParseDataAsInputObject(ctx, defs, inputType.Elem, v, checkRequired)
			if err != nil {
				return nil, err
			}
			out = append(out, o)
		}
		return out, nil
	}
	def := defs.ForName(ctx, inputType.Name())
	vv, ok := data.(map[string]any)
	if !ok {
		return nil, ErrorPosf(inputType.Position, "expected object")
	}
	out := map[string]any{}
	for _, f := range def.Fields {
		v, ok := vv[f.Name]
		if f.Type.NonNull && (!ok || v == nil) && checkRequired {
			return nil, ErrorPosf(inputType.Position, "field %s.%s is required", def.Name, f.Name)
		}
		if !ok {
			continue
		}
		if !IsScalarType(f.Type.Name()) {
			if v == nil {
				continue
			}
			val, err := ParseDataAsInputObject(ctx, defs, f.Type, v, checkRequired)
			if err != nil {
				return nil, err
			}
			out[f.Name] = val
			continue
		}
		parsed := v
		var err error
		if f.Type.NamedType != "" {
			parsed, err = ParseScalarValue(f.Type.Name(), parsed)
			if err != nil {
				return nil, err
			}
		}
		if f.Type.NamedType == "" {
			parsed, err = ParseScalarArray(f.Type.Name(), parsed)
			if err != nil {
				return nil, err
			}
		}
		if err := checkDim(parsed, f.Directives.ForName(base.FieldDimDirectiveName)); err != nil {
			return nil, err
		}
		out[f.Name] = parsed
	}
	return out, nil
}

// JSONTypeHint returns the JSON extraction type hint for a type name and whether
// the type is known. Delegates to the scalar type registry.
var JSONTypeHint = types.JSONTypeHintWithOk

func checkDim(val any, dim *ast.Directive) error {
	if dim == nil {
		return nil
	}
	if dim.Arguments.ForName(base.ArgLen).Value.Raw == "" {
		return ErrorPosf(dim.Position, "missing length argument")
	}
	d, err := dim.Arguments.ForName(base.ArgLen).Value.Value(nil)
	if err != nil {
		return err
	}
	di, ok := d.(int64)
	if !ok || di <= 0 {
		return ErrorPosf(dim.Position, "invalid length argument: %v", d)
	}
	switch v := val.(type) {
	case types.Dimensional:
		if v.Len() != d {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", v.Len(), d)
		}
	case []any:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	case []float64:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	case []string:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	case []int64:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	default:
	}
	return nil
}
