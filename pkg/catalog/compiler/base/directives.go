package base

import (
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"
)

// SystemDirective is the sentinel directive marking compiler-generated definitions.
var SystemDirective = &ast.Directive{Name: "system", Position: CompiledPos("")}

// ModuleDirective creates a @module directive with the given name.
func ModuleDirective(name string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: ModuleDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: name, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// FieldGeometryInfoDirective creates a @geometry_info directive.
func FieldGeometryInfoDirective(geomType string, srid int) *ast.Directive {
	pos := CompiledPos("")
	return &ast.Directive{
		Name: FieldGeometryInfoDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "type", Value: &ast.Value{Raw: geomType, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "srid", Value: &ast.Value{Raw: strconv.Itoa(srid), Kind: ast.IntValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// FieldSqlDirective creates a @sql directive with the given expression.
func FieldSqlDirective(sql string) *ast.Directive {
	pos := CompiledPos("")
	return &ast.Directive{
		Name: FieldSqlDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "exp", Value: &ast.Value{Raw: sql, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// AddH3Directive creates an @add_h3 directive with the given parameters.
func AddH3Directive(field string, resolution int, transformFrom int, buffer float64, divideVals bool, simplify bool, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: AddH3DirectiveName,
		Arguments: []*ast.Argument{
			{Name: "res", Value: &ast.Value{Kind: ast.IntValue, Raw: strconv.Itoa(resolution), Position: pos}, Position: pos},
			{Name: "field", Value: &ast.Value{Kind: ast.StringValue, Raw: field, Position: pos}, Position: pos},
			{Name: "transform_from", Value: &ast.Value{Kind: ast.IntValue, Raw: strconv.Itoa(transformFrom), Position: pos}, Position: pos},
			{Name: "buffer", Value: &ast.Value{Kind: ast.FloatValue, Raw: strconv.FormatFloat(buffer, 'f', -1, 64), Position: pos}, Position: pos},
			{Name: "divide_values", Value: &ast.Value{Kind: ast.BooleanValue, Raw: strconv.FormatBool(divideVals), Position: pos}, Position: pos},
			{Name: "simplify", Value: &ast.Value{Kind: ast.BooleanValue, Raw: strconv.FormatBool(simplify), Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// QuerySideDirectives returns directive names that are valid on query fields.
func QuerySideDirectives() []string {
	return []string{
		"include", "skip", "defer",
		CacheDirectiveName,
		NoCacheDirectiveName,
		InvalidateCacheDirectiveName,
		StatsDirectiveName,
		WithDeletedDirectiveName,
		UnnestDirectiveName,
		AddH3DirectiveName,
		GisFeatureDirectiveName,
		NoPushdownDirectiveName,
		AtDirectiveName,
	}
}

func DefinitionDirectiveArgString(def *ast.Definition, dirName, argName string) string {
	dir := def.Directives.ForName(dirName)
	return DirectiveArgString(dir, argName)
}

func FieldDefDirectiveArgString(field *ast.FieldDefinition, dirName, argName string) string {
	dir := field.Directives.ForName(dirName)
	return DirectiveArgString(dir, argName)
}

func DirectiveArgString(dir *ast.Directive, name string) string {
	if dir == nil {
		return ""
	}
	arg := dir.Arguments.ForName(name)
	if arg == nil || arg.Value == nil || arg.Value.Raw == "" {
		return ""
	}
	return arg.Value.Raw
}

// SetDirectiveArg sets or adds a string argument on a directive.
func SetDirectiveArg(dir *ast.Directive, name, value string) {
	if a := dir.Arguments.ForName(name); a != nil {
		a.Value = &ast.Value{Kind: ast.StringValue, Raw: value, Position: dir.Position}
	} else {
		dir.Arguments = append(dir.Arguments, &ast.Argument{
			Name:     name,
			Value:    &ast.Value{Kind: ast.StringValue, Raw: value, Position: dir.Position},
			Position: dir.Position,
		})
	}
}

// DirectiveArgStrings extracts a list of string values from a directive argument.
func DirectiveArgStrings(dir *ast.Directive, name string) []string {
	if dir == nil {
		return nil
	}
	arg := dir.Arguments.ForName(name)
	if arg == nil || arg.Value == nil {
		return nil
	}
	var result []string
	for _, child := range arg.Value.Children {
		if child.Value != nil {
			result = append(result, child.Value.Raw)
		}
	}
	return result
}
