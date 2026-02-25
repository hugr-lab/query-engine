package base

import "github.com/vektah/gqlparser/v2/ast"

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
