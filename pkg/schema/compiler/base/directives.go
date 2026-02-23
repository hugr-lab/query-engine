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
