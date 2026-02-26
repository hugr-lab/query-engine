package base

import (
	"slices"

	"github.com/vektah/gqlparser/v2/ast"
)

// DDL directive names
const (
	// Drop type, input, enum value, or field from the schema
	DropDirectiveName = "drop"
	// Replace type, input, enum value, or field in the schema with the new definition
	ReplaceDirectiveName = "replace"
	// Skip adding the definition if it already exists
	IfNotExistsDirectiveName = "if_not_exists"
	// Drop a specific directive usage from the target definition or field
	DropDirectiveDirectiveName = "drop_directive"
)

// Definition-level predicates

func IsDropDefinition(def *ast.Definition) bool {
	return def.Directives.ForName(DropDirectiveName) != nil
}

func IsReplaceDefinition(def *ast.Definition) bool {
	return def.Directives.ForName(ReplaceDirectiveName) != nil
}

func IsIfNotExistsDefinition(def *ast.Definition) bool {
	return def.Directives.ForName(IfNotExistsDirectiveName) != nil
}

func DropDirectiveIfExists(def *ast.Definition) bool {
	dir := def.Directives.ForName(DropDirectiveName)
	if dir == nil {
		return false
	}
	arg := dir.Arguments.ForName("if_exists")
	return arg != nil && arg.Value != nil && arg.Value.Raw == "true"
}

// Field-level predicates

func IsDropField(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(DropDirectiveName) != nil
}

func IsReplaceField(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(ReplaceDirectiveName) != nil
}

// Enum value predicates

func IsDropEnumValue(ev *ast.EnumValueDefinition) bool {
	return ev.Directives.ForName(DropDirectiveName) != nil
}

// DropDirectiveSpec describes which directive to remove from a target.
type DropDirectiveSpec struct {
	Name  string            // directive name to drop
	Match map[string]string // argument values to match (nil = drop all instances)
}

// DropDirectiveSpecs extracts all @drop_directive specs from a definition's directives.
func DropDirectiveSpecs(def *ast.Definition) []DropDirectiveSpec {
	dirs := def.Directives.ForNames(DropDirectiveDirectiveName)
	if len(dirs) == 0 {
		return nil
	}
	specs := make([]DropDirectiveSpec, 0, len(dirs))
	for _, dir := range dirs {
		spec := DropDirectiveSpec{
			Name: DirectiveArgString(dir, "name"),
		}
		if spec.Name == "" {
			continue
		}
		matchArg := dir.Arguments.ForName("match")
		if matchArg != nil && matchArg.Value != nil && len(matchArg.Value.Children) > 0 {
			spec.Match = make(map[string]string, len(matchArg.Value.Children))
			for _, child := range matchArg.Value.Children {
				if child.Value != nil {
					spec.Match[child.Name] = child.Value.Raw
				}
			}
		}
		specs = append(specs, spec)
	}
	return specs
}

// FieldDropDirectiveSpecs extracts all @drop_directive specs from a field's directives.
func FieldDropDirectiveSpecs(field *ast.FieldDefinition) []DropDirectiveSpec {
	dirs := field.Directives.ForNames(DropDirectiveDirectiveName)
	if len(dirs) == 0 {
		return nil
	}
	specs := make([]DropDirectiveSpec, 0, len(dirs))
	for _, dir := range dirs {
		spec := DropDirectiveSpec{
			Name: DirectiveArgString(dir, "name"),
		}
		if spec.Name == "" {
			continue
		}
		matchArg := dir.Arguments.ForName("match")
		if matchArg != nil && matchArg.Value != nil && len(matchArg.Value.Children) > 0 {
			spec.Match = make(map[string]string, len(matchArg.Value.Children))
			for _, child := range matchArg.Value.Children {
				if child.Value != nil {
					spec.Match[child.Name] = child.Value.Raw
				}
			}
		}
		specs = append(specs, spec)
	}
	return specs
}

// IsControlDirective returns true for DDL control directives that should be
// stripped before inserting definitions into the schema.
func IsControlDirective(name string) bool {
	switch name {
	case DropDirectiveName, ReplaceDirectiveName, IfNotExistsDirectiveName, DropDirectiveDirectiveName:
		return true
	}
	return false
}

// StripControlDirectives returns a new directive list with all DDL control directives removed.
func StripControlDirectives(dirs ast.DirectiveList) ast.DirectiveList {
	return slices.DeleteFunc(slices.Clone(dirs), func(d *ast.Directive) bool {
		return IsControlDirective(d.Name)
	})
}
