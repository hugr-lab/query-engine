package compare

import (
	"fmt"
	"maps"
	"sort"

	"github.com/vektah/gqlparser/v2/ast"
)

// Compare structurally compares two schemas and returns differences.
// The old schema is treated as the reference; the new schema is checked against it.
func Compare(old, new *ast.Schema, opts ...CompareOption) *Result {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}

	var allDiffs []Diff

	// Build type maps
	oldTypes := typeMap(old)
	newTypes := typeMap(new)

	// Compare types: check all types in old schema
	for name, oldDef := range oldTypes {
		if cfg.shouldSkip(name) {
			continue
		}
		newDef, ok := newTypes[name]
		if !ok {
			allDiffs = append(allDiffs, Diff{
				Path:    "types." + name,
				Kind:    DiffMissing,
				Message: fmt.Sprintf("type %q missing in new schema", name),
			})
			continue
		}
		allDiffs = append(allDiffs, compareDefinitions("types."+name, oldDef, newDef, cfg)...)
	}

	// Check for extra types in new schema
	if !cfg.allowExtraTypes {
		for name := range newTypes {
			if cfg.shouldSkip(name) {
				continue
			}
			if _, ok := oldTypes[name]; !ok {
				allDiffs = append(allDiffs, Diff{
					Path:    "types." + name,
					Kind:    DiffExtra,
					Message: fmt.Sprintf("type %q extra in new schema", name),
				})
			}
		}
	}

	// Sort diffs by path for deterministic output
	sort.Slice(allDiffs, func(i, j int) bool {
		return allDiffs[i].Path < allDiffs[j].Path
	})

	// Separate known issues from real diffs
	result := &Result{}
	for _, d := range allDiffs {
		if cfg.knownIssues[d.Path] {
			result.KnownIssues = append(result.KnownIssues, d)
		} else {
			result.Diffs = append(result.Diffs, d)
		}
	}

	return result
}

func (cfg *compareConfig) shouldSkip(name string) bool {
	if cfg.skipTypes[name] {
		return true
	}
	if cfg.skipSystemTypes && len(name) > 2 && name[0] == '_' && name[1] == '_' {
		return true
	}
	return false
}

func typeMap(s *ast.Schema) map[string]*ast.Definition {
	m := make(map[string]*ast.Definition)
	if s == nil {
		return m
	}
	maps.Copy(m, s.Types)
	return m
}

func compareDefinitions(basePath string, old, new *ast.Definition, cfg *compareConfig) []Diff {
	var diffs []Diff

	// Compare kind
	if old.Kind != new.Kind {
		diffs = append(diffs, Diff{
			Path:    basePath + ".kind",
			Kind:    DiffChanged,
			Message: fmt.Sprintf("kind changed from %s to %s", old.Kind, new.Kind),
		})
	}

	// Compare description
	if !cfg.ignoreDescriptions && old.Description != new.Description {
		diffs = append(diffs, Diff{
			Path:    basePath + ".description",
			Kind:    DiffChanged,
			Message: "description changed",
		})
	}

	// Compare fields
	diffs = append(diffs, compareFields(basePath+".fields", old.Fields, new.Fields, cfg)...)

	// Compare directives
	diffs = append(diffs, compareDirectives(basePath+".directives", old.Directives, new.Directives, cfg)...)

	// Compare enum values
	diffs = append(diffs, compareEnumValues(basePath+".values", old.EnumValues, new.EnumValues)...)

	// Compare interfaces
	diffs = append(diffs, compareInterfaces(basePath+".interfaces", old.Interfaces, new.Interfaces)...)

	return diffs
}

func compareFields(basePath string, old, new ast.FieldList, cfg *compareConfig) []Diff {
	var diffs []Diff
	oldMap := fieldMap(old)
	newMap := fieldMap(new)

	for name, oldF := range oldMap {
		newF, ok := newMap[name]
		if !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffMissing,
				Message: fmt.Sprintf("field %q missing in new", name),
			})
			continue
		}
		diffs = append(diffs, compareField(basePath+"."+name, oldF, newF, cfg)...)
	}
	for name := range newMap {
		if _, ok := oldMap[name]; !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffExtra,
				Message: fmt.Sprintf("field %q extra in new", name),
			})
		}
	}
	return diffs
}

func compareField(basePath string, old, new *ast.FieldDefinition, cfg *compareConfig) []Diff {
	var diffs []Diff

	// Compare type
	oldType := typeString(old.Type)
	newType := typeString(new.Type)
	if oldType != newType {
		diffs = append(diffs, Diff{
			Path:    basePath + ".type",
			Kind:    DiffChanged,
			Message: fmt.Sprintf("type changed from %s to %s", oldType, newType),
		})
	}

	// Compare description
	if !cfg.ignoreDescriptions && old.Description != new.Description {
		diffs = append(diffs, Diff{
			Path:    basePath + ".description",
			Kind:    DiffChanged,
			Message: "description changed",
		})
	}

	// Compare directives
	diffs = append(diffs, compareDirectives(basePath+".directives", old.Directives, new.Directives, cfg)...)

	// Compare arguments
	diffs = append(diffs, compareArgDefs(basePath+".args", old.Arguments, new.Arguments)...)

	return diffs
}

func compareDirectives(basePath string, old, new ast.DirectiveList, cfg *compareConfig) []Diff {
	var diffs []Diff
	oldMap := directiveMap(old)
	newMap := directiveMap(new)

	for name, oldDir := range oldMap {
		if cfg.ignoreDirectives[name] {
			continue
		}
		newDir, ok := newMap[name]
		if !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffMissing,
				Message: fmt.Sprintf("directive @%s missing in new", name),
			})
			continue
		}
		if !cfg.ignoreDirectiveArgs[name] {
			diffs = append(diffs, compareDirectiveArgs(basePath+"."+name, oldDir, newDir)...)
		}
	}
	for name := range newMap {
		if cfg.ignoreDirectives[name] {
			continue
		}
		if _, ok := oldMap[name]; !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffExtra,
				Message: fmt.Sprintf("directive @%s extra in new", name),
			})
		}
	}
	return diffs
}

func compareDirectiveArgs(basePath string, old, new *ast.Directive) []Diff {
	var diffs []Diff
	oldArgs := argMap(old.Arguments)
	newArgs := argMap(new.Arguments)

	for name, oldArg := range oldArgs {
		newArg, ok := newArgs[name]
		if !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + ".args." + name,
				Kind:    DiffMissing,
				Message: fmt.Sprintf("argument %q missing in new", name),
			})
			continue
		}
		if oldArg.Value != nil && newArg.Value != nil && oldArg.Value.Raw != newArg.Value.Raw {
			diffs = append(diffs, Diff{
				Path:    basePath + ".args." + name,
				Kind:    DiffChanged,
				Message: fmt.Sprintf("value changed from %q to %q", oldArg.Value.Raw, newArg.Value.Raw),
			})
		}
	}
	for name := range newArgs {
		if _, ok := oldArgs[name]; !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + ".args." + name,
				Kind:    DiffExtra,
				Message: fmt.Sprintf("argument %q extra in new", name),
			})
		}
	}
	return diffs
}

func compareEnumValues(basePath string, old, new ast.EnumValueList) []Diff {
	var diffs []Diff
	oldSet := make(map[string]bool)
	for _, v := range old {
		oldSet[v.Name] = true
	}
	newSet := make(map[string]bool)
	for _, v := range new {
		newSet[v.Name] = true
	}

	for name := range oldSet {
		if !newSet[name] {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffMissing,
				Message: fmt.Sprintf("enum value %q missing in new", name),
			})
		}
	}
	for name := range newSet {
		if !oldSet[name] {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffExtra,
				Message: fmt.Sprintf("enum value %q extra in new", name),
			})
		}
	}
	return diffs
}

func compareInterfaces(basePath string, old, new []string) []Diff {
	var diffs []Diff
	oldSet := make(map[string]bool)
	for _, i := range old {
		oldSet[i] = true
	}
	newSet := make(map[string]bool)
	for _, i := range new {
		newSet[i] = true
	}

	for name := range oldSet {
		if !newSet[name] {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffMissing,
				Message: fmt.Sprintf("interface %q missing in new", name),
			})
		}
	}
	for name := range newSet {
		if !oldSet[name] {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffExtra,
				Message: fmt.Sprintf("interface %q extra in new", name),
			})
		}
	}
	return diffs
}

func compareArgDefs(basePath string, old, new ast.ArgumentDefinitionList) []Diff {
	var diffs []Diff
	oldMap := make(map[string]*ast.ArgumentDefinition)
	for _, a := range old {
		oldMap[a.Name] = a
	}
	newMap := make(map[string]*ast.ArgumentDefinition)
	for _, a := range new {
		newMap[a.Name] = a
	}

	for name, oldArg := range oldMap {
		newArg, ok := newMap[name]
		if !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffMissing,
				Message: fmt.Sprintf("argument %q missing in new", name),
			})
			continue
		}
		oldType := typeString(oldArg.Type)
		newType := typeString(newArg.Type)
		if oldType != newType {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name + ".type",
				Kind:    DiffChanged,
				Message: fmt.Sprintf("type changed from %s to %s", oldType, newType),
			})
		}
	}
	for name := range newMap {
		if _, ok := oldMap[name]; !ok {
			diffs = append(diffs, Diff{
				Path:    basePath + "." + name,
				Kind:    DiffExtra,
				Message: fmt.Sprintf("argument %q extra in new", name),
			})
		}
	}
	return diffs
}

// Helper functions

func fieldMap(fields ast.FieldList) map[string]*ast.FieldDefinition {
	m := make(map[string]*ast.FieldDefinition, len(fields))
	for _, f := range fields {
		m[f.Name] = f
	}
	return m
}

func directiveMap(dirs ast.DirectiveList) map[string]*ast.Directive {
	m := make(map[string]*ast.Directive, len(dirs))
	for _, d := range dirs {
		m[d.Name] = d
	}
	return m
}

func argMap(args ast.ArgumentList) map[string]*ast.Argument {
	m := make(map[string]*ast.Argument, len(args))
	for _, a := range args {
		m[a.Name] = a
	}
	return m
}

func typeString(t *ast.Type) string {
	if t == nil {
		return ""
	}
	return t.String()
}
