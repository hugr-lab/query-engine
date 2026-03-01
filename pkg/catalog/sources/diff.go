package sources

import (
	"context"
	"iter"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// DiffSchemas compares two schema snapshots and produces incremental DDL
// changes suitable for Compiler.CompileChanges().
//
// Types in newSchema but not oldSchema → add definition (no directive).
// Types in oldSchema but not newSchema → @drop definition.
// Types in both with different fields → field-level extensions:
//   - Field in new but not old → extend type X { newField: Type }
//   - Field in old but not new → extend type X { oldField: Boolean @drop }
//   - Field in both, different → extend type X { field: NewType @replace }
//
// Types in both with different directives → extension with @drop_directive + new directives.
// Identical types → no output.
//
// The result implements both DefinitionsSource (for type adds/drops) and
// ExtensionsSource (for field-level changes).
func DiffSchemas(ctx context.Context, oldSchema, newSchema base.DefinitionsSource) *DiffResult {
	// Index old definitions by name.
	oldDefs := make(map[string]*ast.Definition)
	for def := range oldSchema.Definitions(ctx) {
		oldDefs[def.Name] = def
	}

	result := &DiffResult{}
	seen := make(map[string]bool)
	pos := base.CompiledPos("diff")

	// Walk new schema: additions and field-level changes.
	for def := range newSchema.Definitions(ctx) {
		seen[def.Name] = true
		oldDef, exists := oldDefs[def.Name]
		if !exists {
			// New type — add as-is.
			result.defs = append(result.defs, def)
			continue
		}
		if definitionsEqual(oldDef, def) {
			continue // Identical — skip.
		}

		// Diff fields.
		diffFields(result, def.Kind, def.Name, oldDef.Fields, def.Fields, pos)

		// Diff directives.
		diffDirectives(result, def.Kind, def.Name, oldDef.Directives, def.Directives, pos)
	}

	// Walk old schema: drops.
	for name, def := range oldDefs {
		if seen[name] {
			continue
		}
		result.defs = append(result.defs, &ast.Definition{
			Kind: def.Kind,
			Name: name,
			Directives: ast.DirectiveList{
				{Name: base.DropDirectiveName, Position: pos},
			},
			Position: pos,
			Fields: ast.FieldList{
				{Name: "_stub", Type: ast.NamedType("Boolean", pos), Position: pos},
			},
		})
	}

	return result
}

// diffFields emits field-level extension entries for added/dropped/replaced fields.
func diffFields(result *DiffResult, kind ast.DefinitionKind, typeName string, oldFields, newFields ast.FieldList, pos *ast.Position) {
	oldByName := make(map[string]*ast.FieldDefinition, len(oldFields))
	for _, f := range oldFields {
		oldByName[f.Name] = f
	}

	seenFields := make(map[string]bool)

	// Fields in new: additions and replacements.
	for _, nf := range newFields {
		seenFields[nf.Name] = true
		of, exists := oldByName[nf.Name]
		if !exists {
			// Added field.
			result.exts = append(result.exts, &ast.Definition{
				Kind:     kind,
				Name:     typeName,
				Position: pos,
				Fields:   ast.FieldList{nf},
			})
			continue
		}
		if !fieldsEqual(of, nf) {
			// Changed field — emit @replace.
			replacedField := *nf
			replacedField.Directives = append(
				make(ast.DirectiveList, 0, len(nf.Directives)+1),
				nf.Directives...,
			)
			replacedField.Directives = append(replacedField.Directives, &ast.Directive{
				Name:     base.ReplaceDirectiveName,
				Position: pos,
			})
			result.exts = append(result.exts, &ast.Definition{
				Kind:     kind,
				Name:     typeName,
				Position: pos,
				Fields:   ast.FieldList{&replacedField},
			})
		}
	}

	// Fields in old but not new: drops.
	for _, of := range oldFields {
		if seenFields[of.Name] {
			continue
		}
		dropDirs := ast.DirectiveList{
			{Name: base.DropDirectiveName, Position: pos},
		}
		// Preserve @module on drop fields so the incremental compiler
		// can locate the field in the correct module type (e.g., for functions).
		if modDir := of.Directives.ForName(base.ModuleDirectiveName); modDir != nil {
			dropDirs = append(ast.DirectiveList{modDir}, dropDirs...)
		}
		result.exts = append(result.exts, &ast.Definition{
			Kind:     kind,
			Name:     typeName,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name:       of.Name,
					Type:       ast.NamedType("Boolean", pos),
					Directives: dropDirs,
					Position:   pos,
				},
			},
		})
	}
}

// diffDirectives emits extension directives for added/dropped directives on a type.
func diffDirectives(result *DiffResult, kind ast.DefinitionKind, typeName string, oldDirs, newDirs ast.DirectiveList, pos *ast.Position) {
	oldStrs := make(map[string]*ast.Directive, len(oldDirs))
	for _, d := range oldDirs {
		oldStrs[directiveString(d)] = d
	}

	newStrs := make(map[string]*ast.Directive, len(newDirs))
	for _, d := range newDirs {
		newStrs[directiveString(d)] = d
	}

	var extDirs ast.DirectiveList

	// Directives in old but not new → @drop_directive.
	for str, d := range oldStrs {
		if _, exists := newStrs[str]; !exists {
			extDirs = append(extDirs, dropDirectiveFor(d, pos))
		}
	}

	// Directives in new but not old → add.
	for str, d := range newStrs {
		if _, exists := oldStrs[str]; !exists {
			extDirs = append(extDirs, d)
		}
	}

	if len(extDirs) > 0 {
		result.exts = append(result.exts, &ast.Definition{
			Kind:       kind,
			Name:       typeName,
			Position:   pos,
			Directives: extDirs,
		})
	}
}

// dropDirectiveFor creates a @drop_directive(name: X, match: {arg1: val1, ...}) directive.
func dropDirectiveFor(d *ast.Directive, pos *ast.Position) *ast.Directive {
	args := ast.ArgumentList{
		{Name: "name", Value: &ast.Value{Raw: d.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
	}
	// Add match argument if the directive has arguments.
	if len(d.Arguments) > 0 {
		var children ast.ChildValueList
		for _, arg := range d.Arguments {
			if arg.Value != nil {
				children = append(children, &ast.ChildValue{
					Name:  arg.Name,
					Value: arg.Value,
				})
			}
		}
		if len(children) > 0 {
			args = append(args, &ast.Argument{
				Name: "match",
				Value: &ast.Value{
					Kind:     ast.ObjectValue,
					Children: children,
					Position: pos,
				},
				Position: pos,
			})
		}
	}
	return &ast.Directive{
		Name:      base.DropDirectiveDirectiveName,
		Arguments: args,
		Position:  pos,
	}
}

// definitionsEqual performs a structural comparison of two definitions.
// Returns true only if kind, fields, and user directives are identical.
func definitionsEqual(a, b *ast.Definition) bool {
	if a.Kind != b.Kind {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i, af := range a.Fields {
		bf := b.Fields[i]
		if !fieldsEqual(af, bf) {
			return false
		}
	}
	return directivesEqual(a.Directives, b.Directives)
}

// fieldsEqual compares two field definitions structurally.
func fieldsEqual(a, b *ast.FieldDefinition) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type.String() != b.Type.String() {
		return false
	}
	return directivesEqual(a.Directives, b.Directives)
}

// directivesEqual compares two directive lists by serialized form.
func directivesEqual(a, b ast.DirectiveList) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if directiveString(a[i]) != directiveString(b[i]) {
			return false
		}
	}
	return true
}

// directiveString returns a canonical string form: name(arg1:val1,arg2:val2).
func directiveString(d *ast.Directive) string {
	var sb strings.Builder
	sb.WriteString(d.Name)
	if len(d.Arguments) > 0 {
		sb.WriteByte('(')
		for i, arg := range d.Arguments {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(arg.Name)
			sb.WriteByte(':')
			if arg.Value != nil {
				sb.WriteString(arg.Value.String())
			}
		}
		sb.WriteByte(')')
	}
	return sb.String()
}

// DiffResult implements both DefinitionsSource and ExtensionsSource for diff output.
type DiffResult struct {
	defs []*ast.Definition // type adds and drops
	exts []*ast.Definition // field-level extensions
}

func (d *DiffResult) ForName(_ context.Context, name string) *ast.Definition {
	for _, def := range d.defs {
		if def.Name == name {
			return def
		}
	}
	return nil
}

func (d *DiffResult) DirectiveForName(_ context.Context, _ string) *ast.DirectiveDefinition {
	return nil
}

func (d *DiffResult) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, def := range d.defs {
			if !yield(def) {
				return
			}
		}
	}
}

func (d *DiffResult) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(_ func(string, *ast.DirectiveDefinition) bool) {}
}

// Extensions returns all field-level extension definitions.
func (d *DiffResult) Extensions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, ext := range d.exts {
			if !yield(ext) {
				return
			}
		}
	}
}

// DefinitionExtensions returns extensions for a specific type name.
func (d *DiffResult) DefinitionExtensions(_ context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, ext := range d.exts {
			if ext.Name == name {
				if !yield(ext) {
					return
				}
			}
		}
	}
}
