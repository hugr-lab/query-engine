package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*InternalExtensionMerger)(nil)

// InternalExtensionMerger merges source extensions into source definitions
// during the PREPARE phase. This handles "extend type Foo { ... }" within
// the same catalog where Foo is also defined.
//
// Merge rules (matching old compiler behavior from pkg/compiler/validate.go):
//   - Stub fields (_stub, _placeholder) from extensions are skipped
//   - New fields are appended to the definition
//   - If a field already exists: its type, arguments, position, and default value
//     are replaced; directives are appended; description is updated if non-empty
//   - Directives, enum values, and interfaces from the extension are appended
//
// When the extension targets a type NOT in the source but present in the provider
// (target schema), e.g. "extend type Function { ... }", the extension is promoted
// to a source-level definition via ctx.PromoteToSource so that GENERATE-phase
// DefinitionRules (like FunctionRule) can process it.
type InternalExtensionMerger struct{}

func (r *InternalExtensionMerger) Name() string     { return "InternalExtensionMerger" }
func (r *InternalExtensionMerger) Phase() base.Phase { return base.PhasePrepare }

func (r *InternalExtensionMerger) ProcessAll(ctx base.CompilationContext) error {
	source, ok := ctx.Source().(base.ExtensionsSource)
	if !ok {
		return nil
	}

	for ext := range source.Extensions(ctx.Context()) {
		def := ctx.Source().ForName(ctx.Context(), ext.Name)
		if def != nil {
			// Extension targets a source definition — merge in place
			mergeExtensionIntoDef(def, ext)
			continue
		}

		// Function/MutationFunction: always promote to source so FunctionRule
		// can process them. These are well-known system types that exist even
		// when no provider is available (single-catalog compilation).
		if ext.Name == "Function" || ext.Name == "MutationFunction" {
			promoted := &ast.Definition{
				Kind:        ext.Kind,
				Name:        ext.Name,
				Fields:      ext.Fields,
				Directives:  ext.Directives,
				Position:    ext.Position,
				Description: ext.Description,
				Interfaces:  ext.Interfaces,
				EnumValues:  ext.EnumValues,
				Types:       ext.Types,
			}
			ctx.PromoteToSource(promoted)
			continue
		}

		// Not in source — check the provider (target schema)
		provDef := ctx.LookupType(ext.Name)
		if provDef == nil {
			continue // unknown type — cross-catalog or not yet compiled
		}

		// All other provider types: pass through as extensions so Provider.Update()
		// merges the fields. AddExtension() handles @dependency tagging when IsExtension=true.
		// Filter out _stub/_placeholder fields before passing to AddExtension.
		ctx.AddExtension(filterStubFields(ext))
	}
	return nil
}

// filterStubFields returns a copy of the definition with _stub/_placeholder fields removed.
// Returns the original definition unchanged if no stub fields are present.
func filterStubFields(ext *ast.Definition) *ast.Definition {
	var fields ast.FieldList
	for _, f := range ext.Fields {
		if f.Name == "_stub" || f.Name == "_placeholder" {
			continue
		}
		fields = append(fields, f)
	}
	if len(fields) == len(ext.Fields) {
		return ext
	}
	cp := *ext
	cp.Fields = fields
	return &cp
}

// mergeExtensionIntoDef merges an extension's fields, directives, enum values,
// and interfaces into the target definition.
func mergeExtensionIntoDef(def *ast.Definition, ext *ast.Definition) {
	// Merge fields
	for _, ef := range ext.Fields {
		if ef.Name == "_stub" || ef.Name == "_placeholder" {
			continue
		}
		existing := def.Fields.ForName(ef.Name)
		if existing == nil {
			// New field — append
			def.Fields = append(def.Fields, ef)
			continue
		}
		// Existing field — update type, args, position, defaults; append directives
		existing.Type = ef.Type
		existing.Directives = append(existing.Directives, ef.Directives...)
		existing.Position = ef.Position
		existing.Arguments = ef.Arguments
		existing.DefaultValue = ef.DefaultValue
		if ef.Description != "" {
			existing.Description = ef.Description
		}
	}

	// Merge directives
	def.Directives = append(def.Directives, ext.Directives...)

	// Merge enum values
	def.EnumValues = append(def.EnumValues, ext.EnumValues...)

	// Merge interfaces
	def.Interfaces = append(def.Interfaces, ext.Interfaces...)
}
