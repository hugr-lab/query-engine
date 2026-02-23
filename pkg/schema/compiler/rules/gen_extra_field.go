package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*ExtraFieldRule)(nil)

type ExtraFieldRule struct{}

func (r *ExtraFieldRule) Name() string     { return "ExtraFieldRule" }
func (r *ExtraFieldRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *ExtraFieldRule) Match(def *ast.Definition) bool {
	// Only match data objects (@table or @view) that have at least one
	// field whose scalar implements ExtraFieldProvider.
	if def.Directives.ForName("table") == nil && def.Directives.ForName("view") == nil {
		return false
	}
	return true
}

func (r *ExtraFieldRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	pos := compiledPos(def.Name)

	var extraFields ast.FieldList

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()
		s := ctx.ScalarLookup(typeName)
		if s == nil {
			continue
		}
		efp, ok := s.(types.ExtraFieldProvider)
		if !ok {
			continue
		}

		extraField := efp.GenerateExtraField(f.Name)
		if extraField == nil {
			continue
		}
		extraFields = append(extraFields, extraField)
	}

	if len(extraFields) == 0 {
		return nil
	}

	// Add extra fields as an extension on the original object
	ext := &ast.Definition{
		Kind:     ast.Object,
		Name:     def.Name,
		Position: pos,
		Fields:   extraFields,
	}
	ctx.AddExtension(ext)

	return nil
}
