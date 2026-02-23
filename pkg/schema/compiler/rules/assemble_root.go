package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*RootTypeAssembler)(nil)

// RootTypeAssembler creates/extends Query and Mutation root types
// from the remaining collected fields after module assembly.
type RootTypeAssembler struct{}

func (r *RootTypeAssembler) Name() string     { return "RootTypeAssembler" }
func (r *RootTypeAssembler) Phase() base.Phase { return base.PhaseAssemble }

func (r *RootTypeAssembler) ProcessAll(ctx base.CompilationContext) error {
	pos := compiledPos("root")

	// Collect all remaining query fields into Query extension
	var queryFields []*ast.FieldDefinition
	for _, fields := range ctx.QueryFields() {
		queryFields = append(queryFields, fields...)
	}
	// Add function fields
	queryFields = append(queryFields, ctx.FunctionFields()...)

	if len(queryFields) > 0 {
		ext := &ast.Definition{
			Kind:     ast.Object,
			Name:     "Query",
			Position: pos,
			Fields:   queryFields,
		}
		ctx.AddExtension(ext)
	}

	// Collect all remaining mutation fields into Mutation extension
	var mutFields []*ast.FieldDefinition
	for _, fields := range ctx.MutationFields() {
		mutFields = append(mutFields, fields...)
	}
	// Add function mutation fields
	mutFields = append(mutFields, ctx.FunctionMutationFields()...)

	if len(mutFields) > 0 {
		// Create Mutation type if it doesn't exist
		if ctx.LookupType("Mutation") == nil {
			mutType := &ast.Definition{
				Kind:     ast.Object,
				Name:     "Mutation",
				Position: pos,
				Directives: ast.DirectiveList{
					{Name: "system", Position: pos},
					{Name: "if_not_exists", Position: pos},
				},
				Fields: ast.FieldList{
					{Name: "_stub", Type: ast.NamedType("String", pos), Position: pos},
				},
			}
			ctx.AddDefinition(mutType)
		}

		ext := &ast.Definition{
			Kind:     ast.Object,
			Name:     "Mutation",
			Position: pos,
			Fields:   mutFields,
		}
		ctx.AddExtension(ext)
	}

	return nil
}
