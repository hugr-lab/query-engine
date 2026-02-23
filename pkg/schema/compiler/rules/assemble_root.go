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
	// Add function fields registered by rules
	queryFields = append(queryFields, ctx.FunctionFields()...)

	// Add "function" gateway field on Query if Function type was emitted
	if ctx.LookupType("Function") != nil {
		queryFields = append(queryFields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType("Function", pos),
			Position:    pos,
		})
	}

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
	// Add function mutation fields registered by rules
	mutFields = append(mutFields, ctx.FunctionMutationFields()...)

	// Add "function" gateway field on Mutation if MutationFunction type was emitted
	if ctx.LookupType("MutationFunction") != nil {
		mutFields = append(mutFields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType("MutationFunction", pos),
			Position:    pos,
		})
	}

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
