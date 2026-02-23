package rules

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*ModuleAssembler)(nil)

// ModuleAssembler creates module type hierarchy from ObjectInfo.Module values
// and moves per-object query/mutation fields into module-scoped types.
type ModuleAssembler struct{}

func (r *ModuleAssembler) Name() string     { return "ModuleAssembler" }
func (r *ModuleAssembler) Phase() base.Phase { return base.PhaseAssemble }

func (r *ModuleAssembler) ProcessAll(ctx base.CompilationContext) error {
	// Collect unique modules
	modules := make(map[string]bool)
	for _, info := range ctx.Objects() {
		if info.Module != "" {
			modules[info.Module] = true
		}
	}

	if len(modules) == 0 {
		return nil
	}

	pos := compiledPos("module")

	// Create module types
	for mod := range modules {
		// Module type name: _module_<mod> with dots replaced by underscores
		modTypeName := "_module_" + strings.ReplaceAll(mod, ".", "_")

		// Create module query type
		queryModType := &ast.Definition{
			Kind:     ast.Object,
			Name:     modTypeName + "_query",
			Position: pos,
			Directives: ast.DirectiveList{
				{Name: "system", Position: pos},
				{Name: "module_root", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "QUERY", Kind: ast.EnumValue, Position: pos}, Position: pos},
				}, Position: pos},
				{Name: "if_not_exists", Position: pos},
			},
			Fields: ast.FieldList{
				{Name: "_stub", Type: ast.NamedType("String", pos), Position: pos},
			},
		}
		ctx.AddDefinition(queryModType)

		// Create module mutation type
		mutModType := &ast.Definition{
			Kind:     ast.Object,
			Name:     modTypeName + "_mutation",
			Position: pos,
			Directives: ast.DirectiveList{
				{Name: "system", Position: pos},
				{Name: "module_root", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "MUTATION", Kind: ast.EnumValue, Position: pos}, Position: pos},
				}, Position: pos},
				{Name: "if_not_exists", Position: pos},
			},
			Fields: ast.FieldList{
				{Name: "_stub", Type: ast.NamedType("String", pos), Position: pos},
			},
		}
		ctx.AddDefinition(mutModType)

		// Register module query field on root Query
		modField := &ast.FieldDefinition{
			Name:     mod,
			Type:     ast.NamedType(queryModType.Name, pos),
			Position: pos,
		}
		ctx.RegisterQueryFields("_module_"+mod, []*ast.FieldDefinition{modField})

		// Register module mutation field on root Mutation
		mutField := &ast.FieldDefinition{
			Name:     mod,
			Type:     ast.NamedType(mutModType.Name, pos),
			Position: pos,
		}
		ctx.RegisterMutationFields("_module_"+mod, []*ast.FieldDefinition{mutField})
	}

	// Now assign query/mutation fields to module types instead of root.
	// For each object with a module, move its fields to the module type extension.
	for name, info := range ctx.Objects() {
		if info.Module == "" {
			continue
		}
		modTypeName := "_module_" + strings.ReplaceAll(info.Module, ".", "_")

		// Move query fields to module query type
		if qFields := ctx.QueryFields()[name]; len(qFields) > 0 {
			ext := &ast.Definition{
				Kind:     ast.Object,
				Name:     modTypeName + "_query",
				Position: pos,
				Fields:   qFields,
			}
			ctx.AddExtension(ext)
			// Remove from root-level query fields
			delete(ctx.QueryFields(), name)
		}

		// Move mutation fields to module mutation type
		if mFields := ctx.MutationFields()[name]; len(mFields) > 0 {
			ext := &ast.Definition{
				Kind:     ast.Object,
				Name:     modTypeName + "_mutation",
				Position: pos,
				Fields:   mFields,
			}
			ctx.AddExtension(ext)
			delete(ctx.MutationFields(), name)
		}
	}

	return nil
}
