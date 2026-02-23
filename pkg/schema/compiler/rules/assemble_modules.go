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
	// Collect fields per module first, then create types with real fields
	type moduleFields struct {
		queryFields    ast.FieldList
		mutationFields ast.FieldList
	}
	modules := make(map[string]*moduleFields)

	pos := compiledPos("module")

	for name, info := range ctx.Objects() {
		if info.Module == "" {
			continue
		}

		mf := modules[info.Module]
		if mf == nil {
			mf = &moduleFields{}
			modules[info.Module] = mf
		}

		// Add @module directive on the object definition
		if def := ctx.LookupType(name); def != nil {
			def.Directives = append(def.Directives, &ast.Directive{
				Name: "module",
				Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.Module, Kind: ast.StringValue, Position: pos}, Position: pos},
				},
				Position: pos,
			})
		}

		// Collect query fields
		if qFields := ctx.QueryFields()[name]; len(qFields) > 0 {
			mf.queryFields = append(mf.queryFields, qFields...)
			delete(ctx.QueryFields(), name)
		}

		// Collect mutation fields
		if mFields := ctx.MutationFields()[name]; len(mFields) > 0 {
			mf.mutationFields = append(mf.mutationFields, mFields...)
			delete(ctx.MutationFields(), name)
		}
	}

	if len(modules) == 0 {
		return nil
	}

	// Create module types as definitions (with _stub) and add fields as extensions.
	// This supports multi-catalog: each catalog creates the module type with
	// @if_not_exists if it already exists in the provider, and adds its own
	// fields as extensions.
	for mod, mf := range modules {
		modTypeName := "_module_" + strings.ReplaceAll(mod, ".", "_")
		queryTypeName := modTypeName + "_query"
		mutTypeName := modTypeName + "_mutation"

		// Check if module types already exist in provider (multi-catalog case)
		existsInProvider := ctx.LookupType(queryTypeName) != nil

		// --- Module query type ---
		queryDirs := ast.DirectiveList{
			{Name: "module_root", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "QUERY", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
		}
		if existsInProvider {
			queryDirs = append(queryDirs, &ast.Directive{Name: "if_not_exists", Position: pos})
		}
		queryModType := &ast.Definition{
			Kind:       ast.Object,
			Name:       queryTypeName,
			Position:   pos,
			Directives: queryDirs,
		}
		ctx.AddDefinition(queryModType)

		// Add query fields as extension
		if len(mf.queryFields) > 0 {
			ctx.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     queryTypeName,
				Position: pos,
				Fields:   mf.queryFields,
			})
		}

		// --- Module mutation type ---
		mutDirs := ast.DirectiveList{
			{Name: "module_root", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "MUTATION", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
		}
		if existsInProvider {
			mutDirs = append(mutDirs, &ast.Directive{Name: "if_not_exists", Position: pos})
		}
		mutModType := &ast.Definition{
			Kind:       ast.Object,
			Name:       mutTypeName,
			Position:   pos,
			Directives: mutDirs,
		}
		ctx.AddDefinition(mutModType)

		// Add mutation fields as extension
		if len(mf.mutationFields) > 0 {
			ctx.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     mutTypeName,
				Position: pos,
				Fields:   mf.mutationFields,
			})
		}

		// Register module query field on root Query
		modField := &ast.FieldDefinition{
			Name:     mod,
			Type:     ast.NamedType(queryTypeName, pos),
			Position: pos,
		}
		ctx.RegisterQueryFields("_module_"+mod, []*ast.FieldDefinition{modField})

		// Register module mutation field on root Mutation
		mutField := &ast.FieldDefinition{
			Name:     mod,
			Type:     ast.NamedType(mutTypeName, pos),
			Position: pos,
		}
		ctx.RegisterMutationFields("_module_"+mod, []*ast.FieldDefinition{mutField})
	}

	return nil
}
