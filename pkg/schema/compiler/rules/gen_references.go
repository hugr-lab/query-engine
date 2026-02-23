package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*ReferencesRule)(nil)

type ReferencesRule struct{}

func (r *ReferencesRule) Name() string     { return "ReferencesRule" }
func (r *ReferencesRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *ReferencesRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("references") != nil
}

func (r *ReferencesRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	info := ctx.GetObject(def.Name)
	if info == nil {
		info = &base.ObjectInfo{Name: def.Name, OriginalName: def.Name}
	}
	pos := compiledPos(def.Name)

	for _, dir := range def.Directives.ForNames("references") {
		refName := base.DirectiveArgString(dir, "references_name")
		if refName == "" {
			continue
		}

		// Look up the target (referenced) type.
		// Check output and target schema first, then source (the target definition
		// may not have been added to output yet during single-pass iteration).
		targetDef := ctx.LookupType(refName)
		if targetDef == nil {
			targetDef = ctx.Source().ForName(ctx.Context(), refName)
		}
		if targetDef == nil {
			continue
		}
		targetInfo := ctx.GetObject(refName)
		if targetInfo == nil {
			targetInfo = &base.ObjectInfo{Name: refName, OriginalName: refName}
		}

		// Extract source fields
		sourceFieldsArg := dir.Arguments.ForName("source_fields")
		if sourceFieldsArg == nil || sourceFieldsArg.Value == nil {
			continue
		}
		var sourceFields []string
		for _, child := range sourceFieldsArg.Value.Children {
			if child.Value != nil {
				sourceFields = append(sourceFields, child.Value.Raw)
			}
		}

		// Extract referenced fields
		refFieldsArg := dir.Arguments.ForName("references_fields")
		if refFieldsArg == nil || refFieldsArg.Value == nil {
			continue
		}
		var refFields []string
		for _, child := range refFieldsArg.Value.Children {
			if child.Value != nil {
				refFields = append(refFields, child.Value.Raw)
			}
		}

		if len(sourceFields) == 0 || len(refFields) == 0 {
			continue
		}

		// Generate forward reference field on source object
		// Extends the source type with a field pointing to the target type
		forwardExt := &ast.Definition{
			Kind:     ast.Object,
			Name:     def.Name,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name:     refName,
					Type:     ast.NamedType(refName, pos),
					Position: pos,
					Directives: ast.DirectiveList{
						{Name: "reference", Arguments: ast.ArgumentList{
							{Name: "target", Value: &ast.Value{Raw: refName, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
					},
				},
			},
		}
		ctx.AddExtension(forwardExt)

		// Generate back-reference field on target object
		// The target filter name follows the convention: <TargetName>Filter
		filterName := def.Name + "Filter"
		backRefExt := &ast.Definition{
			Kind:     ast.Object,
			Name:     refName,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name: def.Name,
					Type: ast.NonNullListType(ast.NamedType(def.Name, pos), pos),
					Arguments: ast.ArgumentDefinitionList{
						{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
						{Name: "limit", Type: ast.NamedType("Int", pos), Position: pos},
						{Name: "offset", Type: ast.NamedType("Int", pos), Position: pos},
					},
					Position: pos,
					Directives: ast.DirectiveList{
						{Name: "back_reference", Arguments: ast.ArgumentList{
							{Name: "source", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
					},
				},
			},
		}
		ctx.AddExtension(backRefExt)
	}

	return nil
}
