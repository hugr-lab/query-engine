package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*H3Rule)(nil)

// H3Rule extends the _h3_data_query type with fields from the _spatial extension.
// It runs after JoinSpatialRule because it copies fields from the _spatial type.
//
// Base types (_h3_data_query, _h3_query) and the h3 query field are defined in
// system_types.graphql. This rule only adds catalog-specific data fields as
// extensions so they accumulate across catalogs.
type H3Rule struct{}

func (r *H3Rule) Name() string     { return "H3Rule" }
func (r *H3Rule) Phase() base.Phase { return base.PhaseGenerate }

func (r *H3Rule) ProcessAll(ctx base.CompilationContext) error {
	// Fields are in the extension (added by JoinSpatialRule), not on the definition directly.
	spatialExt := ctx.LookupExtension("_spatial")
	if spatialExt == nil {
		return nil
	}

	pos := compiledPos("h3")

	// Build _h3_data_query fields from this catalog's spatial objects
	var h3DataFields ast.FieldList
	for _, f := range spatialExt.Fields {
		if f.Name == "h3" {
			continue
		}
		field := &ast.FieldDefinition{
			Name:        f.Name,
			Description: f.Description,
			Type:        copyFieldType(f.Type, pos),
			Arguments:   copyArgDefs(f.Arguments, pos),
			Directives:  copyFieldDirectives(f.Directives, pos),
			Position:    pos,
		}
		// Append H3-specific arguments
		field.Arguments = append(field.Arguments,
			&ast.ArgumentDefinition{
				Name:        "transform_from",
				Description: "Reproject geometry from SRID before aggregation",
				Type:        ast.NamedType("Int", pos),
				Position:    pos,
			},
			&ast.ArgumentDefinition{
				Name:        "buffer",
				Description: "Buffer distance in meters to apply to the geometry before aggregation",
				Type:        ast.NamedType("Float", pos),
				Position:    pos,
			},
			&ast.ArgumentDefinition{
				Name:        "divide_values",
				Description: "Divide the aggregated values by a count of split h3 cells",
				Type:        ast.NamedType("Boolean", pos),
				Position:    pos,
			},
			&ast.ArgumentDefinition{
				Name:        "simplify",
				Description: "Simplify the geometry before cast to h3 cells",
				Type:        ast.NamedType("Boolean", pos),
				DefaultValue: &ast.Value{
					Kind:     ast.BooleanValue,
					Raw:      "true",
					Position: pos,
				},
				Position: pos,
			},
		)
		h3DataFields = append(h3DataFields, field)
	}

	if len(h3DataFields) == 0 {
		return nil
	}

	// Add fields as extension so they accumulate across catalogs
	ctx.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     "_h3_data_query",
		Position: pos,
		Fields:   h3DataFields,
	})

	return nil
}

// copyFieldType creates a shallow copy of a type tree with new positions.
func copyFieldType(t *ast.Type, pos *ast.Position) *ast.Type {
	if t == nil {
		return nil
	}
	cp := &ast.Type{
		NamedType: t.NamedType,
		NonNull:   t.NonNull,
		Position:  pos,
	}
	if t.Elem != nil {
		cp.Elem = copyFieldType(t.Elem, pos)
	}
	return cp
}

// copyArgDefs creates copies of argument definitions.
func copyArgDefs(args ast.ArgumentDefinitionList, pos *ast.Position) ast.ArgumentDefinitionList {
	if len(args) == 0 {
		return nil
	}
	out := make(ast.ArgumentDefinitionList, len(args))
	for i, a := range args {
		cp := &ast.ArgumentDefinition{
			Name:        a.Name,
			Description: a.Description,
			Type:        copyFieldType(a.Type, pos),
			Position:    pos,
		}
		if a.DefaultValue != nil {
			cp.DefaultValue = &ast.Value{
				Raw:      a.DefaultValue.Raw,
				Kind:     a.DefaultValue.Kind,
				Position: pos,
			}
		}
		out[i] = cp
	}
	return out
}

// copyFieldDirectives creates copies of field directives.
func copyFieldDirectives(dirs ast.DirectiveList, pos *ast.Position) ast.DirectiveList {
	if len(dirs) == 0 {
		return nil
	}
	out := make(ast.DirectiveList, len(dirs))
	for i, d := range dirs {
		cp := &ast.Directive{
			Name:     d.Name,
			Position: pos,
		}
		if len(d.Arguments) > 0 {
			cp.Arguments = make(ast.ArgumentList, len(d.Arguments))
			for j, a := range d.Arguments {
				cp.Arguments[j] = &ast.Argument{
					Name:     a.Name,
					Position: pos,
				}
				if a.Value != nil {
					cp.Arguments[j].Value = &ast.Value{
						Raw:      a.Value.Raw,
						Kind:     a.Value.Kind,
						Position: pos,
					}
				}
			}
		}
		out[i] = cp
	}
	return out
}
