package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*H3Rule)(nil)

// H3Rule creates _h3_data_query and _h3_query types, and registers the "h3"
// query field on root Query. It runs after JoinSpatialRule because it copies
// fields from the _spatial type.
type H3Rule struct{}

func (r *H3Rule) Name() string     { return "H3Rule" }
func (r *H3Rule) Phase() base.Phase { return base.PhaseGenerate }

func (r *H3Rule) ProcessAll(ctx base.CompilationContext) error {
	// Skip if _h3_query already exists
	if ctx.LookupType("_h3_query") != nil {
		return nil
	}
	// Requires _spatial type to exist
	spatial := ctx.LookupType("_spatial")
	if spatial == nil {
		return nil
	}

	pos := compiledPos("h3")

	// --- _h3_data_query type ---
	// Copy fields from _spatial with added H3-specific arguments.
	h3DataType := &ast.Definition{
		Kind:        ast.Object,
		Name:        "_h3_data_query",
		Description: "Data queries for H3 aggregation",
		Position:    pos,
		Directives: ast.DirectiveList{
			{Name: "system", Position: pos},
		},
	}
	for _, f := range spatial.Fields {
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
		h3DataType.Fields = append(h3DataType.Fields, field)
	}

	if len(h3DataType.Fields) == 0 {
		return nil
	}
	ctx.AddDefinition(h3DataType)

	// --- _h3_query type ---
	h3QueryType := &ast.Definition{
		Kind:     ast.Object,
		Name:     "_h3_query",
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "system", Position: pos},
		},
		Fields: ast.FieldList{
			{
				Name:        "cell",
				Description: "The H3 cell",
				Type:        ast.NonNullNamedType("H3Cell", pos),
				Position:    pos,
			},
			{
				Name:        "resolution",
				Description: "The resolution of the H3 cell",
				Type:        ast.NonNullNamedType("Int", pos),
				Position:    pos,
			},
			{
				Name:        "geom",
				Description: "The geometry representation of the H3 cell",
				Type:        ast.NonNullNamedType("Geometry", pos),
				Position:    pos,
			},
			{
				Name:        "data",
				Description: "The data associated with the H3 cell",
				Type:        ast.NonNullNamedType("_h3_data_query", pos),
				Position:    pos,
			},
			{
				Name:        "distribution_by",
				Description: "Calculate distributed aggregated value",
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "numerator",
						Description: "Path to numerator aggregated value",
						Type:        ast.NonNullNamedType("String", pos),
						Position:    pos,
					},
					{
						Name:        "denominator",
						Description: "Path to denominator aggregated value",
						Type:        ast.NonNullNamedType("String", pos),
						Position:    pos,
					},
				},
				Type:     ast.NamedType("_distribution_by", pos),
				Position: pos,
			},
			{
				Name:        "distribution_by_bucket",
				Description: "Calculate distributed aggregated value across denominator buckets",
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "numerator_key",
						Description: "Path to bucket key in numerator bucket aggregation",
						Type:        ast.NamedType("String", pos),
						Position:    pos,
					},
					{
						Name:        "numerator",
						Description: "Path to numerator aggregated value",
						Type:        ast.NonNullNamedType("String", pos),
						Position:    pos,
					},
					{
						Name:        "denominator_key",
						Description: "Path to bucket key in denominator bucket aggregation",
						Type:        ast.NonNullNamedType("String", pos),
						Position:    pos,
					},
					{
						Name:        "denominator",
						Description: "Path to denominator aggregated value in bucket aggregations",
						Type:        ast.NonNullNamedType("String", pos),
						Position:    pos,
					},
				},
				Type:     ast.ListType(ast.NamedType("_distribution_by_bucket", pos), pos),
				Position: pos,
			},
		},
	}
	ctx.AddDefinition(h3QueryType)

	// Register "h3" query field on root Query
	h3Field := &ast.FieldDefinition{
		Name:        "h3",
		Description: "H3 aggregation query",
		Type:        ast.ListType(ast.NamedType("_h3_query", pos), pos),
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:        "resolution",
				Description: "H3 resolution to use for the query",
				Type:        ast.NonNullNamedType("Int", pos),
				Position:    pos,
			},
		},
		Directives: ast.DirectiveList{
			{Name: "system", Position: pos},
		},
		Position: pos,
	}
	ctx.RegisterQueryFields("_h3", []*ast.FieldDefinition{h3Field})

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
