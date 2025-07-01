package base

import (
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"
)

const (
	AddH3DirectiveName          = "add_h3"
	H3QueryFieldName            = "h3"
	H3QueryTypeName             = "_h3_query"
	H3DataQueryTypeName         = "_h3_data_query"
	H3DataFieldName             = "data"
	DistributionFieldName       = "distribution_by"
	DistributionTypeName        = "_distribution_by"
	BucketDistributionFieldName = "distribution_by_bucket"
	BucketDistributionTypeName  = "_distribution_by_bucket"
)

func AddH3Directive(field string, resolution int, transformFrom int, buffer float64, divideVals bool, simplify bool, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: AddH3DirectiveName,
		Arguments: []*ast.Argument{
			{Name: "res", Value: &ast.Value{Kind: ast.IntValue, Raw: strconv.Itoa(resolution), Position: pos}, Position: pos},
			{Name: "field", Value: &ast.Value{Kind: ast.StringValue, Raw: field, Position: pos}, Position: pos},
			{Name: "transform_from", Value: &ast.Value{Kind: ast.IntValue, Raw: strconv.Itoa(transformFrom), Position: pos}, Position: pos},
			{Name: "buffer", Value: &ast.Value{Kind: ast.FloatValue, Raw: strconv.FormatFloat(buffer, 'f', -1, 64), Position: pos}, Position: pos},
			{Name: "divide_values", Value: &ast.Value{Kind: ast.BooleanValue, Raw: strconv.FormatBool(divideVals), Position: pos}, Position: pos},
			{Name: "simplify", Value: &ast.Value{Kind: ast.BooleanValue, Raw: strconv.FormatBool(simplify), Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

func H3QueryDefinitionTemplate() *ast.Definition {
	return &ast.Definition{
		Name:       H3QueryTypeName,
		Kind:       ast.Object,
		Directives: ast.DirectiveList{SystemDirective},
		Fields: ast.FieldList{
			&ast.FieldDefinition{
				Name:        "cell",
				Description: "The H3 cell",
				Type:        ast.NonNullNamedType("H3Cell", CompiledPos("")),
				Position:    CompiledPos(""),
			},
			&ast.FieldDefinition{
				Name:        "resolution",
				Description: "The resolution of the H3 cell",
				Type:        ast.NonNullNamedType("Int", CompiledPos("")),
				Position:    CompiledPos(""),
			},
			&ast.FieldDefinition{
				Name:        "geom",
				Description: "The geometry representation of the H3 cell",
				Type:        ast.NonNullNamedType("Geometry", CompiledPos("")),
				Position:    CompiledPos(""),
			},
			&ast.FieldDefinition{
				Name:        H3DataFieldName,
				Description: "The data associated with the H3 cell",
				Type:        ast.NonNullNamedType(H3DataQueryTypeName, CompiledPos("")),
				Position:    CompiledPos(""),
			},
			&ast.FieldDefinition{
				Name:        DistributionFieldName,
				Description: "Calculate distributed aggregated value",
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "numerator",
						Description: "Path to numerator aggregated value",
						Type:        ast.NonNullNamedType("String", CompiledPos("")),
						Position:    CompiledPos(""),
					},
					{
						Name:        "denominator",
						Description: "Path to denominator aggregated value",
						Type:        ast.NonNullNamedType("String", CompiledPos("")),
						Position:    CompiledPos(""),
					},
				},
				Type:     ast.NamedType(DistributionTypeName, CompiledPos("")),
				Position: CompiledPos(""),
			},
			&ast.FieldDefinition{
				Name:        BucketDistributionFieldName,
				Description: "Calculate distributed aggregated value across denominator buckets",
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "numerator_key",
						Description: "Path to bucket key in numerator bucket aggregation",
						Type:        ast.NamedType("String", CompiledPos("")),
						Position:    CompiledPos(""),
					},
					{
						Name:        "numerator",
						Description: "Path to numerator aggregated value",
						Type:        ast.NonNullNamedType("String", CompiledPos("")),
						Position:    CompiledPos(""),
					},
					{
						Name:        "denominator_key",
						Description: "Path to bucket key in denominator bucket aggregation",
						Type:        ast.NonNullNamedType("String", CompiledPos("")),
						Position:    CompiledPos(""),
					},
					{
						Name:        "denominator",
						Description: "Path to denominator aggregated value in bucket aggregations",
						Type:        ast.NonNullNamedType("String", CompiledPos("")),
						Position:    CompiledPos(""),
					},
				},
				Type:     ast.ListType(ast.NamedType(BucketDistributionTypeName, CompiledPos("")), CompiledPos("")),
				Position: CompiledPos(""),
			},
		},
		Position: CompiledPos(""),
	}
}
