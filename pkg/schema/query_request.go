package schema

import (
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// QueryRequestInfo recursively classifies an operation's selection set into
// typed query requests (meta, data, mutation, function, jq, h3).
func QueryRequestInfo(ss ast.SelectionSet) ([]base.QueryRequest, base.QueryType) {
	resolvers := make([]base.QueryRequest, 0)
	qtt := base.QueryTypeNone
	for _, sel := range ss {
		if fragment, ok := sel.(*ast.FragmentSpread); ok {
			rr, qt := QueryRequestInfo(fragment.Definition.SelectionSet)
			resolvers = append(resolvers, rr...)
			qtt |= qt
			continue
		}

		if _, ok := sel.(*ast.InlineFragment); ok {
			// ignore inline fragment unions and interfaces should be handled in data query
			continue
		}

		field, ok := sel.(*ast.Field)
		if !ok {
			continue
		}
		if field.Name == base.MetadataSchemaQuery ||
			field.Name == base.MetadataTypeQuery ||
			field.Name == base.MetadataTypeNameQuery {
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeMeta,
			})
			qtt |= base.QueryTypeMeta
			continue
		}
		if field.Directives.ForName("skip") != nil {
			continue
		}
		if field.ObjectDefinition == nil {
			continue
		}
		if field.ObjectDefinition.Kind != ast.Object {
			continue
		}
		fd := field.ObjectDefinition.Fields.ForName(field.Name)
		if fd == nil {
			continue
		}
		info := base.ModuleRootInfo(field.ObjectDefinition)
		if info == nil {
			continue
		}
		switch {
		case info.Type == base.ModuleQuery && fd.Directives.ForName("query") != nil ||
			fd.Directives.ForName(base.FieldAggregationQueryDirectiveName) != nil:
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeQuery,
			})
			qtt |= base.QueryTypeQuery
		case info.Type == base.ModuleMutation && fd.Directives.ForName("mutation") != nil:
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeMutation,
			})
			qtt |= base.QueryTypeMutation
		case info.Type == base.ModuleFunction && fd.Directives.ForName("function") != nil:
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeFunction,
			})
			qtt |= base.QueryTypeFunction
		case info.Type == base.ModuleMutationFunction && fd.Directives.ForName("function") != nil:
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeFunctionMutation,
			})
			qtt |= base.QueryTypeFunctionMutation
		case field.Name == base.JQTransformQueryName:
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeJQTransform,
			})
			qtt |= base.QueryTypeJQTransform
		case field.Name == base.H3QueryFieldName:
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeH3Aggregation,
			})
			qtt |= base.QueryTypeH3Aggregation
		default:
			rr, qt := QueryRequestInfo(field.SelectionSet)
			resolvers = append(resolvers, base.QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: base.QueryTypeNone,
				Subset:    rr,
			})
			qtt |= qt
		}
	}

	return resolvers, qtt
}
