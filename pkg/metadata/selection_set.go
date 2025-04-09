package metadata

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/vektah/gqlparser/v2/ast"
)

type fieldResolverFunc func(ctx context.Context, field *ast.Field, onType string) (any, error)

func processSelectionSet(ctx context.Context, ss ast.SelectionSet, resolvers map[string]fieldResolverFunc, onType string) (map[string]any, error) {
	res := make(map[string]any)
	for _, selection := range ss {
		switch selection := selection.(type) {
		case *ast.FragmentSpread:
			data, err := processSelectionSet(ctx, selection.Definition.SelectionSet, resolvers, onType)
			if err != nil {
				return nil, err
			}
			for k, v := range data {
				res[k] = v
			}
		case *ast.InlineFragment:
			if selection.TypeCondition != onType {
				continue
			}
			data, err := processSelectionSet(ctx, selection.SelectionSet, resolvers, onType)
			if err != nil {
				return nil, err
			}
			for k, v := range data {
				res[k] = v
			}
		case *ast.Field:
			resolver, ok := resolvers[selection.Name]
			if !ok {
				pos := selection.Position
				if pos == nil {
					pos = &ast.Position{}
				}
				return nil, compiler.ErrorPosf(selection.Position, "%s: couldn't find field resolver", selection.Name)
			}
			data, err := resolver(ctx, selection, "")
			if err != nil {
				return nil, err
			}
			if selection.Alias == "" {
				selection.Alias = selection.Name
			}
			res[selection.Alias] = data

		}
	}
	return res, nil
}
