package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// PossibleFragmentSpreads checks that fragments spread on types that are valid for the parent.
type PossibleFragmentSpreads struct{}

func (r *PossibleFragmentSpreads) Validate(ctx *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	var walkSelections func(parentDef *ast.Definition, selections ast.SelectionSet)
	walkSelections = func(parentDef *ast.Definition, selections ast.SelectionSet) {
		for _, sel := range selections {
			switch sel := sel.(type) {
			case *ast.Field:
				if sel.Definition != nil {
					nextDef := ctx.Provider.ForName(ctx.Context, sel.Definition.Type.Name())
					walkSelections(nextDef, sel.SelectionSet)
				}
			case *ast.InlineFragment:
				nextDef := parentDef
				if sel.TypeCondition != "" {
					nextDef = ctx.Provider.ForName(ctx.Context, sel.TypeCondition)
				}
				if parentDef != nil && nextDef != nil && sel.TypeCondition != "" {
					if !doTypesOverlap(ctx, parentDef, nextDef) {
						errs = append(errs, gqlerror.ErrorPosf(sel.Position,
							`Fragment cannot be spread here as objects of type "%s" can never be of type "%s".`,
							parentDef.Name, sel.TypeCondition))
					}
				}
				walkSelections(nextDef, sel.SelectionSet)
			case *ast.FragmentSpread:
				if sel.Definition != nil && parentDef != nil {
					fragTypeDef := ctx.Provider.ForName(ctx.Context, sel.Definition.TypeCondition)
					if fragTypeDef != nil {
						if !doTypesOverlap(ctx, parentDef, fragTypeDef) {
							errs = append(errs, gqlerror.ErrorPosf(sel.Position,
								`Fragment "%s" cannot be spread here as objects of type "%s" can never be of type "%s".`,
								sel.Name, parentDef.Name, sel.Definition.TypeCondition))
						}
					}
				}
			}
		}
	}

	for _, op := range doc.Operations {
		var rootDef *ast.Definition
		switch op.Operation {
		case ast.Query, "":
			rootDef = ctx.Provider.QueryType(ctx.Context)
		case ast.Mutation:
			rootDef = ctx.Provider.MutationType(ctx.Context)
		case ast.Subscription:
			rootDef = ctx.Provider.SubscriptionType(ctx.Context)
		}
		walkSelections(rootDef, op.SelectionSet)
	}
	for _, frag := range doc.Fragments {
		fragDef := ctx.Provider.ForName(ctx.Context, frag.TypeCondition)
		walkSelections(fragDef, frag.SelectionSet)
	}
	return errs
}

func doTypesOverlap(ctx *validator.WalkContext, t1, t2 *ast.Definition) bool {
	if t1.Name == t2.Name {
		return true
	}
	possibles1 := possibleTypeSet(ctx, t1)
	possibles2 := possibleTypeSet(ctx, t2)
	for name := range possibles1 {
		if possibles2[name] {
			return true
		}
	}
	return false
}

func possibleTypeSet(ctx *validator.WalkContext, def *ast.Definition) map[string]bool {
	result := make(map[string]bool)
	switch def.Kind {
	case ast.Object:
		result[def.Name] = true
	case ast.Interface, ast.Union:
		for _, t := range ctx.Provider.PossibleTypes(ctx.Context, def) {
			result[t.Name] = true
		}
	}
	return result
}
