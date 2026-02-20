package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ProvidedRequiredArguments checks that all required arguments are provided.
type ProvidedRequiredArguments struct{}

func (r *ProvidedRequiredArguments) Validate(ctx *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	var walkSelections func(selections ast.SelectionSet)
	walkSelections = func(selections ast.SelectionSet) {
		for _, sel := range selections {
			switch sel := sel.(type) {
			case *ast.Field:
				if sel.Definition != nil {
					for _, argDef := range sel.Definition.Arguments {
						if argDef.Type.NonNull && argDef.DefaultValue == nil {
							if sel.Arguments.ForName(argDef.Name) == nil {
								errs = append(errs, gqlerror.ErrorPosf(sel.Position,
									`Field "%s" argument "%s" of type "%s" is required, but it was not provided.`,
									sel.Name, argDef.Name, argDef.Type.String()))
							}
						}
					}
				}
				walkSelections(sel.SelectionSet)
			case *ast.InlineFragment:
				walkSelections(sel.SelectionSet)
			}
		}
	}
	checkDirectiveArgs := func(directives []*ast.Directive) {
		for _, dir := range directives {
			if dir.Definition == nil {
				continue
			}
			for _, argDef := range dir.Definition.Arguments {
				if argDef.Type.NonNull && argDef.DefaultValue == nil {
					if dir.Arguments.ForName(argDef.Name) == nil {
						errs = append(errs, gqlerror.ErrorPosf(dir.Position,
							`Directive "@%s" argument "%s" of type "%s" is required, but it was not provided.`,
							dir.Name, argDef.Name, argDef.Type.String()))
					}
				}
			}
		}
	}
	for _, op := range doc.Operations {
		walkSelections(op.SelectionSet)
		checkDirectiveArgs(op.Directives)
		WalkDirectivesInSelections(op.SelectionSet, checkDirectiveArgs)
	}
	for _, frag := range doc.Fragments {
		walkSelections(frag.SelectionSet)
		checkDirectiveArgs(frag.Directives)
		WalkDirectivesInSelections(frag.SelectionSet, checkDirectiveArgs)
	}
	return errs
}
