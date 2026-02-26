package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// UniqueArgumentNames checks that arguments are unique within each field and directive.
type UniqueArgumentNames struct{}

func (r *UniqueArgumentNames) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	for _, op := range doc.Operations {
		errs = append(errs, checkUniqueArgsInSelections(op.SelectionSet)...)
		errs = append(errs, checkUniqueArgsInDirectives(op.Directives)...)
	}
	for _, frag := range doc.Fragments {
		errs = append(errs, checkUniqueArgsInSelections(frag.SelectionSet)...)
		errs = append(errs, checkUniqueArgsInDirectives(frag.Directives)...)
	}
	return errs
}

func checkUniqueArgsInSelections(selections ast.SelectionSet) gqlerror.List {
	var errs gqlerror.List
	for _, sel := range selections {
		switch sel := sel.(type) {
		case *ast.Field:
			seen := make(map[string]bool)
			for _, arg := range sel.Arguments {
				if seen[arg.Name] {
					errs = append(errs, gqlerror.ErrorPosf(arg.Position,
						`There can be only one argument named "%s".`, arg.Name))
				}
				seen[arg.Name] = true
			}
			errs = append(errs, checkUniqueArgsInDirectives(sel.Directives)...)
			errs = append(errs, checkUniqueArgsInSelections(sel.SelectionSet)...)
		case *ast.InlineFragment:
			errs = append(errs, checkUniqueArgsInDirectives(sel.Directives)...)
			errs = append(errs, checkUniqueArgsInSelections(sel.SelectionSet)...)
		case *ast.FragmentSpread:
			errs = append(errs, checkUniqueArgsInDirectives(sel.Directives)...)
		}
	}
	return errs
}

func checkUniqueArgsInDirectives(directives []*ast.Directive) gqlerror.List {
	var errs gqlerror.List
	for _, dir := range directives {
		seen := make(map[string]bool)
		for _, arg := range dir.Arguments {
			if seen[arg.Name] {
				errs = append(errs, gqlerror.ErrorPosf(arg.Position,
					`There can be only one argument named "%s".`, arg.Name))
			}
			seen[arg.Name] = true
		}
	}
	return errs
}
