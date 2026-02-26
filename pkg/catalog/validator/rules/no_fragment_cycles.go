package rules

import (
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// NoFragmentCycles checks that fragments do not form cycles.
type NoFragmentCycles struct{}

func (r *NoFragmentCycles) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	visitedFrags := make(map[string]bool)

	for _, frag := range doc.Fragments {
		var spreadPath []*ast.FragmentSpread
		spreadPathIndexByName := make(map[string]int)

		var recursive func(frag *ast.FragmentDefinition)
		recursive = func(frag *ast.FragmentDefinition) {
			if visitedFrags[frag.Name] {
				return
			}
			visitedFrags[frag.Name] = true

			spreadNodes := collectFragmentSpreads(frag.SelectionSet)
			if len(spreadNodes) == 0 {
				return
			}
			spreadPathIndexByName[frag.Name] = len(spreadPath)

			for _, spreadNode := range spreadNodes {
				spreadName := spreadNode.Name
				cycleIndex, ok := spreadPathIndexByName[spreadName]
				spreadPath = append(spreadPath, spreadNode)
				if !ok {
					spreadFragment := doc.Fragments.ForName(spreadName)
					if spreadFragment != nil {
						recursive(spreadFragment)
					}
				} else {
					cyclePath := spreadPath[cycleIndex : len(spreadPath)-1]
					var fragmentNames []string
					for _, fs := range cyclePath {
						fragmentNames = append(fragmentNames, fmt.Sprintf(`"%s"`, fs.Name))
					}
					var via string
					if len(fragmentNames) != 0 {
						via = fmt.Sprintf(" via %s", strings.Join(fragmentNames, ", "))
					}
					errs = append(errs, gqlerror.ErrorPosf(spreadNode.Position,
						`Cannot spread fragment "%s" within itself%s.`, spreadName, via))
				}
				spreadPath = spreadPath[:len(spreadPath)-1]
			}
			delete(spreadPathIndexByName, frag.Name)
		}
		recursive(frag)
	}
	return errs
}

func collectFragmentSpreads(node ast.SelectionSet) []*ast.FragmentSpread {
	var spreads []*ast.FragmentSpread
	setsToVisit := []ast.SelectionSet{node}
	for len(setsToVisit) != 0 {
		set := setsToVisit[len(setsToVisit)-1]
		setsToVisit = setsToVisit[:len(setsToVisit)-1]
		for _, selection := range set {
			switch sel := selection.(type) {
			case *ast.FragmentSpread:
				spreads = append(spreads, sel)
			case *ast.Field:
				setsToVisit = append(setsToVisit, sel.SelectionSet)
			case *ast.InlineFragment:
				setsToVisit = append(setsToVisit, sel.SelectionSet)
			}
		}
	}
	return spreads
}
