package rules

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// OverlappingFieldsCanBeMerged checks that fields with the same response name can be merged.
type OverlappingFieldsCanBeMerged struct{}

func (r *OverlappingFieldsCanBeMerged) Validate(ctx *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	m := &mergeManager{
		ctx:                   ctx,
		comparedFragmentPairs: make(map[string]map[string]bool),
	}
	var errs gqlerror.List
	for _, op := range doc.Operations {
		conflicts := m.findConflictsWithinSelectionSet(op.SelectionSet)
		for _, c := range conflicts {
			errs = append(errs, c.toError())
		}
	}
	var walkFields func(selections ast.SelectionSet)
	walkFields = func(selections ast.SelectionSet) {
		for _, sel := range selections {
			switch sel := sel.(type) {
			case *ast.Field:
				conflicts := m.findConflictsWithinSelectionSet(sel.SelectionSet)
				for _, c := range conflicts {
					errs = append(errs, c.toError())
				}
				walkFields(sel.SelectionSet)
			case *ast.InlineFragment:
				conflicts := m.findConflictsWithinSelectionSet(sel.SelectionSet)
				for _, c := range conflicts {
					errs = append(errs, c.toError())
				}
				walkFields(sel.SelectionSet)
			}
		}
	}
	for _, op := range doc.Operations {
		walkFields(op.SelectionSet)
	}
	for _, frag := range doc.Fragments {
		conflicts := m.findConflictsWithinSelectionSet(frag.SelectionSet)
		for _, c := range conflicts {
			errs = append(errs, c.toError())
		}
		walkFields(frag.SelectionSet)
	}
	return errs
}

type mergeConflict struct {
	responseName string
	message      string
	subConflicts []*mergeConflict
	position     *ast.Position
}

func (c *mergeConflict) toError() *gqlerror.Error {
	var buf bytes.Buffer
	c.writeMessage(&buf)
	return gqlerror.ErrorPosf(c.position,
		`Fields "%s" conflict because %s. Use different aliases on the fields to fetch both if this was intentional.`,
		c.responseName, buf.String())
}

func (c *mergeConflict) writeMessage(buf *bytes.Buffer) {
	if len(c.subConflicts) == 0 {
		buf.WriteString(c.message)
		return
	}
	for i, sub := range c.subConflicts {
		buf.WriteString(`subfields "`)
		buf.WriteString(sub.responseName)
		buf.WriteString(`" conflict because `)
		sub.writeMessage(buf)
		if i < len(c.subConflicts)-1 {
			buf.WriteString(" and ")
		}
	}
}

type mergeManager struct {
	ctx                   *validator.WalkContext
	comparedFragmentPairs map[string]map[string]bool
	comparedFragments     map[string]bool
}

type seqFieldsMap struct {
	seq  []string
	data map[string][]*ast.Field
}

func (m *seqFieldsMap) push(name string, f *ast.Field) {
	if _, ok := m.data[name]; !ok {
		m.seq = append(m.seq, name)
	}
	m.data[name] = append(m.data[name], f)
}

func (m *mergeManager) findConflictsWithinSelectionSet(ss ast.SelectionSet) []*mergeConflict {
	if len(ss) == 0 {
		return nil
	}
	fieldsMap, spreads := collectFieldsAndSpreads(ss)
	var conflicts []*mergeConflict

	for _, fields := range fieldsMap.data {
		for i, a := range fields {
			for _, b := range fields[i+1:] {
				if c := m.findConflict(false, a, b); c != nil {
					conflicts = append(conflicts, c)
				}
			}
		}
	}

	m.comparedFragments = make(map[string]bool)
	for i, spreadA := range spreads {
		m.collectBetweenFieldsAndFragment(&conflicts, false, fieldsMap, spreadA)
		for _, spreadB := range spreads[i+1:] {
			m.collectBetweenFragments(&conflicts, false, spreadA, spreadB)
		}
	}
	return conflicts
}

func (m *mergeManager) collectBetweenFieldsAndFragment(conflicts *[]*mergeConflict, exclusive bool, fieldsMap *seqFieldsMap, spread *ast.FragmentSpread) {
	if m.comparedFragments[spread.Name] {
		return
	}
	m.comparedFragments[spread.Name] = true
	if spread.Definition == nil {
		return
	}
	fieldsB, spreadsB := collectFieldsAndSpreads(spread.Definition.SelectionSet)
	if reflect.DeepEqual(fieldsMap, fieldsB) {
		return
	}
	m.collectBetween(conflicts, exclusive, fieldsMap, fieldsB)
	for _, s := range spreadsB {
		if s.Name == spread.Name {
			continue
		}
		m.collectBetweenFieldsAndFragment(conflicts, exclusive, fieldsMap, s)
	}
}

func (m *mergeManager) collectBetweenFragments(conflicts *[]*mergeConflict, exclusive bool, a, b *ast.FragmentSpread) {
	var check func(a, b *ast.FragmentSpread)
	check = func(a, b *ast.FragmentSpread) {
		if a.Name == b.Name {
			return
		}
		if am, ok := m.comparedFragmentPairs[a.Name]; ok {
			if _, ok := am[b.Name]; ok {
				return
			}
		}
		if m.comparedFragmentPairs[a.Name] == nil {
			m.comparedFragmentPairs[a.Name] = make(map[string]bool)
		}
		m.comparedFragmentPairs[a.Name][b.Name] = exclusive
		if m.comparedFragmentPairs[b.Name] == nil {
			m.comparedFragmentPairs[b.Name] = make(map[string]bool)
		}
		m.comparedFragmentPairs[b.Name][a.Name] = exclusive

		if a.Definition == nil || b.Definition == nil {
			return
		}
		fieldsA, spreadsA := collectFieldsAndSpreads(a.Definition.SelectionSet)
		fieldsB, spreadsB := collectFieldsAndSpreads(b.Definition.SelectionSet)
		m.collectBetween(conflicts, exclusive, fieldsA, fieldsB)
		for _, s := range spreadsB {
			check(a, s)
		}
		for _, s := range spreadsA {
			check(s, b)
		}
	}
	check(a, b)
}

func (m *mergeManager) collectBetween(conflicts *[]*mergeConflict, exclusive bool, a, b *seqFieldsMap) {
	for _, name := range a.seq {
		fieldsB, ok := b.data[name]
		if !ok {
			continue
		}
		for _, fa := range a.data[name] {
			for _, fb := range fieldsB {
				if c := m.findConflict(exclusive, fa, fb); c != nil {
					*conflicts = append(*conflicts, c)
				}
			}
		}
	}
}

func (m *mergeManager) findConflict(parentExclusive bool, a, b *ast.Field) *mergeConflict {
	if a.ObjectDefinition == nil || b.ObjectDefinition == nil {
		return nil
	}
	exclusive := parentExclusive
	if !exclusive {
		tmp := a.ObjectDefinition.Name != b.ObjectDefinition.Name
		tmp = tmp && a.ObjectDefinition.Kind == ast.Object
		tmp = tmp && b.ObjectDefinition.Kind == ast.Object
		tmp = tmp && a.Definition != nil && b.Definition != nil
		exclusive = tmp
	}
	responseName := a.Name
	if a.Alias != "" {
		responseName = a.Alias
	}
	if !exclusive {
		if a.Name != b.Name {
			return &mergeConflict{
				responseName: responseName,
				message:      fmt.Sprintf(`"%s" and "%s" are different fields`, a.Name, b.Name),
				position:     b.Position,
			}
		}
		if !sameArgs(a.Arguments, b.Arguments) {
			return &mergeConflict{
				responseName: responseName,
				message:      "they have differing arguments",
				position:     b.Position,
			}
		}
	}
	if a.Definition != nil && b.Definition != nil && typesConflict(m.ctx, a.Definition.Type, b.Definition.Type) {
		return &mergeConflict{
			responseName: responseName,
			message:      fmt.Sprintf(`they return conflicting types "%s" and "%s"`, a.Definition.Type.String(), b.Definition.Type.String()),
			position:     b.Position,
		}
	}
	if len(a.SelectionSet) > 0 && len(b.SelectionSet) > 0 {
		subConflicts := m.findConflictsBetweenSubSelections(exclusive, a.SelectionSet, b.SelectionSet)
		if len(subConflicts) > 0 {
			return &mergeConflict{
				responseName: responseName,
				subConflicts: subConflicts,
				position:     b.Position,
			}
		}
	}
	return nil
}

func (m *mergeManager) findConflictsBetweenSubSelections(exclusive bool, ssA, ssB ast.SelectionSet) []*mergeConflict {
	fieldsA, spreadsA := collectFieldsAndSpreads(ssA)
	fieldsB, spreadsB := collectFieldsAndSpreads(ssB)
	var conflicts []*mergeConflict
	m.collectBetween(&conflicts, exclusive, fieldsA, fieldsB)
	for _, s := range spreadsB {
		m.comparedFragments = make(map[string]bool)
		m.collectBetweenFieldsAndFragment(&conflicts, exclusive, fieldsA, s)
	}
	for _, s := range spreadsA {
		m.comparedFragments = make(map[string]bool)
		m.collectBetweenFieldsAndFragment(&conflicts, exclusive, fieldsB, s)
	}
	for _, sa := range spreadsA {
		for _, sb := range spreadsB {
			m.collectBetweenFragments(&conflicts, exclusive, sa, sb)
		}
	}
	return conflicts
}

func collectFieldsAndSpreads(ss ast.SelectionSet) (*seqFieldsMap, []*ast.FragmentSpread) {
	m := &seqFieldsMap{data: make(map[string][]*ast.Field)}
	var spreads []*ast.FragmentSpread
	var walk func(ast.SelectionSet)
	walk = func(ss ast.SelectionSet) {
		for _, sel := range ss {
			switch sel := sel.(type) {
			case *ast.Field:
				name := sel.Name
				if sel.Alias != "" {
					name = sel.Alias
				}
				m.push(name, sel)
			case *ast.InlineFragment:
				walk(sel.SelectionSet)
			case *ast.FragmentSpread:
				spreads = append(spreads, sel)
			}
		}
	}
	walk(ss)
	return m, spreads
}

func sameArgs(a, b []*ast.Argument) bool {
	if len(a) != len(b) {
		return false
	}
	for _, argA := range a {
		matched := false
		for _, argB := range b {
			if argA.Name == argB.Name && sameArgValue(argA.Value, argB.Value) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func sameArgValue(a, b *ast.Value) bool {
	if a.Kind != b.Kind {
		return false
	}
	return a.Raw == b.Raw
}

func typesConflict(ctx *validator.WalkContext, t1, t2 *ast.Type) bool {
	if t1.Elem != nil {
		if t2.Elem != nil {
			return typesConflict(ctx, t1.Elem, t2.Elem)
		}
		return true
	}
	if t2.Elem != nil {
		return true
	}
	if t1.NonNull && !t2.NonNull {
		return true
	}
	if !t1.NonNull && t2.NonNull {
		return true
	}
	d1 := ctx.Provider.ForName(ctx.Context, t1.NamedType)
	d2 := ctx.Provider.ForName(ctx.Context, t2.NamedType)
	if d1 == nil || d2 == nil {
		return false
	}
	if (d1.Kind == ast.Scalar || d1.Kind == ast.Enum) && (d2.Kind == ast.Scalar || d2.Kind == ast.Enum) {
		return d1.Name != d2.Name
	}
	return false
}
