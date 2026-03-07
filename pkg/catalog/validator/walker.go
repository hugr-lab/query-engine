package validator

import (
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type walker struct {
	wCtx        *WalkContext
	inlineRules []InlineRule
	errs        gqlerror.List

	validatedFragmentSpreads map[string]bool
}

func (w *walker) walk() {
	for _, op := range w.wCtx.Document.Operations {
		w.validatedFragmentSpreads = make(map[string]bool)
		w.walkOperation(op)
	}
	for _, frag := range w.wCtx.Document.Fragments {
		w.validatedFragmentSpreads = make(map[string]bool)
		w.walkFragment(frag)
	}
}

func (w *walker) walkOperation(op *ast.OperationDefinition) {
	w.wCtx.CurrentOperation = op
	ctx := w.wCtx.Context

	// Enrich variable definitions
	for _, varDef := range op.VariableDefinitions {
		varDef.Definition = w.wCtx.Provider.ForName(ctx, varDef.Type.Name())
		if varDef.DefaultValue != nil {
			varDef.DefaultValue.ExpectedType = varDef.Type
			varDef.DefaultValue.Definition = w.wCtx.Provider.ForName(ctx, varDef.Type.Name())
		}
	}

	// Resolve root type
	var def *ast.Definition
	var loc ast.DirectiveLocation
	switch op.Operation {
	case ast.Query, "":
		def = w.wCtx.Provider.QueryType(ctx)
		loc = ast.LocationQuery
	case ast.Mutation:
		def = w.wCtx.Provider.MutationType(ctx)
		loc = ast.LocationMutation
	case ast.Subscription:
		def = w.wCtx.Provider.SubscriptionType(ctx)
		loc = ast.LocationSubscription
	}

	// Walk variable default values and directives
	for _, varDef := range op.VariableDefinitions {
		if varDef.DefaultValue != nil {
			w.walkValue(varDef.DefaultValue)
		}
		w.walkDirectives(varDef.Definition, varDef.Directives, ast.LocationVariableDefinition)
	}

	if def == nil {
		w.errs = append(w.errs, gqlerror.ErrorPosf(op.Position, "operation %s not defined", op.Operation))
		w.wCtx.CurrentOperation = nil
		return
	}

	w.walkDirectives(def, op.Directives, loc)
	w.walkSelectionSet(def, op.SelectionSet)
	w.wCtx.CurrentOperation = nil
}

func (w *walker) walkFragment(frag *ast.FragmentDefinition) {
	def := w.wCtx.Provider.ForName(w.wCtx.Context, frag.TypeCondition)
	frag.Definition = def
	w.walkDirectives(def, frag.Directives, ast.LocationFragmentDefinition)
	w.walkSelectionSet(def, frag.SelectionSet)
}

func (w *walker) walkSelectionSet(parentDef *ast.Definition, it ast.SelectionSet) {
	for _, child := range it {
		w.walkSelection(parentDef, child)
	}
}

func (w *walker) walkSelection(parentDef *ast.Definition, sel ast.Selection) {
	switch sel := sel.(type) {
	case *ast.Field:
		var fieldDef *ast.FieldDefinition
		if sel.Name == "__typename" {
			fieldDef = &ast.FieldDefinition{
				Name: "__typename",
				Type: ast.NamedType("String", nil),
			}
		} else if parentDef != nil {
			fieldDef = parentDef.Fields.ForName(sel.Name)
		}

		// Enrichment
		sel.Definition = fieldDef
		sel.ObjectDefinition = parentDef

		// Inline rules: EnterField
		for _, rule := range w.inlineRules {
			w.errs = append(w.errs, rule.EnterField(w.wCtx, parentDef, sel)...)
		}

		// If field not resolved, skip subtree
		if fieldDef == nil {
			return
		}

		nextParentDef := w.wCtx.Provider.ForName(w.wCtx.Context, fieldDef.Type.Name())

		// Walk arguments
		for _, arg := range sel.Arguments {
			argDef := fieldDef.Arguments.ForName(arg.Name)
			w.walkArgument(argDef, arg)
		}

		w.walkDirectives(nextParentDef, sel.Directives, ast.LocationField)
		w.walkSelectionSet(nextParentDef, sel.SelectionSet)

	case *ast.InlineFragment:
		sel.ObjectDefinition = parentDef
		nextParentDef := parentDef
		if sel.TypeCondition != "" {
			nextParentDef = w.wCtx.Provider.ForName(w.wCtx.Context, sel.TypeCondition)
		}

		// Inline rules: EnterFragment
		for _, rule := range w.inlineRules {
			w.errs = append(w.errs, rule.EnterFragment(w.wCtx, parentDef, sel)...)
		}

		w.walkDirectives(nextParentDef, sel.Directives, ast.LocationInlineFragment)
		w.walkSelectionSet(nextParentDef, sel.SelectionSet)

	case *ast.FragmentSpread:
		def := w.wCtx.Document.Fragments.ForName(sel.Name)
		sel.Definition = def
		sel.ObjectDefinition = parentDef

		// Inline rules: EnterFragment
		for _, rule := range w.inlineRules {
			w.errs = append(w.errs, rule.EnterFragment(w.wCtx, parentDef, sel)...)
		}

		var nextParentDef *ast.Definition
		if def != nil {
			nextParentDef = w.wCtx.Provider.ForName(w.wCtx.Context, def.TypeCondition)
		}

		w.walkDirectives(nextParentDef, sel.Directives, ast.LocationFragmentSpread)

		if def != nil && !w.validatedFragmentSpreads[def.Name] {
			w.validatedFragmentSpreads[def.Name] = true
			w.walkSelectionSet(nextParentDef, def.SelectionSet)
		}
	}
}

func (w *walker) walkDirectives(parentDef *ast.Definition, dirs []*ast.Directive, loc ast.DirectiveLocation) {
	for _, dir := range dirs {
		def := w.wCtx.Provider.DirectiveForName(w.wCtx.Context, dir.Name)
		dir.Definition = def
		dir.ParentDefinition = parentDef
		dir.Location = loc

		// Inline rules: EnterDirective
		for _, rule := range w.inlineRules {
			w.errs = append(w.errs, rule.EnterDirective(w.wCtx, parentDef, dir)...)
		}

		// Walk directive arguments
		if def != nil {
			for _, arg := range dir.Arguments {
				argDef := def.Arguments.ForName(arg.Name)
				w.walkArgument(argDef, arg)
			}
		}
	}
}

func (w *walker) walkArgument(argDef *ast.ArgumentDefinition, arg *ast.Argument) {
	if argDef != nil {
		arg.Value.ExpectedType = argDef.Type
		arg.Value.ExpectedTypeHasDefault = argDef.DefaultValue != nil && argDef.DefaultValue.Kind != ast.NullValue
		arg.Value.Definition = w.wCtx.Provider.ForName(w.wCtx.Context, argDef.Type.Name())
	}

	// Inline rules: EnterArgument
	for _, rule := range w.inlineRules {
		w.errs = append(w.errs, rule.EnterArgument(w.wCtx, argDef, arg)...)
	}

	w.walkValue(arg.Value)
}

func (w *walker) walkValue(value *ast.Value) {
	if value.Kind == ast.Variable && w.wCtx.CurrentOperation != nil {
		value.VariableDefinition = w.wCtx.CurrentOperation.VariableDefinitions.ForName(value.Raw)
		if value.VariableDefinition != nil {
			value.VariableDefinition.Used = true
		}
	}

	if value.Kind == ast.ObjectValue {
		for _, child := range value.Children {
			if value.Definition != nil {
				fieldDef := value.Definition.Fields.ForName(child.Name)
				if fieldDef != nil {
					child.Value.ExpectedType = fieldDef.Type
					child.Value.ExpectedTypeHasDefault = fieldDef.DefaultValue != nil && fieldDef.DefaultValue.Kind != ast.NullValue
					child.Value.Definition = w.wCtx.Provider.ForName(w.wCtx.Context, fieldDef.Type.Name())
				}
			}
			w.walkValue(child.Value)
		}
	}

	if value.Kind == ast.ListValue {
		for _, child := range value.Children {
			if value.ExpectedType != nil && value.ExpectedType.Elem != nil {
				child.Value.ExpectedType = value.ExpectedType.Elem
				child.Value.Definition = value.Definition
			}
			w.walkValue(child.Value)
		}
	}
}
