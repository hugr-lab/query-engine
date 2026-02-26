package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// BaseInlineRule provides no-op implementations for all InlineRule methods.
// Embed it in inline rules and override only the relevant method.
type BaseInlineRule struct{}

func (BaseInlineRule) EnterField(*validator.WalkContext, *ast.Definition, *ast.Field) gqlerror.List {
	return nil
}
func (BaseInlineRule) EnterFragment(*validator.WalkContext, *ast.Definition, ast.Selection) gqlerror.List {
	return nil
}
func (BaseInlineRule) EnterDirective(*validator.WalkContext, *ast.Definition, *ast.Directive) gqlerror.List {
	return nil
}
func (BaseInlineRule) EnterArgument(*validator.WalkContext, *ast.ArgumentDefinition, *ast.Argument) gqlerror.List {
	return nil
}

// WalkAllValues walks all argument/default values in a document.
func WalkAllValues(doc *ast.QueryDocument, fn func(*ast.Value)) {
	for _, op := range doc.Operations {
		WalkAllValuesInOperation(op, fn)
	}
	for _, frag := range doc.Fragments {
		WalkValuesInSelections(frag.SelectionSet, fn)
	}
}

// WalkAllValuesInOperation walks all values (variable defaults + argument values) in an operation.
func WalkAllValuesInOperation(op *ast.OperationDefinition, fn func(*ast.Value)) {
	for _, varDef := range op.VariableDefinitions {
		if varDef.DefaultValue != nil {
			WalkValueRecursive(varDef.DefaultValue, fn)
		}
	}
	WalkValuesInSelections(op.SelectionSet, fn)
}

// WalkValuesInSelections walks all argument values in a selection set.
func WalkValuesInSelections(selections ast.SelectionSet, fn func(*ast.Value)) {
	for _, sel := range selections {
		switch sel := sel.(type) {
		case *ast.Field:
			for _, arg := range sel.Arguments {
				WalkValueRecursive(arg.Value, fn)
			}
			for _, dir := range sel.Directives {
				for _, arg := range dir.Arguments {
					WalkValueRecursive(arg.Value, fn)
				}
			}
			WalkValuesInSelections(sel.SelectionSet, fn)
		case *ast.InlineFragment:
			for _, dir := range sel.Directives {
				for _, arg := range dir.Arguments {
					WalkValueRecursive(arg.Value, fn)
				}
			}
			WalkValuesInSelections(sel.SelectionSet, fn)
		case *ast.FragmentSpread:
			for _, dir := range sel.Directives {
				for _, arg := range dir.Arguments {
					WalkValueRecursive(arg.Value, fn)
				}
			}
		}
	}
}

// WalkValueRecursive visits a value and all its children.
func WalkValueRecursive(value *ast.Value, fn func(*ast.Value)) {
	if value == nil {
		return
	}
	fn(value)
	for _, child := range value.Children {
		WalkValueRecursive(child.Value, fn)
	}
}

// WalkDirectivesInSelections walks directives in all selections recursively.
func WalkDirectivesInSelections(selections ast.SelectionSet, fn func([]*ast.Directive)) {
	for _, sel := range selections {
		switch sel := sel.(type) {
		case *ast.Field:
			fn(sel.Directives)
			WalkDirectivesInSelections(sel.SelectionSet, fn)
		case *ast.InlineFragment:
			fn(sel.Directives)
			WalkDirectivesInSelections(sel.SelectionSet, fn)
		case *ast.FragmentSpread:
			fn(sel.Directives)
		}
	}
}
