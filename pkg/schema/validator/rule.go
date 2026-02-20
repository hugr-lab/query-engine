package validator

import (
	"context"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// InlineRule is called by the walker during AST traversal at each node.
type InlineRule interface {
	EnterField(ctx *WalkContext, parentDef *ast.Definition, field *ast.Field) gqlerror.List
	EnterFragment(ctx *WalkContext, parentDef *ast.Definition, frag ast.Selection) gqlerror.List
	EnterDirective(ctx *WalkContext, parentDef *ast.Definition, dir *ast.Directive) gqlerror.List
	EnterArgument(ctx *WalkContext, argDef *ast.ArgumentDefinition, arg *ast.Argument) gqlerror.List
}

// PostRule is called after the walker completes its full traversal.
type PostRule interface {
	Validate(ctx *WalkContext, document *ast.QueryDocument) gqlerror.List
}

// WalkContext provides context accessible to all rules during validation.
type WalkContext struct {
	Context          context.Context
	Provider         TypeResolver
	Document         *ast.QueryDocument
	CurrentOperation *ast.OperationDefinition
}
