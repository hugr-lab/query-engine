package planner

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

type ctxKey string

var rawResultsKey = ctxKey("rawResults")

func ContextWithRawResultsFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, rawResultsKey, true)
}

func IsRawResultsQuery(ctx context.Context, field *ast.Field) bool {
	if field.Directives.ForName(base.RawResultsDirective) != nil {
		return true
	}
	if ctx == nil {
		return false
	}
	if v := ctx.Value(rawResultsKey); v != nil {
		if raw, ok := v.(bool); ok {
			return raw
		}
	}
	return false
}
