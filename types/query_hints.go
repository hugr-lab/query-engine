package types

import "context"

type queryHintContextKeyType int

const (
	queryValidateOnlyHint queryHintContextKeyType = iota + 1
	queryNoMutationHint
)

type QueryHint func(context.Context) context.Context

func ContextWithQueryHint(ctx context.Context, hint ...QueryHint) context.Context {
	for _, h := range hint {
		ctx = h(ctx)
	}
	return ctx
}

func ValidateOnlyHint() QueryHint {
	return func(ctx context.Context) context.Context {
		return context.WithValue(ctx, queryValidateOnlyHint, true)
	}
}

func NoMutationHint() QueryHint {
	return func(ctx context.Context) context.Context {
		return context.WithValue(ctx, queryNoMutationHint, true)
	}
}

func IsValidateOnlyContext(ctx context.Context) bool {
	v := ctx.Value(queryValidateOnlyHint)
	if v == nil {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

func IsNoMutationContext(ctx context.Context) bool {
	v := ctx.Value(queryNoMutationHint)
	if v == nil {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}
