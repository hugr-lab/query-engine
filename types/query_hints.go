package types

import (
	"context"
	"time"
)

type queryHintContextKeyType int

const (
	queryValidateOnlyHint queryHintContextKeyType = iota + 1
	queryNoMutationHint
	queryCacheHint
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

// QueryCacheHint asks the engine to serve a read query from the result cache
// for ttl. It exists for callers that pass a query through verbatim and so
// cannot graft an @cache directive onto the right field. An @cache directive
// in the query always wins, @no_cache disables either, and a mutation is
// never cached.
//
// Leave key empty and the engine derives it from the query and its variables,
// so it follows the question by itself. Pass one only to share a single entry
// across queries that are not textually identical.
func QueryCacheHint(key string, ttl time.Duration) QueryHint {
	return func(ctx context.Context) context.Context {
		return context.WithValue(ctx, queryCacheHint, QueryCacheOptions{Key: key, TTL: ttl})
	}
}

type QueryCacheOptions struct {
	Key string
	TTL time.Duration
}

func QueryCacheFromContext(ctx context.Context) (QueryCacheOptions, bool) {
	v, ok := ctx.Value(queryCacheHint).(QueryCacheOptions)
	return v, ok && v.TTL > 0
}
