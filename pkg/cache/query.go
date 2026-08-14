package cache

import (
	"bytes"
	"context"
	"time"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	enginetypes "github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

type Info struct {
	Use        bool
	Key        string
	Tags       []string
	TTL        time.Duration
	Invalidate bool
}

// QueryInfo decides whether this field's result is cached, and under what
// key. Caching is asked for either by an @cache directive on the field or its
// definition, or — for a caller that passes a query through verbatim and so
// cannot add one — by a hint on the context. The directive wins where both are
// present, @no_cache disables either, and a mutation is never cached.
func QueryInfo(ctx context.Context, field *ast.Field, vars map[string]any) Info {
	if field == nil {
		return Info{}
	}

	hasDirective := field.Directives.ForName(base.CacheDirectiveName) != nil ||
		field.Definition.Directives.ForName(base.CacheDirectiveName) != nil
	hint, hasHint := enginetypes.QueryCacheFromContext(ctx)
	if !hasDirective && !hasHint {
		return Info{}
	}

	var info Info
	if hasDirective {
		info = cacheDirectiveInfo(field.Directives.ForName(base.CacheDirectiveName), vars)
		info.Merge(
			cacheDirectiveInfo(field.Definition.Directives.ForName(base.CacheDirectiveName), vars),
		)
	} else {
		info = Info{Key: hint.Key, TTL: hint.TTL}
	}
	if info.Key == "" {
		info.Key, _ = FieldKey(field, vars)
	}
	info.Use = field.Directives.ForName(base.NoCacheDirectiveName) == nil
	info.Invalidate = field.Directives.ForName(base.InvalidateCacheDirectiveName) != nil

	// If mutation check if need invalidate cache
	if sdl.IsInsertQuery(field) ||
		sdl.IsUpdateQuery(field) ||
		sdl.IsDeleteQuery(field) {
		info.Invalidate = true
		info.Use = false
	}

	return info
}

func FieldKey(field *ast.Field, vars map[string]any) (string, error) {
	if field == nil {
		return "", nil
	}
	var bb []byte
	w := bytes.NewBuffer(bb)
	formatter.NewFormatter(w).FormatQueryDocument(&ast.QueryDocument{
		Operations: []*ast.OperationDefinition{
			{Operation: "cached", SelectionSet: ast.SelectionSet{field}, Position: field.Position},
		},
		Position: field.Position,
	})
	return QueryKey(w.String(), vars)
}

func cacheDirectiveInfo(d *ast.Directive, vars map[string]any) Info {
	if d == nil {
		return Info{}
	}
	ttlStr := sdl.DirectiveArgValue(d, "ttl", vars)
	ttl, _ := types.ParseIntervalValue(ttlStr)

	return Info{
		Key:  sdl.DirectiveArgValue(d, "key", vars),
		Tags: sdl.DirectiveArgChildValues(d, "tags", vars),
		TTL:  time.Duration(ttl) * time.Second,
	}
}

func (i *Info) Merge(other Info) {
	if i == nil || other.Key == "" {
		return
	}
	i.Key = other.Key
	i.Tags = append(i.Tags, other.Tags...)
	if other.TTL != 0 {
		i.TTL = other.TTL
	}
}

func (i Info) Options() []Option {
	options := make([]Option, 0)
	if i.TTL != 0 {
		options = append(options, WithTTL(i.TTL))
	}
	if len(i.Tags) != 0 {
		options = append(options, WithTags(i.Tags...))
	}
	return options
}
