package cache

import (
	"bytes"
	"strconv"
	"time"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/sdl"
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

func QueryInfo(field *ast.Field, vars map[string]any) Info {
	if field == nil {
		return Info{}
	}

	// If query check if need cache
	if field.Directives.ForName(base.CacheDirectiveName) == nil &&
		field.Definition.Directives.ForName(base.CacheDirectiveName) == nil {
		return Info{}
	}

	info := cacheDirectiveInfo(field.Directives.ForName(base.CacheDirectiveName), vars)
	info.Merge(
		cacheDirectiveInfo(field.Definition.Directives.ForName(base.CacheDirectiveName), vars),
	)
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

	ttl, _ := strconv.Atoi(sdl.DirectiveArgValue(d, "ttl", vars))

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
