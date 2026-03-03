package metadata

import (
	"context"
	"errors"
	"log/slog"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	ErrInvalidMetaDataQuery = errors.New("invalid query type")
	ErrInvalidTypeQuery     = errors.New("invalid type query")
)

func ProcessQuery(ctx context.Context, provider catalog.Provider, query sdl.QueryRequest, maxDepth int, vars map[string]any) (any, error) {
	if query.QueryType != sdl.QueryTypeMeta {
		return nil, ErrInvalidMetaDataQuery
	}
	if query.Field == nil {
		return nil, ErrInvalidMetaDataQuery
	}

	slog.Debug("metadata query", "field", query.Field.Name, "maxDepth", maxDepth)

	switch query.Field.Name {
	case "__schema":
		res, err := processSchemaQuery(ctx, provider, query.Field, maxDepth)
		if err != nil {
			slog.Error("metadata __schema query failed", "error", err)
		}
		return res, err
	case "__type":
		res, err := processTypeQuery(ctx, provider, query.Field, maxDepth, vars)
		if err != nil {
			slog.Error("metadata __type query failed", "error", err)
		}
		return res, err
	}

	return nil, nil
}

func processTypeQuery(ctx context.Context, provider catalog.Provider, field *ast.Field, maxDepth int, vars map[string]any) (any, error) {
	if field.Arguments == nil || field.Arguments.ForName("name") == nil {
		return nil, ErrInvalidTypeQuery
	}
	args := field.ArgumentMap(vars)
	if args == nil {
		return nil, ErrInvalidTypeQuery
	}
	typeName, ok := args["name"]
	if !ok {
		return nil, ErrInvalidTypeQuery
	}
	tn, ok := typeName.(string)
	if !ok || tn == "" {
		return nil, ErrInvalidTypeQuery
	}

	if provider.ForName(ctx, tn) == nil {
		return nil, ErrTypeNotFound
	}

	return typeResolver(ctx, provider, ast.NamedType(tn, &ast.Position{}), field.SelectionSet, maxDepth)
}
