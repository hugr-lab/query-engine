package metadata

import (
	"context"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	ErrInvalidMetaDataQuery = errors.New("invalid query type")
	ErrInvalidTypeQuery     = errors.New("invalid type query")
)

func ProcessQuery(ctx context.Context, schema *ast.Schema, query compiler.QueryRequest, maxDepth int, vars map[string]any) (any, error) {
	if query.QueryType != compiler.QueryTypeMeta {
		return nil, ErrInvalidMetaDataQuery
	}
	if query.Field == nil {
		return nil, ErrInvalidMetaDataQuery
	}

	switch query.Field.Name {
	case "__schema":
		return processSchemaQuery(ctx, schema, query.Field, maxDepth)
	case "__type":
		return processTypeQuery(ctx, schema, query.Field, maxDepth, vars)
	}

	return nil, nil
}

func processTypeQuery(ctx context.Context, schema *ast.Schema, field *ast.Field, maxDepth int, vars map[string]any) (any, error) {
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
	tn := typeName.(string)
	if tn == "" {
		return nil, ErrInvalidTypeQuery
	}

	if _, ok := schema.Types[tn]; !ok {
		return nil, ErrTypeNotFound
	}

	return typeResolver(ctx, schema, ast.NamedType(tn, &ast.Position{}), field.SelectionSet, maxDepth)
}
