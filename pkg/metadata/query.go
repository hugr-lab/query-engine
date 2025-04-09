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

func ProcessQuery(ctx context.Context, schema *ast.Schema, query compiler.QueryRequest, maxDepth int) (any, error) {
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
		return processTypeQuery(ctx, schema, query.Field, maxDepth)
	}

	return nil, nil
}

func processTypeQuery(ctx context.Context, schema *ast.Schema, field *ast.Field, maxDepth int) (any, error) {
	if field.Arguments == nil || field.Arguments.ForName("name") == nil {
		return nil, ErrInvalidTypeQuery
	}

	typeName := field.Arguments.ForName("name").Value.Raw
	if typeName == "" {
		return nil, ErrInvalidTypeQuery
	}

	if _, ok := schema.Types[typeName]; !ok {
		return nil, ErrTypeNotFound
	}

	return typeResolver(ctx, schema, ast.NamedType(typeName, &ast.Position{}), field.SelectionSet, maxDepth)
}
