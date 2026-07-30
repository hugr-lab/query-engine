package metadata

import (
	"context"
	"errors"
	"log/slog"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	qetypes "github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	ErrInvalidMetaDataQuery = errors.New("invalid query type")
	ErrInvalidTypeQuery     = errors.New("invalid type query")
)

// Querier is the engine seam the search resolver needs: _search ranks over the
// catalog's own entity views, which is a GraphQL query the engine executes for
// itself. Narrower than types.Querier on purpose — this package has no
// business loading data sources, and a one-method seam is trivial to fake in a
// test.
type Querier interface {
	Query(ctx context.Context, query string, vars map[string]any) (*qetypes.Response, error)
}

// ProcessQuery resolves one meta query. The querier is nil for every path
// except _search (and may legitimately be nil there too — a caller that has no
// engine to re-enter gets a clear error rather than a panic).
func ProcessQuery(ctx context.Context, provider catalog.Provider, query sdl.QueryRequest, maxDepth int, vars map[string]any, querier Querier) (any, error) {
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
	case sdl.MetadataCatalogQuery:
		res, err := processCatalogQuery(ctx, provider, query.Field, maxDepth)
		if err != nil {
			slog.Error("metadata _catalog query failed", "error", err)
		}
		return res, err
	case sdl.MetadataModuleQuery:
		res, err := processModuleQuery(ctx, provider, query.Field, maxDepth, vars)
		if err != nil {
			slog.Error("metadata _module query failed", "error", err)
		}
		return res, err
	case sdl.MetadataDataObjectQuery:
		res, err := processDataObjectQuery(ctx, provider, query.Field, maxDepth, vars)
		if err != nil {
			slog.Error("metadata _dataObject query failed", "error", err)
		}
		return res, err
	case sdl.MetadataFunctionQuery:
		res, err := processFunctionQuery(ctx, provider, query.Field, maxDepth, vars)
		if err != nil {
			slog.Error("metadata _function query failed", "error", err)
		}
		return res, err
	case sdl.MetadataDataSourcesQuery:
		res, err := processDataSourcesQuery(ctx, provider, query.Field, maxDepth)
		if err != nil {
			slog.Error("metadata _dataSources query failed", "error", err)
		}
		return res, err
	case sdl.MetadataDataSourceQuery:
		res, err := processDataSourceQuery(ctx, provider, query.Field, maxDepth, vars)
		if err != nil {
			slog.Error("metadata _dataSource query failed", "error", err)
		}
		return res, err
	case sdl.MetadataTypesQuery:
		res, err := processTypesQuery(ctx, provider, query.Field, maxDepth, vars)
		if err != nil {
			slog.Error("metadata _types query failed", "error", err)
		}
		return res, err
	case sdl.MetadataSearchQuery:
		res, err := processSearchQuery(ctx, provider, querier, query.Field, maxDepth, vars)
		if err != nil {
			slog.Error("metadata _search query failed", "error", err)
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
		slog.Debug("metadata __type: type not found", "name", tn)
		return nil, nil
	}

	return typeResolver(ctx, provider, ast.NamedType(tn, &ast.Position{}), field.SelectionSet, maxDepth)
}
