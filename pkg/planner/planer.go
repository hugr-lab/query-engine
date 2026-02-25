package planner

import (
	"context"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/schema/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	ErrInternalPlanner = errors.New("internal planner error")
)

var defaultEngine = &engines.DuckDB{}

type Catalog interface {
	Engine(name string) (engines.Engine, error)
}

type Service struct {
	engines Catalog
	querier types.Querier
}

func New(c Catalog, q types.Querier) *Service {
	return &Service{
		engines: c,
		querier: q,
	}
}

func (s *Service) Plan(ctx context.Context, provider schema.Provider, query *ast.Field, vars map[string]interface{}) (*QueryPlan, error) {
	var node *QueryPlanNode
	var err error
	switch {
	case sdl.IsFunctionCallQuery(query):
		node, err = functionCallRootNode(ctx, provider, s.engines, query, vars)
	case sdl.IsSelectQuery(query):
		node, err = selectDataObjectRootNode(ctx, provider, s.engines, query, vars)
	case sdl.IsSelectOneQuery(query):
		node, err = selectDataObjectRootNode(ctx, provider, s.engines, query, vars)
	case sdl.IsAggregateQuery(query), sdl.IsBucketAggregateQuery(query):
		node, err = aggregateRootNode(ctx, provider, s.engines, query, vars)
	case sdl.IsInsertQuery(query):
		node, err = insertRootNode(ctx, provider, s.engines, query, vars)
	case sdl.IsUpdateQuery(query):
		node, err = updateRootNode(ctx, provider, s.engines, query, vars)
	case sdl.IsDeleteQuery(query):
		node, err = deleteRootNode(ctx, provider, s.engines, query, vars)
	case sdl.IsH3Query(query):
		node, err = h3RootNode(ctx, provider, s.engines, query, vars)
	default:
		return nil, errors.New("unsupported query type")
	}
	if err != nil {
		return nil, err
	}
	node.provider = provider
	node.engines = s.engines
	node.querier = s.querier

	return &QueryPlan{Query: query, RootNode: node}, nil
}
