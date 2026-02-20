package planner

import (
	"context"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/compiler"
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
	case compiler.IsFunctionCallQuery(query):
		node, err = functionCallRootNode(ctx, provider, s.engines, query, vars)
	case compiler.IsSelectQuery(query):
		node, err = selectDataObjectRootNode(ctx, provider, s.engines, query, vars)
	case compiler.IsSelectOneQuery(query):
		node, err = selectDataObjectRootNode(ctx, provider, s.engines, query, vars)
	case compiler.IsAggregateQuery(query), compiler.IsBucketAggregateQuery(query):
		node, err = aggregateRootNode(ctx, provider, s.engines, query, vars)
	case compiler.IsInsertQuery(query):
		node, err = insertRootNode(ctx, provider, s.engines, query, vars)
	case compiler.IsUpdateQuery(query):
		node, err = updateRootNode(ctx, provider, s.engines, query, vars)
	case compiler.IsDeleteQuery(query):
		node, err = deleteRootNode(ctx, provider, s.engines, query, vars)
	case compiler.IsH3Query(query):
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
