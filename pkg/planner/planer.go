package planner

import (
	"context"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
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
}

func New(c Catalog) *Service {
	return &Service{
		engines: c,
	}
}

func (s *Service) Plan(ctx context.Context, schema *ast.Schema, query *ast.Field, vars map[string]interface{}) (*QueryPlan, error) {
	var node *QueryPlanNode
	var err error
	switch {
	case compiler.IsFunctionCallQuery(query):
		node, err = functionCallRootNode(ctx, schema, s.engines, query, vars)
	case compiler.IsSelectQuery(query):
		node, err = selectDataObjectRootNode(ctx, schema, s.engines, query, vars)
	case compiler.IsSelectOneQuery(query):
		node, err = selectDataObjectRootNode(ctx, schema, s.engines, query, vars)
	case compiler.IsAggregateQuery(query), compiler.IsBucketAggregateQuery(query):
		node, err = aggregateRootNode(ctx, schema, s.engines, query, vars)
	case compiler.IsInsertQuery(query):
		node, err = insertRootNode(ctx, schema, s.engines, query, vars)
	case compiler.IsUpdateQuery(query):
		node, err = updateRootNode(ctx, schema, s.engines, query, vars)
	case compiler.IsDeleteQuery(query):
		node, err = deleteRootNode(ctx, schema, s.engines, query, vars)
	case compiler.IsH3Query(query):
		node, err = h3RootNode(ctx, schema, s.engines, query, vars)
	default:
		return nil, errors.New("unsupported query type")
	}
	if err != nil {
		return nil, err
	}
	node.schema = schema
	node.engines = s.engines

	return &QueryPlan{Query: query, RootNode: node}, nil
}
