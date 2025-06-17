package planner

import (
	"context"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

type QueryPlan struct {
	Query    *ast.Field
	RootNode *QueryPlanNode

	CompiledQuery string
	Params        []any
}

func (p *QueryPlan) Compile() error {
	res, err := p.RootNode.Compile(p.RootNode, nil)
	if err != nil {
		return err
	}
	p.CompiledQuery = res.Result
	p.Params = res.Params
	p.RootNode.plan = p
	return nil
}

func (p *QueryPlan) Execute(ctx context.Context, db *db.Pool) (data interface{}, err error) {
	if p.CompiledQuery == "" {
		return nil, errors.New("no compiled query")
	}
	if p.RootNode.Before != nil {
		err = p.RootNode.Before(ctx, db, p.RootNode)
		if err != nil {
			return nil, err
		}
	}

	switch {
	case compiler.IsScalarType(p.Query.Definition.Type.Name()) &&
		p.Query.Definition.Type.NamedType == "":
		return db.QueryJsonScalarArray(ctx, p.CompiledQuery, p.Params...)
	case p.Query.Definition.Type.NamedType == "":
		return db.QueryArrowTable(ctx, p.CompiledQuery, !IsRawResultsQuery(ctx, p.Query), p.Params...)
	case compiler.IsScalarType(p.Query.Definition.Type.Name()):
		return db.QueryScalarValue(ctx, p.CompiledQuery, p.Params...)
	default:
		return db.QueryJsonRow(ctx, p.CompiledQuery, p.Params...)
	}
}

func (p *QueryPlan) Log() string {
	return p.CompiledQuery
}

// QueryPlan represents the execution plan for a query
type QueryPlanNode struct {
	Name        string
	Query       *ast.Field
	Nodes       QueryPlanNodes
	Comment     string
	CollectFunc NodeFunc

	BeforeParamsLen int
	AddedParams     int
	Parent          *QueryPlanNode

	Before NodeBeforeExecFunc

	schema  *ast.Schema
	engines Catalog

	plan *QueryPlan
}

type NodeBeforeExecFunc func(ctx context.Context, db *db.Pool, node *QueryPlanNode) error

type QueryPlanNodes []*QueryPlanNode

func (n *QueryPlanNodes) Add(node *QueryPlanNode) {
	*n = append(*n, node)
}

func (n *QueryPlanNodes) ForName(name string) *QueryPlanNode {
	for _, v := range *n {
		if v.Name == name {
			return v
		}
	}
	return nil
}

func (n *QueryPlanNode) Compile(parent *QueryPlanNode, res *Result) (*Result, error) {
	var children Results
	var params []any
	if res != nil {
		params = res.Params
	}
	var err error
	for _, v := range n.Nodes {
		if v == nil {
			return nil, errors.New("nil node in query plan")
		}
		v.BeforeParamsLen = len(params)
		v.Parent = n
		res, err = v.Compile(v, res)
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, errors.New("no plan node result")
		}
		v.AddedParams = len(res.Params) - v.BeforeParamsLen
		children = append(children, &Result{
			Name:     v.Name,
			Result:   res.Result,
			Params:   res.Params,
			Children: res.Children,
		})
		params = res.Params
	}

	q, p, err := n.CollectFunc(parent, children, params)
	if err != nil {
		return nil, err
	}
	return &Result{
		Name:     n.Name,
		Result:   q,
		Params:   p,
		Children: children,
	}, nil
}

type Result struct {
	Name     string
	Result   string
	Params   []any
	Children Results
}

type Results []*Result

func (r *Results) ForName(name string) *Result {
	for _, v := range *r {
		if v.Name == name {
			return v
		}
	}
	return nil
}

func (r *Results) FirstResult() *Result {
	if len(*r) == 0 {
		return nil
	}
	return (*r)[0]
}

func (n *QueryPlanNode) Schema() *ast.Schema {
	if n.schema != nil {
		return n.schema
	}
	if n.Parent != nil {
		return n.Parent.Schema()
	}
	return nil
}

func (n *QueryPlanNode) TypeDefs() compiler.DefinitionsSource {
	return compiler.SchemaDefs(n.Schema())
}

func (n *QueryPlanNode) Engine(name string) (engines.Engine, error) {
	if n.engines != nil {
		return n.engines.Engine(name)
	}
	if n.Parent != nil {
		return n.Parent.Engine(name)
	}
	return nil, errors.New("no data source found")
}

type NodeFunc func(node *QueryPlanNode, children Results, params []any) (string, []any, error)
