package planner

import (
	"context"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/types"
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
	case sdl.IsScalarType(p.Query.Definition.Type.Name()) &&
		p.Query.Definition.Type.NamedType == "":
		return db.QueryJsonScalarArray(ctx, p.CompiledQuery, p.Params...)
	case p.Query.Definition.Type.NamedType == "":
		// List-typed fields always use the native-Arrow path (wrap=false).
		// Attach per-field geometry metadata so RecordToJSON and the Arrow
		// scanner know how to render / decode each column without peeking
		// at bytes.
		result, err := db.QueryArrowTable(ctx, p.CompiledQuery, false, p.Params...)
		if err != nil {
			return nil, err
		}
		if tbl, ok := result.(types.ArrowTable); ok {
			if gi := GeomInfoFromField(p.Query); gi != nil {
				tbl.SetGeometryInfo(gi)
			}
		}
		return result, nil
	case sdl.IsScalarType(p.Query.Definition.Type.Name()):
		return db.QueryScalarValue(ctx, p.CompiledQuery, p.Params...)
	default:
		return db.QueryJsonRow(ctx, p.CompiledQuery, p.Params...)
	}
}

func (p *QueryPlan) ExecuteStream(ctx context.Context, db *db.Pool) (types.ArrowTable, func(), error) {
	if p.CompiledQuery == "" {
		return nil, nil, errors.New("no compiled query")
	}
	if p.RootNode.Before != nil {
		err := p.RootNode.Before(ctx, db, p.RootNode)
		if err != nil {
			return nil, nil, err
		}
	}

	tbl, done, err := db.QueryTableStream(ctx, p.CompiledQuery, p.Params...)
	if err != nil {
		return nil, nil, err
	}
	if tbl != nil {
		if gi := GeomInfoFromField(p.Query); gi != nil {
			tbl.SetGeometryInfo(gi)
		}
	}
	return tbl, done, nil
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

	provider catalog.Provider
	engines  Catalog
	querier  types.Querier

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

func (n *QueryPlanNode) SchemaProvider() catalog.Provider {
	if n.provider != nil {
		return n.provider
	}
	if n.Parent != nil {
		return n.Parent.SchemaProvider()
	}
	return nil
}

func (n *QueryPlanNode) TypeDefs() base.DefinitionsSource {
	return n.SchemaProvider()
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

func (n *QueryPlanNode) Querier() types.Querier {
	if n.querier != nil {
		return n.querier
	}
	if n.Parent != nil {
		return n.Parent.Querier()
	}
	return nil
}

type NodeFunc func(node *QueryPlanNode, children Results, params []any) (string, []any, error)
