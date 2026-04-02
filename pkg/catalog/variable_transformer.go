package catalog

import (
	"context"
	"fmt"
	"maps"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/jq"
)

type jqVariableTransformer struct {
	querier Querier
}

// NewJQVariableTransformer creates a VariableTransformer that processes _jq expressions.
func NewJQVariableTransformer(querier Querier) VariableTransformer {
	return &jqVariableTransformer{querier: querier}
}

func (t *jqVariableTransformer) TransformVariables(ctx context.Context, vars map[string]any) (map[string]any, error) {
	if len(vars) == 0 {
		return vars, nil
	}
	jqExpr, ok := vars["_jq"]
	if !ok {
		return vars, nil
	}
	expr, ok := jqExpr.(string)
	if !ok {
		return nil, fmt.Errorf("invalid _jq variable: expected string")
	}

	var opts []jq.Option
	if t.querier != nil {
		opts = append(opts, jq.WithQuerier(t.querier))
	}
	transformer, err := jq.NewTransformer(db.ClearTxContext(ctx), expr, opts...)
	if err != nil {
		return nil, fmt.Errorf("jq compiler for variables: %w", err)
	}
	result, err := transformer.Transform(ctx, vars, nil)
	if err != nil {
		return nil, fmt.Errorf("jq transform for variables: %w", err)
	}
	res, ok := result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid _jq result: expected object")
	}
	delete(res, "_jq")
	maps.Copy(vars, res)
	return vars, nil
}
