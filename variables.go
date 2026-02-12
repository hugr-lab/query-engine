package hugr

import (
	"context"
	"fmt"
	"maps"

	"github.com/hugr-lab/query-engine/pkg/jq"
)

func (s *Service) transformVariables(ctx context.Context, vars map[string]any) (map[string]any, bool, error) {
	if len(vars) == 0 {
		return vars, false, nil
	}

	it, ok := vars["_jq"]
	if !ok {
		return vars, false, nil
	}
	tt, ok := it.(string)
	if !ok {
		return nil, false, fmt.Errorf("invalid _jq variable: expected string")
	}

	t, err := jq.NewTransformer(ctx, tt, jq.WithQuerier(s))
	if err != nil {
		return nil, false, fmt.Errorf("JQ compiler for variables: %w", err)
	}
	transformed, err := t.Transform(ctx, vars, nil)
	if err != nil {
		return nil, false, fmt.Errorf("JQ transform for variables: %w", err)
	}
	res, ok := transformed.(map[string]any)
	if !ok {
		return nil, false, fmt.Errorf("invalid _jq variable: expected object result")
	}
	delete(res, "_jq")
	maps.Copy(vars, res)
	return vars, true, nil
}
