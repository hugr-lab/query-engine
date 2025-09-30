package queries

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/types"
)

func CreateEmbedding(ctx context.Context, qe types.Querier, model, text string) (types.Vector, error) {
	res, err := qe.Query(auth.ContextWithFullAccess(ctx), `
		query ($model: String!, $input: String!) {
			function {
				core {
					vector: create_embedding(model: $model, input: $input) @cache(ttl: 300)
				}
			}
		}`, map[string]any{
		"model": model,
		"input": text,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get embedding from model %s: %w", model, err)
	}
	defer res.Close()
	var vec types.Vector
	err = res.ScanData("function.core.vector", &vec)
	if err != nil {
		return nil, fmt.Errorf("failed to get embedding from model %s: %w", model, err)
	}
	if len(vec) == 0 {
		return nil, fmt.Errorf("model %s returned empty embedding", model)
	}
	return vec, nil
}
