package queries

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/types"
)

func CreateEmbedding(ctx context.Context, qe types.Querier, model, text string) (types.Vector, error) {
	res, err := qe.Query(auth.ContextWithFullAccess(ctx), `
		query ($model: String!, $input: String!) {
			function {
				core {
					model {
						embedding(model: $model, input: $input) @cache(ttl: 300) {
							vector
						}
					}
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
	var out types.EmbeddingResult
	err = res.ScanData("function..model.embedding", &out)
	if err != nil {
		return nil, fmt.Errorf("failed to get embedding from model %s: %w", model, err)
	}
	if len(out.Vector) == 0 {
		return nil, fmt.Errorf("model %s returned empty embedding", model)
	}
	return out.Vector, nil
}
