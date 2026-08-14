package viz

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/hugr-lab/query-engine/types"
)

// Querier is the one engine capability the pipeline needs; types.Querier
// satisfies it.
type Querier interface {
	Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error)
}

// QueryRows runs a read-only GraphQL query, optionally shapes it with jq,
// and canonicalizes the outcome into flat rows.
func QueryRows(ctx context.Context, q Querier, query string, vars map[string]any, jqTransform string, maxRows int, ttl time.Duration) ([]map[string]any, bool, error) {
	v, err := QueryValue(ctx, q, query, vars, jqTransform, ttl)
	if err != nil {
		return nil, false, err
	}
	rows, err := CanonicalRows(v)
	if err != nil {
		return nil, false, err
	}
	if maxRows > 0 && len(rows) > maxRows {
		return rows[:maxRows], true, nil
	}
	return rows, false, nil
}

// QueryValue is the shared query pipeline: read-only hint, cache hint, jq
// fail-fast compile, execute, transform, and a JSON round-trip that
// normalizes engine/gojq value types into plain maps. What the value MEANS —
// rows or KPI cards — is the caller's canonicalizer's business.
func QueryValue(ctx context.Context, q Querier, query string, vars map[string]any, jqTransform string, ttl time.Duration) (any, error) {
	ctx = types.ContextWithQueryHint(ctx, types.NoMutationHint())
	if ttl > 0 {
		// Caching is the deployment's call (MCP_QUERY_TTL), not the caller's,
		// so nothing about it appears in any tool surface. No key of our own
		// either: the engine derives one from the query and its variables,
		// which is exactly the identity we would have had to reconstruct —
		// and cannot then disagree with what it runs.
		ctx = types.ContextWithQueryHint(ctx, types.QueryCacheHint("", ttl))
	}
	if vars == nil {
		vars = map[string]any{}
	}

	// Compile before executing — fail fast on a bad expression.
	var transformer *jq.Transformer
	if jqTransform != "" {
		var err error
		transformer, err = jq.NewTransformer(ctx, jqTransform, jq.WithVariables(vars), jq.WithQuerier(q))
		if err != nil {
			return nil, fmt.Errorf("jq compile: %w", err)
		}
	}

	res, err := q.Query(ctx, query, vars)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	if rerr := res.Err(); rerr != nil {
		return nil, fmt.Errorf("query error: %w", rerr)
	}
	defer res.Close()

	// The transform runs over the whole response envelope, exactly as in
	// data-inline_graphql_result — hence the leading .data in every path.
	var data any = res.Data
	if transformer != nil {
		data, err = transformer.Transform(ctx, res, nil)
		if err != nil {
			return nil, fmt.Errorf("jq transform: %w (the jq input is the full response envelope, so paths start with .data)", err)
		}
		if data == nil {
			return nil, fmt.Errorf("jq transform produced null — the jq input is the full response envelope, so paths start with .data (e.g. .data.%s)", JQPathHint(query))
		}
	}

	// JSON round-trip normalizes engine/gojq value types into plain maps.
	b, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("normalize result: %w", err)
	}

	return v, nil
}
