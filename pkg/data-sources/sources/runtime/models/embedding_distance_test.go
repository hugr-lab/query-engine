//go:build duckdb_arrow

package models_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	models "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/models"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

// stubEmbedder implements sources.Source + sources.EmbeddingSource, returning
// fixed vectors per input string so distances are fully deterministic.
type stubEmbedder struct {
	name string
	vecs map[string]types.Vector
}

func (s *stubEmbedder) Name() string                               { return s.name }
func (s *stubEmbedder) Definition() types.DataSource               { return types.DataSource{} }
func (s *stubEmbedder) Engine() engines.Engine                     { return engines.NewDuckDB() }
func (s *stubEmbedder) IsAttached() bool                           { return true }
func (s *stubEmbedder) ReadOnly() bool                             { return true }
func (s *stubEmbedder) Attach(_ context.Context, _ *db.Pool) error { return nil }
func (s *stubEmbedder) Detach(_ context.Context, _ *db.Pool) error { return nil }
func (s *stubEmbedder) ModelInfo() types.ModelInfo {
	return types.ModelInfo{Name: s.name, Type: "embedding"}
}

func (s *stubEmbedder) CreateEmbedding(_ context.Context, input string) (*types.EmbeddingResult, error) {
	v, ok := s.vecs[input]
	if !ok {
		return nil, fmt.Errorf("no vector for %q", input)
	}
	return &types.EmbeddingResult{Vector: v}, nil
}

func (s *stubEmbedder) CreateEmbeddings(_ context.Context, inputs []string) (*types.EmbeddingsResult, error) {
	out := make([]types.Vector, len(inputs))
	for i, in := range inputs {
		v, ok := s.vecs[in]
		if !ok {
			return nil, fmt.Errorf("no vector for %q", in)
		}
		out[i] = v
	}
	return &types.EmbeddingsResult{Vectors: out}, nil
}

// plainSource implements sources.Source but NOT sources.EmbeddingSource.
type plainSource struct{ name string }

func (s *plainSource) Name() string                               { return s.name }
func (s *plainSource) Definition() types.DataSource               { return types.DataSource{} }
func (s *plainSource) Engine() engines.Engine                     { return engines.NewDuckDB() }
func (s *plainSource) IsAttached() bool                           { return true }
func (s *plainSource) ReadOnly() bool                             { return true }
func (s *plainSource) Attach(_ context.Context, _ *db.Pool) error { return nil }
func (s *plainSource) Detach(_ context.Context, _ *db.Pool) error { return nil }

// fakeResolver resolves sources by name from a fixed map.
type fakeResolver struct{ m map[string]sources.Source }

func (r fakeResolver) Resolve(name string) (sources.Source, error) {
	s, ok := r.m[name]
	if !ok {
		return nil, fmt.Errorf("data source %q not found", name)
	}
	return s, nil
}

func (r fakeResolver) ResolveAll() []sources.Source {
	out := make([]sources.Source, 0, len(r.m))
	for _, s := range r.m {
		out = append(out, s)
	}
	return out
}

// newTestSource spins up the core.models source against an in-memory DuckDB with
// a stub embedding source ("emb", with fixed vectors) and a non-embedding source
// ("plain"). Returns the pool for issuing queries.
func newTestSource(t *testing.T) *db.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	src := models.New()
	src.DataSourceServiceSetup(fakeResolver{m: map[string]sources.Source{
		"emb": &stubEmbedder{name: "emb", vecs: map[string]types.Vector{
			"a":    {1, 0, 0},
			"a2":   {1, 0, 0},     // identical to "a"
			"sim":  {0.9, 0.1, 0}, // close to "a", non-orthogonal (nonzero dot)
			"orth": {0, 1, 0},     // orthogonal to "a"
			"zero": {0, 0, 0},     // zero-norm (cosine undefined)
		}},
		"plain": &plainSource{name: "plain"},
	}})
	if err := src.Attach(ctx, pool); err != nil {
		t.Fatalf("attach: %v", err)
	}
	return pool
}

func scalar(t *testing.T, pool *db.Pool, query string) (float64, error) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	var v float64
	err = conn.QueryRow(ctx, query).Scan(&v)
	return v, err
}

// T004 [US1]: a single call returns a distance; identical → ~0, related < unrelated.
func TestEmbeddingDistance_Behavior(t *testing.T) {
	pool := newTestSource(t)

	same, err := scalar(t, pool, `SELECT core_models_embedding_distance('emb','a','a2','Cosine')`)
	if err != nil {
		t.Fatalf("identical: %v", err)
	}
	if math.Abs(same) > 1e-6 {
		t.Errorf("identical strings: cosine distance = %g, want ~0", same)
	}

	related, err := scalar(t, pool, `SELECT core_models_embedding_distance('emb','a','sim','Cosine')`)
	if err != nil {
		t.Fatalf("related: %v", err)
	}
	unrelated, err := scalar(t, pool, `SELECT core_models_embedding_distance('emb','a','orth','Cosine')`)
	if err != nil {
		t.Fatalf("unrelated: %v", err)
	}
	if !(related < unrelated) {
		t.Errorf("expected related (%g) < unrelated (%g)", related, unrelated)
	}
}

// T005/T006 [US2]: each metric equals a direct array_* call over the same vectors.
func TestEmbeddingDistance_ParityWithVectorSearch(t *testing.T) {
	pool := newTestSource(t)

	cases := []struct {
		metric string
		arrFn  string
	}{
		{"L2", "array_distance"},
		{"Cosine", "array_cosine_distance"},
		{"Inner", "array_negative_inner_product"},
	}
	// Use non-orthogonal vectors ("a"=[1,0,0], "sim"=[0.9,0.1,0]) so every metric
	// has a distinctive non-zero expected value — in particular Inner = -0.9, not 0
	// (orthogonal inputs would make the Inner case a meaningless 0==0 comparison).
	for _, c := range cases {
		t.Run(c.metric, func(t *testing.T) {
			got, err := scalar(t, pool,
				fmt.Sprintf(`SELECT core_models_embedding_distance('emb','a','sim','%s')`, c.metric))
			if err != nil {
				t.Fatalf("function: %v", err)
			}
			want, err := scalar(t, pool,
				fmt.Sprintf(`SELECT %s([1,0,0]::FLOAT[3], [0.9,0.1,0]::FLOAT[3])`, c.arrFn))
			if err != nil {
				t.Fatalf("direct %s: %v", c.arrFn, err)
			}
			if math.Abs(got-want) > 1e-6 {
				t.Errorf("%s: function=%g, direct %s=%g (must match)", c.metric, got, c.arrFn, want)
			}
		})
	}
}

// T008 [US3]: bad inputs return errors carrying the documented wording, no value.
func TestEmbeddingDistance_Errors(t *testing.T) {
	pool := newTestSource(t)

	cases := []struct {
		name, query, wantSubstr string
	}{
		{"unknown model", `SELECT core_models_embedding_distance('missing','a','orth','Cosine')`, "not found"},
		{"non-embedding source", `SELECT core_models_embedding_distance('plain','a','orth','Cosine')`, "is not an embedding source"},
		{"unsupported metric", `SELECT core_models_embedding_distance('emb','a','orth','Nope')`, "unsupported distance metric"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := scalar(t, pool, c.query)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstr) {
				t.Errorf("error = %q, want substring %q", err.Error(), c.wantSubstr)
			}
		})
	}
}

// T006 / research R6 [US2]: a zero-norm vector under Cosine. DuckDB's
// array_cosine_distance returns the maximum distance 2.0 for an undefined
// (zero-norm) cosine — it does not error or fabricate a small value. This test
// locks that observed engine behavior (same value vector search would yield).
func TestEmbeddingDistance_ZeroNormCosine(t *testing.T) {
	pool := newTestSource(t)
	got, err := scalar(t, pool, `SELECT core_models_embedding_distance('emb','a','zero','Cosine')`)
	if err != nil {
		t.Fatalf("zero-norm cosine: %v", err)
	}
	if math.Abs(got-2.0) > 1e-6 {
		t.Errorf("zero-norm cosine = %g, want 2.0 (DuckDB max cosine distance)", got)
	}
}

// US2: every metric is symmetric, and Inner self-distance of a unit vector = -1.
func TestEmbeddingDistance_SymmetryAndSelf(t *testing.T) {
	pool := newTestSource(t)

	for _, m := range []string{"L2", "Cosine", "Inner"} {
		ab, err := scalar(t, pool, fmt.Sprintf(`SELECT core_models_embedding_distance('emb','a','sim','%s')`, m))
		if err != nil {
			t.Fatalf("%s a,sim: %v", m, err)
		}
		ba, err := scalar(t, pool, fmt.Sprintf(`SELECT core_models_embedding_distance('emb','sim','a','%s')`, m))
		if err != nil {
			t.Fatalf("%s sim,a: %v", m, err)
		}
		if math.Abs(ab-ba) > 1e-6 {
			t.Errorf("%s not symmetric: d(a,sim)=%g d(sim,a)=%g", m, ab, ba)
		}
	}

	// Inner self-distance for unit vector a=[1,0,0] is -(a·a) = -1.
	self, err := scalar(t, pool, `SELECT core_models_embedding_distance('emb','a','a2','Inner')`)
	if err != nil {
		t.Fatalf("inner self: %v", err)
	}
	if math.Abs(self-(-1.0)) > 1e-6 {
		t.Errorf("inner self-distance = %g, want -1", self)
	}
}
