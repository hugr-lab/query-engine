package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	qetypes "github.com/hugr-lab/query-engine/types"
)

// The embedding-text helpers below are worded exactly as the retired
// compiled-schema storage worded them: vectors seeded before the switch stay
// comparable to the ones seeded after it, so a CoreDB carried across the
// migration keeps ranking the same.

// embeddingText returns the best available text to embed, falling back through
// longDesc → desc → syntheticDesc (an empty result means nothing to embed).
func embeddingText(longDesc, desc, syntheticDesc string) string {
	if longDesc != "" {
		return longDesc
	}
	if desc != "" {
		return desc
	}
	return syntheticDesc
}

// syntheticDescription builds a fallback description from entity metadata, used
// when no human-written description is available — the seed embedding text for
// a description-less entity.
func syntheticDescription(hugrType, entityName, parentName, moduleName, catalog string) string {
	var parts []string
	if hugrType != "" {
		parts = append(parts, hugrType)
	}
	parts = append(parts, entityName)
	if parentName != "" {
		parts = append(parts, fmt.Sprintf("on %s", parentName))
	}
	if moduleName != "" {
		parts = append(parts, fmt.Sprintf("in module %s", moduleName))
	}
	if catalog != "" {
		parts = append(parts, fmt.Sprintf("from catalog %s", catalog))
	}
	return strings.Join(parts, " ")
}

// embed computes an embedding vector for text, or (nil, nil) when embeddings
// are not configured (no embedder or zero vector size) or the text is empty —
// the caller then writes a curation row without a vector, leaving any load-time
// seed vector untouched.
func (s *Store) embed(ctx context.Context, text string) (qetypes.Vector, error) {
	if s.embedder == nil || s.vecSize == 0 || text == "" {
		return nil, nil
	}
	res, err := s.embedder.CreateEmbedding(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("embed %q: %w", text, err)
	}
	return res.Vector, nil
}

// embedBatchSize bounds one CreateEmbeddings call — embedding backends cap
// their batch sizes, and a large source (thousands of fields) would otherwise
// go out in a single oversized request and fail wholesale.
const embedBatchSize = 256

// embedBatch computes embedding vectors for many texts — the seed path embeds
// every entity of a source, chunked by embedBatchSize. Returns nil when
// embeddings are not configured.
func (s *Store) embedBatch(ctx context.Context, texts []string) ([]qetypes.Vector, error) {
	if s.embedder == nil || s.vecSize == 0 || len(texts) == 0 {
		return nil, nil
	}
	out := make([]qetypes.Vector, 0, len(texts))
	for start := 0; start < len(texts); start += embedBatchSize {
		end := min(start+embedBatchSize, len(texts))
		res, err := s.embedder.CreateEmbeddings(ctx, texts[start:end])
		if err != nil {
			return nil, fmt.Errorf("embed batch %d..%d of %d: %w", start, end, len(texts), err)
		}
		out = append(out, res.Vectors...)
	}
	return out, nil
}

// vecLiteral renders a vector as a cross-engine SQL STRING literal
// '[v1,v2,...]'. This is the ONLY vec write form that casts on BOTH CoreDB
// backends: DuckDB casts VARCHAR→FLOAT[N], and an attached PostgreSQL casts
// text→vector(N) through the postgres extension — pinned by the CoreDB
// statement inventory (core-db/hugr_catalog_test.go probe E1/E2). A typed
// DuckDB array literal (ARRAY[...]::FLOAT[N]) would NOT reach a pgvector column.
// Fixed-notation formatting at float32 precision (the column width) keeps the
// text free of exponents both backends would have to re-parse.
func vecLiteral(vec qetypes.Vector) string {
	parts := make([]string, len(vec))
	for i, f := range vec {
		parts[i] = strconv.FormatFloat(f, 'f', -1, 32)
	}
	return lit("[" + strings.Join(parts, ",") + "]")
}
