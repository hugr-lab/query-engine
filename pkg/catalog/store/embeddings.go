package store

import (
	"context"
	"fmt"

	qetypes "github.com/hugr-lab/query-engine/types"
)

// The store owns its own copies of the embedding-text helpers rather than
// importing the retiring catalog/db package: the entity storage is the
// long-term home of curation, and it must not depend on the compiled provider
// it replaces. The wording matches catalog/db so seeds and curations produced
// by either path embed to comparable vectors.

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
