package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/types"
)

// Embedder is an optional dependency for computing embedding vectors.
// Defined in this package to avoid import cycles.
// The engine wires it to an EmbeddingSource at init time.
type Embedder interface {
	CreateEmbedding(ctx context.Context, input string) (types.Vector, error)
	CreateEmbeddings(ctx context.Context, inputs []string) ([]types.Vector, error)
}

// computeEmbeddings computes embedding vectors for a batch of texts.
// Returns zero-length vectors for each input if the embedder is nil or vecSize is 0.
func (p *Provider) computeEmbeddings(ctx context.Context, texts []string) ([]types.Vector, error) {
	if p.embedder == nil || p.vecSize == 0 || len(texts) == 0 {
		return make([]types.Vector, len(texts)), nil
	}
	return p.embedder.CreateEmbeddings(ctx, texts)
}

// EmbeddingText returns the best available text for embedding generation.
// Falls back through: longDesc → desc → syntheticDesc.
func EmbeddingText(longDesc, desc, syntheticDesc string) string {
	if longDesc != "" {
		return longDesc
	}
	if desc != "" {
		return desc
	}
	return syntheticDesc
}

// SyntheticDescription builds a fallback description from entity metadata.
// Used when no human-written description is available.
func SyntheticDescription(hugrType, entityName, parentName, moduleName, catalog string) string {
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
