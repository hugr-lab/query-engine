package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*EmbeddingsRule)(nil)

type EmbeddingsRule struct{}

func (r *EmbeddingsRule) Name() string     { return "EmbeddingsRule" }
func (r *EmbeddingsRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *EmbeddingsRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("embeddings") != nil
}

func (r *EmbeddingsRule) Process(_ base.CompilationContext, _ *ast.Definition) error {
	// Stub: embeddings/vector-search generation will be fleshed out
	// incrementally. The full implementation will:
	//   - Read embedding configuration from the @embeddings directive
	//   - Generate vector search query types (similarity search input,
	//     result wrapper with distance/score fields)
	//   - Register vector search query fields
	return nil
}
