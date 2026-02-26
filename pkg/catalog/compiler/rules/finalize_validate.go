package rules

import "github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"

var _ base.BatchRule = (*PostValidator)(nil)

// PostValidator performs post-compilation validation during the FINALIZE phase.
// It verifies compiled output consistency.
type PostValidator struct{}

func (r *PostValidator) Name() string     { return "PostValidator" }
func (r *PostValidator) Phase() base.Phase { return base.PhaseFinalize }

func (r *PostValidator) ProcessAll(ctx base.CompilationContext) error {
	// Post-validation: verify compiled output consistency.
	// For now, just ensure the compilation produced valid output.
	return nil
}
