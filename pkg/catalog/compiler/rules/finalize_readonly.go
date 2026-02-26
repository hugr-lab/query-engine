package rules

import "github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"

var _ base.BatchRule = (*ReadOnlyFinalizer)(nil)

// ReadOnlyFinalizer handles read-only mode cleanup during the FINALIZE phase.
// ReadOnly is primarily handled by rules not generating mutations in the first place;
// this rule is here for any final cleanup if needed.
type ReadOnlyFinalizer struct{}

func (r *ReadOnlyFinalizer) Name() string     { return "ReadOnlyFinalizer" }
func (r *ReadOnlyFinalizer) Phase() base.Phase { return base.PhaseFinalize }

func (r *ReadOnlyFinalizer) ProcessAll(ctx base.CompilationContext) error {
	return nil
}
