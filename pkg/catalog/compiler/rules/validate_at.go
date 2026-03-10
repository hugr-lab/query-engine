package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ base.BatchRule = (*AtValidator)(nil)

// AtValidator validates @at directives on OBJECT types.
// It checks that:
// - Exactly one of version or timestamp is provided
// - The type belongs to a catalog source whose capabilities support time travel
//
// Runs in FINALIZE phase so all types and capabilities are resolved.
type AtValidator struct{}

func (r *AtValidator) Name() string     { return "AtValidator" }
func (r *AtValidator) Phase() base.Phase { return base.PhaseFinalize }

func (r *AtValidator) ProcessAll(ctx base.CompilationContext) error {
	for name := range ctx.Objects() {
		def := ctx.LookupType(name)
		if def == nil {
			continue
		}
		atDir := def.Directives.ForName(base.AtDirectiveName)
		if atDir == nil {
			continue
		}

		// Validate exactly one of version or timestamp is provided
		hasVersion := atDir.Arguments.ForName("version") != nil &&
			atDir.Arguments.ForName("version").Value != nil &&
			atDir.Arguments.ForName("version").Value.Raw != ""
		hasTimestamp := atDir.Arguments.ForName("timestamp") != nil &&
			atDir.Arguments.ForName("timestamp").Value != nil &&
			atDir.Arguments.ForName("timestamp").Value.Raw != ""

		if !hasVersion && !hasTimestamp {
			return gqlerror.ErrorPosf(atDir.Position,
				"@at on %s: exactly one of version or timestamp must be provided",
				name)
		}
		if hasVersion && hasTimestamp {
			return gqlerror.ErrorPosf(atDir.Position,
				"@at on %s: only one of version or timestamp can be provided, not both",
				name)
		}

		// Validate the source supports time travel
		opts := ctx.CompileOptions()
		if !opts.IsTimeTravelSupported() {
			return gqlerror.ErrorPosf(atDir.Position,
				"@at on %s: data source %q does not support time travel",
				name, opts.Name)
		}
	}
	return nil
}
