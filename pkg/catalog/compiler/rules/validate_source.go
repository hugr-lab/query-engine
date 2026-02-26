package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ base.DefinitionRule = (*SourceValidator)(nil)

// SourceValidator rejects redefinition of system types during the VALIDATE phase.
type SourceValidator struct{}

func (r *SourceValidator) Name() string      { return "SourceValidator" }
func (r *SourceValidator) Phase() base.Phase { return base.PhaseValidate }

func (r *SourceValidator) Match(_ *ast.Definition) bool { return true }

func (r *SourceValidator) Process(_ base.CompilationContext, def *ast.Definition) error {
	if isSystemType(def.Name) {
		return gqlerror.ErrorPosf(def.Position, "cannot redefine system type %q", def.Name)
	}
	return nil
}

var systemTypes = map[string]bool{
	"Query":        true,
	"Mutation":     true,
	"Subscription": true,
}

func isSystemType(name string) bool {
	if len(name) > 2 && name[0] == '_' && name[1] == '_' {
		return true
	}
	return systemTypes[name]
}
