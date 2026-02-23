package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*VectorSearchRule)(nil)

// VectorSearchRule adds the similarity query argument to objects that have
// any Vector-typed field. Objects with @embeddings additionally get semantic
// search args via EmbeddingsRule.
type VectorSearchRule struct{}

func (r *VectorSearchRule) Name() string     { return "VectorSearchRule" }
func (r *VectorSearchRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *VectorSearchRule) Match(def *ast.Definition) bool {
	if def.Directives.ForName("table") == nil && def.Directives.ForName("view") == nil {
		return false
	}
	for _, f := range def.Fields {
		if f.Type.Name() == "Vector" {
			return true
		}
	}
	return false
}

func (r *VectorSearchRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	pos := compiledPos(def.Name)
	similarityArg := &ast.ArgumentDefinition{
		Name:        "similarity",
		Description: "Search for vector similarity",
		Type:        ast.NamedType("VectorSearchInput", pos),
		Position:    pos,
	}

	// Add similarity to all query fields except SELECT_ONE (by_pk)
	queryFields := ctx.QueryFields()[def.Name]
	for _, qf := range queryFields {
		if isSelectOneQuery(qf) {
			continue
		}
		qf.Arguments = append(qf.Arguments, similarityArg)
	}

	return nil
}
