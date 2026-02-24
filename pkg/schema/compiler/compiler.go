package compiler

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// Catalog is a source of definitions with compile options.
// Used by pkg/schema to pass catalog sources to compilation.
type Catalog interface {
	base.DefinitionsSource
	CompileOptions() Options
}

// Compile is a convenience function that creates a Compiler from GlobalRules()
// and compiles the given source against the target schema.
func Compile(ctx context.Context, provider base.Provider, source base.DefinitionsSource, opts Options) (base.CompiledCatalog, error) {
	c := New(GlobalRules()...)
	return c.Compile(ctx, provider, source, opts)
}

// Compiler orchestrates 5-phase rule-based schema compilation.
type Compiler struct {
	rules map[base.Phase][]base.Rule
}

// New creates a Compiler with the given rules sorted by phase.
func New(rules ...base.Rule) *Compiler {
	c := &Compiler{
		rules: make(map[base.Phase][]base.Rule),
	}
	for _, r := range rules {
		c.rules[r.Phase()] = append(c.rules[r.Phase()], r)
	}
	return c
}

// Compile executes 5-phase compilation of source definitions against the target schema.
// Returns a CompiledCatalog (DDL feed) that can be applied via Provider.Update().
func (c *Compiler) Compile(
	ctx context.Context,
	schema base.Provider,
	source base.DefinitionsSource,
	opts base.Options,
) (base.CompiledCatalog, error) {
	// Count source definitions for pre-sizing
	defCount := 0
	for range source.Definitions(ctx) {
		defCount++
	}

	output := newIndexedOutput(defCount * 6)
	cctx := newCompilationContext(ctx, source, schema, opts, output)

	// Execute phases in order
	phases := []base.Phase{
		base.PhaseValidate,
		base.PhasePrepare,
		base.PhaseGenerate,
		base.PhaseAssemble,
		base.PhaseFinalize,
	}

	for _, phase := range phases {
		rules := c.rules[phase]
		if len(rules) == 0 {
			continue // phase skipped silently
		}

		// Separate DefinitionRules and BatchRules
		var defRules []base.DefinitionRule
		var batchRules []base.BatchRule
		for _, r := range rules {
			switch rule := r.(type) {
			case base.DefinitionRule:
				defRules = append(defRules, rule)
			case base.BatchRule:
				batchRules = append(batchRules, rule)
			}
		}

		// Single iteration pass over source definitions dispatching all matching DefinitionRules (FR-009)
		if len(defRules) > 0 {
			for def := range source.Definitions(ctx) {
				for _, rule := range defRules {
					if rule.Match(def) {
						if err := rule.Process(cctx, def); err != nil {
							return nil, wrapRuleError(phase, rule.Name(), def, err)
						}
					}
				}
			}
		}

		// BatchRules execute after all DefinitionRules in registration order (FR-010)
		for _, rule := range batchRules {
			if err := rule.ProcessAll(cctx); err != nil {
				return nil, wrapRuleError(phase, rule.Name(), nil, err)
			}
		}
	}

	return newCompiledCatalog(output), nil
}

// wrapRuleError wraps an error with phase, rule name, and definition context.
func wrapRuleError(phase base.Phase, ruleName string, def *ast.Definition, err error) error {
	// If the error is already a gqlerror with position info, wrap with context
	if gqlErr, ok := err.(*gqlerror.Error); ok {
		if def != nil {
			gqlErr.Message = fmt.Sprintf("[%s/%s] %s: %s", phase, ruleName, def.Name, gqlErr.Message)
		} else {
			gqlErr.Message = fmt.Sprintf("[%s/%s] %s", phase, ruleName, gqlErr.Message)
		}
		return gqlErr
	}
	// Create a new gqlerror with position from the definition
	var pos *ast.Position
	if def != nil {
		pos = def.Position
	}
	msg := fmt.Sprintf("[%s/%s] %v", phase, ruleName, err)
	if def != nil {
		msg = fmt.Sprintf("[%s/%s] %s: %v", phase, ruleName, def.Name, err)
	}
	if pos != nil {
		return gqlerror.ErrorPosf(pos, "%s", msg)
	}
	return fmt.Errorf("%s", msg)
}
