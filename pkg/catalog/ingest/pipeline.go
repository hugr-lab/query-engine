package ingest

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// Catalog is a source of definitions with compile options.
type Catalog interface {
	base.DefinitionsSource
	CompileOptions() base.Options
}

// Pipeline runs a rule set over one source's SDL, in phase order.
//
// There used to be a package-level Compile() over a mutable global rule
// registry, filled from init(). With a single rule set left there is nothing to
// register and nothing to choose: New(Default()...) is the pipeline, and a test
// that wants a subset passes one explicitly.
type Pipeline struct {
	rules map[base.Phase][]base.Rule
}

// New creates a Pipeline with the given rules sorted by phase.
func New(rules ...base.Rule) *Pipeline {
	c := &Pipeline{
		rules: make(map[base.Phase][]base.Rule),
	}
	for _, r := range rules {
		c.rules[r.Phase()] = append(c.rules[r.Phase()], r)
	}
	return c
}

// Compile runs the phases over the source's definitions against the target
// schema. VALIDATE and PREPARE mutate those definitions IN PLACE — prefix,
// catalog tag, merged extensions — and the returned CompiledCatalog carries
// what could not go in place: the extensions and the dependency set. GENERATE
// and ASSEMBLE have no rules and are skipped; the served GraphQL surface is
// generated on read by the catalog storage.
func (c *Pipeline) Compile(
	ctx context.Context,
	schema base.Provider,
	source base.DefinitionsSource,
	opts base.Options,
) (base.CompiledCatalog, error) {
	if strings.HasPrefix(opts.Prefix, "_") {
		return nil, fmt.Errorf("catalog %q: prefix %q must not start with underscore — generated type names would begin with '__' which is reserved by GraphQL introspection", opts.Name, opts.Prefix)
	}

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
			// Also dispatch promoted definitions (added by PREPARE rules,
			// e.g. extensions targeting provider types like Function).
			for _, def := range cctx.promoted {
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
