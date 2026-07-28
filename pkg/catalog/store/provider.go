package store

import (
	"context"
	"iter"
	"slices"
	"sort"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// The store implements the full read-only base.Provider surface. ForName (the
// on-demand type synthesizer) lives in reconstruct.go; this file adds the rest
// of the interface on top of it: the schema description, the operation roots,
// directive delegation to the static prelude, the interface/union
// relationships, and the whole-schema enumeration used by introspection.
var _ base.Provider = (*Store)(nil)

// Description returns the schema-level description. The entity store keeps no
// root schema description — sources describe themselves through their modules
// and data objects — so this is empty (the db provider is likewise empty when
// no schema description was stored).
func (s *Store) Description(_ context.Context) string { return "" }

// QueryType / MutationType / SubscriptionType synthesize the top-level roots
// through ForName: resolveModuleRoot builds each from its module members, so a
// root with no members resolves to nil.
func (s *Store) QueryType(ctx context.Context) *ast.Definition {
	return s.ForName(ctx, base.QueryBaseName)
}

func (s *Store) MutationType(ctx context.Context) *ast.Definition {
	return s.ForName(ctx, base.MutationBaseName)
}

func (s *Store) SubscriptionType(ctx context.Context) *ast.Definition {
	return s.ForName(ctx, base.SubscriptionBaseName)
}

// DirectiveForName / DirectiveDefinitions delegate to the static prelude: the
// whole directive set (system + hugr) is binary-owned; sources never define
// directives, so the entity storage holds none.
func (s *Store) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return s.static.DirectiveForName(ctx, name)
}

func (s *Store) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return s.static.DirectiveDefinitions(ctx)
}

// Implements yields the interfaces the named type declares.
func (s *Store) Implements(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		def := s.ForName(ctx, name)
		if def == nil {
			return
		}
		for _, iface := range def.Interfaces {
			if d := s.ForName(ctx, iface); d != nil {
				if !yield(d) {
					return
				}
			}
		}
	}
}

// PossibleTypes yields the concrete members of an interface or union. Unions
// carry their members inline; interface implementors are found by scanning the
// enumerable object types. Interfaces and unions are source-defined and rare,
// and this is a cold validator/introspection path, so the scan is acceptable.
func (s *Store) PossibleTypes(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		def := s.ForName(ctx, name)
		if def == nil {
			return
		}
		switch def.Kind {
		case ast.Union:
			for _, member := range def.Types {
				if d := s.ForName(ctx, member); d != nil {
					if !yield(d) {
						return
					}
				}
			}
		case ast.Interface:
			for def := range s.Definitions(ctx) {
				if def.Kind == ast.Object && slices.Contains(def.Interfaces, name) {
					if !yield(def) {
						return
					}
				}
			}
		}
	}
}

// Types enumerates the whole compiled schema as (name, definition) pairs.
func (s *Store) Types(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for def := range s.Definitions(ctx) {
			if !yield(def.Name, def) {
				return
			}
		}
	}
}

// Definitions enumerates every compiled definition the schema serves, by name.
// The store synthesizes derived types on demand, so there is no table to list —
// the set is the reachability closure of the schema from its roots
// (schemaTypeNames), each name resolved through ForName.
func (s *Store) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, name := range s.schemaTypeNames(ctx) {
			if def := s.ForName(ctx, name); def != nil {
				if !yield(def) {
					return
				}
			}
		}
	}
}

// schemaSeeds are the fixed roots of the schema reachability walk: the system
// prelude (scalars, introspection, directives' companion types), the top-level
// operation roots and the shared system types. Everything else — data objects,
// their derived families, the per-module roots, structural types — is reachable
// from these through field and argument type references.
func (s *Store) schemaSeeds(ctx context.Context) []string {
	seeds := []string{
		base.QueryBaseName, base.MutationBaseName, base.SubscriptionBaseName,
		base.FunctionTypeName, base.FunctionMutationTypeName,
	}
	for i := range sharedTypeRules {
		seeds = append(seeds, sharedTypeRules[i].name)
	}
	for name := range s.static.Types(ctx) {
		seeds = append(seeds, name)
	}
	return seeds
}

// schemaTypeNames returns every compiled type name the schema serves, sorted —
// the reachability closure from schemaSeeds. This is the GraphQL-introspection
// set (`__schema.types` is the types reachable from the roots and directives):
// the entity store synthesizes derived types on demand and would serve, e.g., a
// _X_aggregation_sub_aggregation at any depth, but only the depths the schema
// actually references are part of the served schema. Reachability captures the
// derived families (X_filter, _X_aggregation and its used sub-aggregation
// depth, X_mut_*_data, the per-module roots reached through the gateway fields,
// structural aggregations) at exactly the depth the schema uses, with no suffix
// grammar to keep in sync with the generators.
func (s *Store) schemaTypeNames(ctx context.Context) []string {
	return reachableTypeNames(ctx, s, s.schemaSeeds(ctx))
}

// reachableTypeNames is the breadth-first type closure of any provider from a
// seed set: a seed that resolves contributes its referenced types and joins the
// result; a name that does not resolve is dropped. It works against the
// base.Provider surface (ForName only), so the same walk measures the store and
// the fully-compiled reference identically.
func reachableTypeNames(ctx context.Context, p base.Provider, seeds []string) []string {
	seen := map[string]struct{}{}
	var queue []string
	enqueue := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		queue = append(queue, name)
	}
	for _, name := range seeds {
		enqueue(name)
	}
	var served []string
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		def := p.ForName(ctx, name)
		if def == nil {
			continue
		}
		served = append(served, name)
		for _, ref := range referencedTypeNames(def) {
			enqueue(ref)
		}
	}
	sort.Strings(served)
	return served
}

// referencedTypeNames collects the named types a definition points at: its
// interface and union members, and every field output type with its argument
// types (input-object fields carry no arguments, so the same walk covers them).
func referencedTypeNames(def *ast.Definition) []string {
	var out []string
	out = append(out, def.Interfaces...)
	out = append(out, def.Types...)
	for _, f := range def.Fields {
		out = append(out, f.Type.Name())
		for _, a := range f.Arguments {
			out = append(out, a.Type.Name())
		}
	}
	return out
}
