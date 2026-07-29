package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// The read side reconstructs a COMPILED GraphQL type by NAME on demand (never a
// bulk inverse of collect): ForName classifies the name and dispatches to the
// RULE that builds that one type from catalog.*.
//
// The dispatch is a list of resolvers tried in order — extensible like the
// compiler's rule pipeline: a new kind of name is handled by appending a
// resolver. Order matters: module roots come before the system layer because
// Query/Mutation/Subscription are @system-classified (the static prelude holds
// stubs) yet must be SYNTHESIZED from their module members.
type resolver func(ctx context.Context, g *genContext, name string) (*ast.Definition, error)

// resolverEntry pairs a resolver with its caching class: system definitions
// come from the in-memory static layer and are never cached; global
// definitions depend on the whole active-source set and index under
// allSources; everything else indexes under the sources its session
// actually read rows from.
type resolverEntry struct {
	resolve resolver
	global  bool
	system  bool
}

var resolvers = []resolverEntry{
	{resolve: resolveModuleRoot, global: true}, // 4: Query / Mutation / _module_*_query — synthesized from members
	{resolve: resolveSharedType, global: true}, // 6: _join / _spatial / … — synthesized from active objects
	{resolve: resolveSystemType, system: true}, // 1: scalars / __* / @system — static prelude
	{resolve: resolveDataObject},               // 3: catalog.data_objects — generated object type
	{resolve: resolveSourceType},               // 2: catalog.types — parsed from stored SDL
	{resolve: resolveDerivedType},              // 5: _X_aggregation / X_filter / … — generated from base X
}

// ForName resolves a type definition by name (nil when absent): cache hit →
// singleflight-collapsed resolution through the chain, on a fresh read
// session. The catalog.Provider surface has no error channel, so a read
// failure is logged here and served as nil — never cached (absence is a nil
// WITHOUT an error and is simply not cached either).
//
// A freshly built (non-system) definition is finalised before caching:
// description defaults for synthetic query members, and the annotation
// overlay. System definitions come straight from the shared static prelude
// and are neither finalised nor cached.
func (s *Store) ForName(ctx context.Context, name string) *ast.Definition {
	if def := s.cache.get(name); def != nil {
		return def
	}
	v, _, _ := s.sf.Do("DEF:"+name, func() (any, error) {
		if def := s.cache.get(name); def != nil {
			return def, nil
		}
		gen := s.gen.Load()
		g := s.genCtx()
		def, sources, system, err := s.resolveByName(ctx, g, name)
		if err != nil {
			logReadErr("resolve "+name, err)
			return nil, nil
		}
		if def == nil || system {
			return def, nil
		}
		if err := s.finalizeDescriptions(ctx, g, def); err != nil {
			logReadErr("finalize "+name, err)
			return nil, nil
		}
		s.cache.put(name, sources, def, func() bool { return s.gen.Load() == gen })
		return def, nil
	})
	if v == nil {
		return nil
	}
	return v.(*ast.Definition)
}

// resolveByName runs the resolver chain and returns the built definition, the
// cache source list to index it under, and whether it is a system definition
// (served from the shared static prelude — never cached, never finalised). A
// nil definition means the name is not served.
func (s *Store) resolveByName(ctx context.Context, g *genContext, name string) (*ast.Definition, []string, bool, error) {
	for i := range resolvers {
		e := &resolvers[i]
		def, err := e.resolve(ctx, g, name)
		if err != nil {
			return nil, nil, false, err
		}
		if def == nil {
			continue
		}
		if e.system {
			return def, nil, true, nil
		}
		sources := []string{allSources}
		if !e.global {
			sources = g.sourceList()
		}
		return def, sources, false, nil
	}
	return nil, nil, false, nil
}

// forNameRaw resolves a definition through the generator chain WITHOUT the
// description finalisation or the read cache — the pre-enrichment generation
// the golden parity harness compares against the fully-compiled reference.
func (s *Store) forNameRaw(ctx context.Context, name string) *ast.Definition {
	def, _, _, err := s.resolveByName(ctx, s.genCtx(), name)
	if err != nil {
		logReadErr("resolve "+name, err)
		return nil
	}
	return def
}

// resolveSystemType (1) serves the binary-owned system layer.
func resolveSystemType(ctx context.Context, g *genContext, name string) (*ast.Definition, error) {
	return g.s.static.ForName(ctx, name), nil
}

// resolveSourceType (2) reconstructs a residual source type from stored SDL.
func resolveSourceType(ctx context.Context, g *genContext, name string) (*ast.Definition, error) {
	return g.reconstructType(ctx, name)
}

// resolveDataObject (3) generates the compiled object type: the reconstructed
// base object plus the fieldRules contributions (gen.go).
func resolveDataObject(ctx context.Context, g *genContext, name string) (*ast.Definition, error) {
	return reconstructDataObject(ctx, g, name)
}

// reconstructType parses a residual source type from its stored SDL
// (catalog.types.definition) and re-attaches @catalog(name, engine) — the
// compiler tags every definition, but the SDL is stored pre-tagging; nil when
// the name is not a stored type. Structural Object types also get the scalar
// field arguments the passthrough applies before storing its compiled copy.
func (g *genContext) reconstructType(ctx context.Context, name string) (*ast.Definition, error) {
	def, dataSource, err := g.readType(ctx, name)
	if err != nil || def == nil {
		return nil, err
	}
	applyScalarFieldArguments(def)
	engines, err := g.activeEngines(ctx)
	if err != nil {
		return nil, err
	}
	return attachCatalog(def, dataSource, engines), nil
}

// applyScalarFieldArguments mirrors setScalarFieldArguments on read: non-list
// fields of FieldArgumentsProvider scalars carry the scalar's argument list.
func applyScalarFieldArguments(def *ast.Definition) {
	if def.Kind != ast.Object {
		return
	}
	for _, f := range def.Fields {
		if f.Name == "_stub" || f.Type.NamedType == "" {
			continue
		}
		s := types.Lookup(f.Type.Name())
		if s == nil {
			continue
		}
		if args := scalarFieldArguments(s); args != nil {
			f.Arguments = args
		}
	}
}

// attachCatalog re-attaches @catalog(name, engine) to a stored-SDL definition
// when the stored text does not already carry it.
func attachCatalog(def *ast.Definition, dataSource string, engines map[string]string) *ast.Definition {
	if def == nil || def.Directives.ForName(base.CatalogDirectiveName) != nil {
		return def
	}
	def.Directives = append(def.Directives, catalogDirective(dataSource, engines[dataSource]))
	return def
}

// readType reads a catalog.types row, memoizing the STORED text per session —
// a fresh AST is parsed per call because callers mutate the returned
// definition (attachCatalog, applyScalarFieldArguments). Absent = (nil, "",
// nil).
func (g *genContext) readType(ctx context.Context, name string) (*ast.Definition, string, error) {
	if row, ok := g.typeRows[name]; ok {
		if row == nil {
			return nil, "", nil
		}
		return parseStoredDefinition(name, row.definition), row.dataSource, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("read type %s: %w", name, err)
	}
	defer conn.Close()
	var definition, dataSource string
	err = conn.QueryRow(ctx,
		`SELECT t.definition, t.data_source FROM core.catalog.types t`+activeMeta("m", "t.data_source")+
			` WHERE t.name = `+lit(name)).Scan(&definition, &dataSource)
	if err == sql.ErrNoRows {
		if g.typeRows == nil {
			g.typeRows = map[string]*typeRow{}
		}
		g.typeRows[name] = nil
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("read type %s: %w", name, err)
	}
	if g.typeRows == nil {
		g.typeRows = map[string]*typeRow{}
	}
	g.typeRows[name] = &typeRow{definition: definition, dataSource: dataSource}
	g.touch(dataSource)
	return parseStoredDefinition(name, definition), dataSource, nil
}

// parseStoredDefinition parses one stored SDL definition (catalog.types rows).
// A parse failure means a corrupt stored row (writes are validated) — logged,
// classified as absent.
func parseStoredDefinition(name, definition string) *ast.Definition {
	doc, err := parser.ParseSchema(&ast.Source{Name: "catalog:" + name, Input: definition})
	if err != nil || len(doc.Definitions) == 0 {
		if err != nil {
			slog.Warn("catalog store: stored definition does not parse",
				"type", name, "error", err)
		}
		return nil
	}
	return doc.Definitions[0]
}
