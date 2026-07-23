package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
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
//
// (Future — huge schemas: a resolver's cheap classification will consult an
// in-memory name index / bloom filter refreshed on write, so a miss costs no DB
// round-trip. The resolver structure keeps that a drop-in.)
type resolver func(ctx context.Context, s *Store, name string) *ast.Definition

var resolvers = []resolver{
	resolveModuleRoot,  // 4: Query / Mutation / _module_*_query — synthesized from members
	resolveSharedType,  // 6: _join / _spatial / … — synthesized from active objects
	resolveSystemType,  // 1: scalars / __* / @system — static prelude
	resolveDataObject,  // 3: catalog.data_objects — generated object type
	resolveSourceType,  // 2: catalog.types — parsed from stored SDL
	resolveDerivedType, // 5: _X_aggregation / X_filter / … — generated from base X
}

// ForName resolves a type definition by name (nil when absent).
func (s *Store) ForName(ctx context.Context, name string) *ast.Definition {
	for _, resolve := range resolvers {
		if def := resolve(ctx, s, name); def != nil {
			return def
		}
	}
	return nil
}

// resolveSystemType (1) serves the binary-owned system layer.
func resolveSystemType(ctx context.Context, s *Store, name string) *ast.Definition {
	return s.static.ForName(ctx, name)
}

// resolveSourceType (2) reconstructs a residual source type from stored SDL.
func resolveSourceType(ctx context.Context, s *Store, name string) *ast.Definition {
	return s.reconstructType(ctx, name)
}

// resolveDataObject (3) generates the compiled object type: the reconstructed
// base object plus the fieldRules contributions (gen.go).
func resolveDataObject(ctx context.Context, s *Store, name string) *ast.Definition {
	return reconstructDataObject(ctx, s, name)
}

// reconstructType parses a residual source type from its stored SDL
// (catalog.types.definition) and re-attaches @catalog(name, engine) — the
// compiler tags every definition, but the SDL is stored pre-tagging; nil when
// the name is not a stored type.
func (s *Store) reconstructType(ctx context.Context, name string) *ast.Definition {
	def, dataSource, err := s.readType(ctx, name)
	if err != nil || def == nil {
		return nil
	}
	return attachCatalog(def, dataSource, s.activeEngines(ctx))
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

func (s *Store) readType(ctx context.Context, name string) (*ast.Definition, string, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, "", err
	}
	defer conn.Close()
	var definition, dataSource string
	err = conn.QueryRow(ctx,
		`SELECT t.definition, t.data_source FROM core.catalog.types t`+activeMeta("m", "t.data_source")+
			` WHERE t.name = `+lit(name)).Scan(&definition, &dataSource)
	if err == sql.ErrNoRows {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("catalog read type %s: %w", name, err)
	}
	return parseStoredDefinition(name, definition), dataSource, nil
}

// parseStoredDefinition parses one stored SDL definition (catalog.types rows).
func parseStoredDefinition(name, definition string) *ast.Definition {
	doc, err := parser.ParseSchema(&ast.Source{Name: "catalog:" + name, Input: definition})
	if err != nil || len(doc.Definitions) == 0 {
		return nil
	}
	return doc.Definitions[0]
}
