package store

import (
	"context"
	"database/sql"
	"fmt"

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

// resolveModuleRoot (4) synthesizes Query/Mutation/Subscription and per-module
// roots from their members. TODO(M3).
func resolveModuleRoot(ctx context.Context, s *Store, name string) *ast.Definition {
	return nil
}

// resolveDataObject (3) generates the compiled object type. M3 base slice: the
// reconstructed base object (nav / aggregation / extra fields via fieldRules
// follow).
func resolveDataObject(ctx context.Context, s *Store, name string) *ast.Definition {
	return reconstructDataObject(ctx, s, name)
}

// resolveDerivedType (5) generates a derived type (_X_aggregation, X_filter, …)
// from its base X. TODO(M3).
func resolveDerivedType(ctx context.Context, s *Store, name string) *ast.Definition {
	return nil
}

// reconstructType parses a residual source type from its stored SDL
// (catalog.types.definition); nil when the name is not a stored type.
func (s *Store) reconstructType(ctx context.Context, name string) *ast.Definition {
	def, err := s.readType(ctx, name)
	if err != nil {
		return nil
	}
	return def
}

func (s *Store) readType(ctx context.Context, name string) (*ast.Definition, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var definition string
	err = conn.QueryRow(ctx,
		`SELECT definition FROM core.catalog.types WHERE name = `+lit(name)).Scan(&definition)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("catalog read type %s: %w", name, err)
	}
	doc, gerr := parser.ParseSchema(&ast.Source{Name: "catalog:" + name, Input: definition})
	if gerr != nil {
		return nil, fmt.Errorf("catalog parse type %s: %w", name, gerr)
	}
	if len(doc.Definitions) == 0 {
		return nil, nil
	}
	return doc.Definitions[0], nil
}
