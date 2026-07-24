package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	qetypes "github.com/hugr-lab/query-engine/types"
)

// The store implements both curation surfaces. LogicalAnnotator writes the
// source-level entity kinds the entity_* views read; GraphQLAnnotator writes
// the generated-surface kinds. Every write upserts one catalog.annotations row
// by (entity_kind, entity_key) — text plus, when an embedder is configured, the
// text's embedding vector so MCP vector search is current — and then evicts the
// affected reconstructed types from the definition cache. An empty description
// stores NULL (clearing the curation, the generated text shows through again)
// and leaves the vec column untouched so any load-time seed vector survives.
var (
	_ base.LogicalAnnotator = (*Store)(nil)
	_ base.GraphQLAnnotator = (*Store)(nil)
)

// errReadonly rejects curation on a read-only store (cluster workers read the
// already-populated catalog schema).
var errReadonly = errors.New("catalog store: read-only, cannot annotate")

// --- LogicalAnnotator ---

// SetModuleDescription curates a module (no ForName surface — module
// descriptions reach clients through the logical model / entity_modules view).
func (s *Store) SetModuleDescription(ctx context.Context, module, desc, longDesc string) error {
	return s.curate(ctx, kindModule, module, "", desc, longDesc, nil)
}

// SetDataObjectDescription curates a data object's description; evicts that type.
func (s *Store) SetDataObjectDescription(ctx context.Context, name, desc, longDesc string) error {
	return s.curate(ctx, kindDataObject, name, "", desc, longDesc, func() { s.evictType(name) })
}

// SetSourceTypeDescription curates a residual source type's description; evicts it.
func (s *Store) SetSourceTypeDescription(ctx context.Context, name, desc, longDesc string) error {
	return s.curate(ctx, kindType, name, "", desc, longDesc, func() { s.evictType(name) })
}

// SetObjectFieldDescription curates a data-object field (incl. a relation
// navigation field); evicts the owning type AND its cached derived types
// (filters / mutation inputs / aggregations), whose same-named fields inherit
// the description at reconstruction.
func (s *Store) SetObjectFieldDescription(ctx context.Context, owner, field, desc, longDesc string) error {
	return s.curate(ctx, kindField, fieldKey(owner, field), owner, desc, longDesc, func() { s.evictTypeFamily(owner) })
}

// SetFunctionDescription curates a function (keyed module.name, parent=module);
// evicts every module root that could expose it as a direct field.
func (s *Store) SetFunctionDescription(ctx context.Context, module, name, desc, longDesc string) error {
	return s.curate(ctx, kindFunction, functionKey(module, name), module, desc, longDesc, func() {
		for _, root := range functionRootTypes(module) {
			s.evictType(root)
		}
	})
}

// SetDataSourceDescription curates a data source (no ForName surface — data
// source descriptions reach clients through the entity_data_sources view).
func (s *Store) SetDataSourceDescription(ctx context.Context, name, desc, longDesc string) error {
	return s.curate(ctx, kindDataSource, name, "", desc, longDesc, nil)
}

// --- GraphQLAnnotator ---

// SetDefinitionDescription curates a generated type's description; evicts it.
func (s *Store) SetDefinitionDescription(ctx context.Context, typeName, desc, longDesc string) error {
	return s.curate(ctx, kindGQLType, typeName, "", desc, longDesc, func() { s.evictType(typeName) })
}

// SetFieldDescription curates a generated field's description; evicts its type.
func (s *Store) SetFieldDescription(ctx context.Context, typeName, fieldName, desc, longDesc string) error {
	return s.curate(ctx, kindGQLField, fieldKey(typeName, fieldName), typeName, desc, longDesc,
		func() { s.evictType(typeName) })
}

// SetArgumentDescription curates a generated field argument's description;
// evicts the owning type.
func (s *Store) SetArgumentDescription(ctx context.Context, typeName, fieldName, argName, desc, longDesc string) error {
	return s.curate(ctx, kindGQLArgument, argumentKey(typeName, fieldName, argName),
		fieldKey(typeName, fieldName), desc, longDesc, func() { s.evictType(typeName) })
}

// functionRootTypes are the module roots that may carry a function as a direct
// field — the eviction targets for a function-description write (the function's
// kind is not known at the call site, so all three candidates are evicted).
func functionRootTypes(module string) []string {
	return []string{
		sdl.ModuleTypeName(module, base.ModuleFunction),
		sdl.ModuleTypeName(module, base.ModuleMutationFunction),
		sdl.ModuleTypeName(module, base.ModuleSubscription),
	}
}

// curate upserts one annotation row and runs the caller's cache eviction (nil
// when the curation has no ForName surface). The embedding vector is computed
// from the curated text (nil when embeddings are off or the text is empty — a
// clear); the upsert then preserves any existing seed vector.
func (s *Store) curate(ctx context.Context, kind, key, parent, desc, longDesc string, evict func()) error {
	if s.isReadonly {
		return errReadonly
	}
	vec, err := s.embed(ctx, embeddingText(longDesc, desc, ""))
	if err != nil {
		return err
	}
	if err := s.upsertAnnotation(ctx, kind, key, parent, desc, longDesc, vec); err != nil {
		return err
	}
	if evict != nil {
		evict()
	}
	return nil
}

// upsertAnnotation writes one curation row by (entity_kind, entity_key). An
// empty description is stored as NULL. When vec is non-nil it is written and
// updated on conflict; when nil the vec column is left untouched so a load-time
// seed vector survives a text-only curation (or a clear). The vector is
// rendered as a cross-engine text literal (vecLiteral) and CURRENT_TIMESTAMP is
// used for updated_at — the statement shapes the CoreDB inventory pins as
// working on both DuckDB and attached PostgreSQL (core-db/hugr_catalog_test.go).
func (s *Store) upsertAnnotation(ctx context.Context, kind, key, parent, desc, longDesc string, vec qetypes.Vector) error {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set annotation %s %s: %w", kind, key, err)
	}
	defer conn.Close()
	head := `INSERT INTO core.catalog.annotations
		(entity_kind, entity_key, parent, description, long_description`
	vals := `VALUES (` + lit(kind) + `, ` + lit(key) + `, ` + lit(nilIfEmpty(parent)) + `, ` +
		lit(nilIfEmpty(desc)) + `, ` + lit(nilIfEmpty(longDesc))
	set := `ON CONFLICT (entity_kind, entity_key) DO UPDATE SET
		parent = EXCLUDED.parent, description = EXCLUDED.description,
		long_description = EXCLUDED.long_description`
	var stmt string
	if vec != nil {
		stmt = head + `, vec, updated_at) ` + vals + `, ` + vecLiteral(vec) + `, CURRENT_TIMESTAMP) ` +
			set + `, vec = EXCLUDED.vec, updated_at = EXCLUDED.updated_at`
	} else {
		stmt = head + `, updated_at) ` + vals + `, CURRENT_TIMESTAMP) ` +
			set + `, updated_at = EXCLUDED.updated_at`
	}
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("set annotation %s %s: %w", kind, key, err)
	}
	return nil
}
