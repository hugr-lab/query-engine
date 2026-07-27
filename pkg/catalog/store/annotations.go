package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// The annotation overlay is the curation layer applied at the end of ForName,
// AFTER the default descriptions: a row in catalog.annotations overrides the
// generated description of the reconstructed definition. Two layers stack in
// order (logical first, GraphQL second, each overriding the previous), keyed
// EXACTLY the way the CoreDB entity_* views select their overlay so a single
// annotations table drives both the reconstructed schema and MCP vector search.
//
//	LOGICAL (the source-level model the entity_* views expose):
//	  module        entity_key = module name                        (no ForName surface)
//	  data_object   entity_key = compiled object type name          → def.Description
//	  type          entity_key = residual source type name          → def.Description
//	  field         entity_key = "owner.field" (incl. relation nav) → field.Description
//	  function      entity_key = "module.name" (or "name"); parent=module → root field.Description
//	                entity_kind is the function's KIND (see below), so a name
//	                declared in two root namespaces curates independently
//	  data_source   entity_key = data source name                   (no ForName surface)
//
//	GRAPHQL (the generated surface with no logical entity to key on):
//	  gql_type      entity_key = type name          → def.Description
//	  gql_field     entity_key = "type.field"       → field.Description
//	  gql_argument  entity_key = "type.field.arg"   → argument.Description
//
// A row with a NULL description is a vector-only seed (D24) and is skipped by
// the reader — it leaves the generated text intact. These key conventions are
// the single contract shared with the LogicalAnnotator / GraphQLAnnotator
// writers (annotate.go).

const (
	// Logical kinds (mirror the entity_* view annotation joins).
	kindModule     = "module"
	kindDataObject = "data_object"
	kindType       = "type"
	kindField      = "field"
	kindDataSource = "data_source"

	// A function's KIND IS its annotation kind. Query functions, mutation
	// functions and subscriptions are three independent root namespaces, and one
	// name may be declared in several of them (core.models exposes `completion`
	// as both a function and a streaming subscription) — three operations with
	// three descriptions. The kind therefore joins the identity here exactly as
	// it does in the catalog.functions primary key, and it does so with the very
	// value that table stores: the overlay join needs no mapping
	// (a.entity_kind = f.kind) and a client curating a function passes back the
	// kind it read from entity_functions / _catalog.
	kindFunction         = "function"     // query function — also the pre-kind value
	kindMutationFunction = "mutation"     // mutation function
	kindSubscription     = "subscription" // subscription

	// GraphQL kinds (a separate namespace for the generated surface).
	kindGQLType     = "gql_type"
	kindGQLField    = "gql_field"
	kindGQLArgument = "gql_argument"
)

// indexedKind reports whether an annotation kind carries a search vector.
// Only kindType is out: a residual source type is reached deterministically
// from the data object that uses it, so it is never a search target
// (design-035 vector index scope — the same decision seedableField and the
// reindex enumeration implement). Its CURATION is unaffected: the description
// overlay lives in the text columns and entity_types still coalesces it; only
// the embedder call, and the vector nothing would read, are skipped.
func indexedKind(kind string) bool {
	return kind != kindType
}

// functionAnnotationKind normalizes a function kind into its annotation kind
// (the same value — see the kind constants). An empty kind means the
// query-function default, which is also what the rows written before the kind
// joined the identity carry. ok=false marks an unrecognized kind: the write
// path rejects it rather than curating an unreachable key, the read paths fall
// back to the default.
func functionAnnotationKind(kind string) (string, bool) {
	switch kind {
	case kindFunction, kindMutationFunction, kindSubscription:
		return kind, true
	case "":
		return kindFunction, true
	}
	return kindFunction, false
}

// fieldKey / functionKey / argKey build the entity_key for each kind — the
// write side of the convention the overlay reads back.
func fieldKey(owner, field string) string { return owner + "." + field }

func functionKey(module, name string) string {
	if module == "" {
		return name
	}
	return module + "." + name
}

func argumentKey(typeName, fieldName, argName string) string {
	return typeName + "." + fieldName + "." + argName
}

// applyLogicalAnnotations overlays the logical-model curation onto a
// reconstructed definition: the def-level description (data_object / type) and
// the field descriptions (field, incl. relation navigation fields), plus — for
// function-exposing module roots — the function field descriptions keyed by
// module.name. Derived types (filters / mutation inputs / aggregations) first
// INHERIT their base entity's field curation onto same-named fields, so
// curating shop_items.name implicitly shows on shop_items_filter.name and
// _shop_items_aggregation.name; direct rows (and the GraphQL layer) still win.
func (s *Store) applyLogicalAnnotations(ctx context.Context, g *genContext, def *ast.Definition, rows []annotationRow) error {
	// A derived type (filter / mutation input / aggregation) inherits its base's
	// field descriptions onto same-named fields. The base name comes from the
	// session (recorded by resolveDerivedType from the matched rule — never
	// parsed back from directives, whose input markers are directional), and the
	// base is read through ForName — cached, and already carrying defaults AND
	// curation. Only derived rules record a base, so the chain is one level deep
	// and the shared query types (_join_aggregation — a shared rule) keep their
	// more specific default wording.
	if owner := g.derivedBase; owner != "" && owner != def.Name {
		if baseDef := s.ForName(ctx, owner); baseDef != nil {
			for _, f := range def.Fields {
				if bf := baseDef.Fields.ForName(f.Name); bf != nil && bf.Description != "" {
					f.Description = bf.Description
				}
			}
		}
	}
	prefix := def.Name + "."
	for _, a := range rows {
		switch a.kind {
		case kindDataObject, kindType:
			def.Description = a.description
		case kindField:
			if f := def.Fields.ForName(strings.TrimPrefix(a.key, prefix)); f != nil {
				f.Description = a.description
			}
		}
	}
	return s.applyFunctionAnnotations(ctx, g, def)
}

// applyGraphQLAnnotations overlays the generated-surface curation (gql_type /
// gql_field / gql_argument), which runs after the logical layer so a GraphQL
// curation wins. Field and argument rows share the "type.…" prefix and are
// routed by kind; an argument key is "type.field.arg".
func (s *Store) applyGraphQLAnnotations(def *ast.Definition, rows []annotationRow) {
	prefix := def.Name + "."
	for _, a := range rows {
		switch a.kind {
		case kindGQLType:
			def.Description = a.description
		case kindGQLField:
			if f := def.Fields.ForName(strings.TrimPrefix(a.key, prefix)); f != nil {
				f.Description = a.description
			}
		case kindGQLArgument:
			fieldName, argName, ok := strings.Cut(strings.TrimPrefix(a.key, prefix), ".")
			if !ok {
				continue
			}
			if f := def.Fields.ForName(fieldName); f != nil {
				if arg := f.Arguments.ForName(argName); arg != nil {
					arg.Description = a.description
				}
			}
		}
	}
}

// applyFunctionAnnotations overlays the logical function descriptions onto a
// function-exposing module root (Function / MutationFunction / Subscription and
// their _module_* variants). Functions are keyed by module.name under their own
// kind, so the fields are matched by resolving the root's module and kind, then
// reading that kind's annotations for the direct functions' exact keys — a
// curation written for the mutation function never reaches the same-named query
// function.
func (s *Store) applyFunctionAnnotations(ctx context.Context, g *genContext, def *ast.Definition) error {
	module, fnKind, ok := rootDirectFunctionKind(def)
	if !ok {
		return nil
	}
	fns, err := g.readFunctions(ctx, module)
	if err != nil {
		return err
	}
	keyToField := map[string]string{}
	var keys []string
	for _, fn := range fns {
		if fn.Kind != fnKind {
			continue
		}
		k := functionKey(module, fn.Name)
		keyToField[k] = fn.Name
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil
	}
	descByKey, err := g.readFunctionAnnotations(ctx, fnKind, keys)
	if err != nil {
		return err
	}
	for k, desc := range descByKey {
		if fname, ok := keyToField[k]; ok {
			if f := def.Fields.ForName(fname); f != nil {
				f.Description = desc
			}
		}
	}
	return nil
}

// rootDirectFunctionKind reports the catalog.functions.kind a module root
// exposes as DIRECT fields (function / mutation / subscription), with its
// module, or ok=false when the definition is not such a root. Non-top roots
// carry @module_root(name, type); the top-level roots are identified by name
// (their prelude stubs carry no directive). Query/Mutation expose functions
// only through the `function` gateway, never directly, so they return false.
func rootDirectFunctionKind(def *ast.Definition) (module, fnKind string, ok bool) {
	var enum string
	if d := def.Directives.ForName(base.ModuleRootDirectiveName); d != nil {
		module = directiveArg(d, base.ArgName)
		enum = directiveArg(d, base.ArgType)
	} else if k, isTop := topLevelRoots[def.Name]; isTop {
		enum = rootKindEnum(k)
	} else {
		return "", "", false
	}
	switch enum {
	case "FUNCTION":
		return module, "function", true
	case "MUT_FUNCTION":
		return module, "mutation", true
	case "SUBSCRIPTION":
		return module, "subscription", true
	}
	return "", "", false
}

// annotationRow is one curation row carrying text (seed rows are dropped).
type annotationRow struct {
	kind        string
	key         string
	description string
}

// readAnnotationsByName loads every curated (non-seed) row anchored on
// typeName: the type-level rows (data_object / type / gql_type with entity_key
// = typeName) and the member rows (field / gql_field / gql_argument with
// entity_key starting "typeName."). The dot guard keeps "shop." from matching
// "shop_items.name". Function rows are NOT included — they key by module.name
// and are read separately (readFunctionAnnotations).
func (g *genContext) readAnnotationsByName(ctx context.Context, typeName string) ([]annotationRow, error) {
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT entity_kind, entity_key, description
		FROM core.catalog.annotations
		WHERE description IS NOT NULL AND (
		  (entity_kind IN (`+lit(kindDataObject)+`, `+lit(kindType)+`, `+lit(kindGQLType)+`)
		    AND entity_key = `+lit(typeName)+`)
		  OR (entity_kind IN (`+lit(kindField)+`, `+lit(kindGQLField)+`, `+lit(kindGQLArgument)+`)
		    AND starts_with(entity_key, `+lit(typeName+".")+`)))`)
	if err != nil {
		return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
	}
	defer rows.Close()
	var out []annotationRow
	for rows.Next() {
		var kind, key string
		var desc sql.NullString
		if err := rows.Scan(&kind, &key, &desc); err != nil {
			return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
		}
		if !desc.Valid {
			continue
		}
		out = append(out, annotationRow{kind: kind, key: key, description: desc.String})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
	}
	return out, nil
}

// readFunctionAnnotations loads the curated descriptions of one function kind
// for the given exact entity keys, returning key → description.
func (g *genContext) readFunctionAnnotations(ctx context.Context, fnKind string, keys []string) (map[string]string, error) {
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read function annotations: %w", err)
	}
	defer conn.Close()
	quoted := make([]string, len(keys))
	for i, k := range keys {
		quoted[i] = lit(k)
	}
	rows, err := conn.Query(ctx, `SELECT entity_key, description
		FROM core.catalog.annotations
		WHERE entity_kind = `+lit(fnKind)+` AND description IS NOT NULL
		  AND entity_key IN (`+strings.Join(quoted, ", ")+`)`)
	if err != nil {
		return nil, fmt.Errorf("read function annotations: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var key string
		var desc sql.NullString
		if err := rows.Scan(&key, &desc); err != nil {
			return nil, fmt.Errorf("read function annotations: %w", err)
		}
		if desc.Valid {
			out[key] = desc.String
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read function annotations: %w", err)
	}
	return out, nil
}
