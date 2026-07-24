package store

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Description finalisation runs at the END of ForName, over a freshly built
// (non-system) definition, and is the single place the store curates human
// descriptions:
//
//   1. default descriptions for synthetic query members that the source schema
//      leaves blank — the module-root data-object fields and the cross-source
//      shared-type members (_join / _spatial / _h3_data_query and their
//      aggregation twins);
//   2. the logical-model annotation overlay (data_object / type / field /
//      function curation), then
//   3. the GraphQL annotation overlay (gql_type / gql_field / gql_argument),
//      each layer overriding the previous where a curation row exists.
//
// Only EMPTY descriptions are filled by the defaults — a source- or
// generator-provided description always wins; the overlays override
// unconditionally. These defaults are store-only enrichment beyond the
// (retiring) compiler; the golden parity harness compares the pre-finalise
// generation (forNameRaw), so they do not drift the oracle.

// finalizeDescriptions decorates a freshly built definition in place: the
// default descriptions first, then the logical overlay, then the GraphQL
// overlay. The two overlays share one name-anchored annotation read.
func (s *Store) finalizeDescriptions(ctx context.Context, g *genContext, def *ast.Definition) error {
	applyDefaultDescriptions(def)
	rows, err := g.readAnnotationsByName(ctx, def.Name)
	if err != nil {
		return err
	}
	if err := s.applyLogicalAnnotations(ctx, g, def, rows); err != nil {
		return err
	}
	s.applyGraphQLAnnotations(def, rows)
	return nil
}

// sharedKind labels a cross-source shared query type for its member wording.
type sharedKind int

const (
	sharedJoin sharedKind = iota
	sharedSpatial
	sharedH3
)

// sharedQueryKinds maps each shared query type to its member wording.
var sharedQueryKinds = map[string]sharedKind{
	"_join":                sharedJoin,
	"_join_aggregation":    sharedJoin,
	"_spatial":             sharedSpatial,
	"_spatial_aggregation": sharedSpatial,
	"_h3_data_query":       sharedH3,
}

// applyDefaultDescriptions fills empty descriptions on the synthetic query
// members of a definition: the shared-type members by their shared kind, the
// module-root data-object fields by their query/mutation shape.
func applyDefaultDescriptions(def *ast.Definition) {
	if kind, ok := sharedQueryKinds[def.Name]; ok {
		for _, f := range def.Fields {
			if f.Description == "" {
				f.Description = sharedMemberDescription(kind, f)
			}
			enrichMemberArgs(f)
		}
		return
	}
	if !isRootTypeName(def.Name) {
		return
	}
	for _, f := range def.Fields {
		if f.Description == "" {
			f.Description = rootShapeDescription(f)
		}
		enrichMemberArgs(f)
	}
}

// isRootTypeName reports the module-root types (top-level and per-module).
func isRootTypeName(name string) bool {
	switch name {
	case base.QueryBaseName, base.MutationBaseName, base.SubscriptionBaseName,
		base.FunctionTypeName, base.FunctionMutationTypeName:
		return true
	}
	return strings.HasPrefix(name, "_module_")
}

// rootShapeDescription is the default for a module-root data-object field,
// derived from its @query / @mutation shape marker. Aggregation shapes already
// carry a generated description, so they are not covered here (they are never
// empty); a field without a shape marker (a child-module gateway, a function)
// returns "" and keeps whatever it had.
func rootShapeDescription(f *ast.FieldDefinition) string {
	if d := f.Directives.ForName(base.QueryDirectiveName); d != nil {
		obj := directiveArg(d, base.ArgName)
		switch directiveArg(d, base.ArgType) {
		case base.QueryTypeTextSelect:
			return "Query " + obj + " objects"
		case base.QueryTypeTextSelectOne:
			return "Query a single " + obj + " object"
		}
	}
	if d := f.Directives.ForName(base.MutationDirectiveName); d != nil {
		obj := directiveArg(d, base.ArgName)
		switch directiveArg(d, base.ArgType) {
		case "INSERT":
			return "Insert " + obj + " objects"
		case "UPDATE":
			return "Update " + obj + " objects"
		case "DELETE":
			return "Delete " + obj + " objects"
		}
	}
	return ""
}

// sharedMemberSubtype classifies a shared-type member as select / aggregation /
// bucket aggregation from its aggregation-query marker.
type sharedMemberSubtype int

const (
	memberSelect sharedMemberSubtype = iota
	memberAgg
	memberBucket
)

func sharedMemberOf(f *ast.FieldDefinition) (sharedMemberSubtype, string) {
	if d := f.Directives.ForName(base.FieldAggregationQueryDirectiveName); d != nil {
		if directiveArg(d, base.ArgIsBucket) == "true" {
			return memberBucket, directiveArg(d, base.ArgName)
		}
		return memberAgg, directiveArg(d, base.ArgName)
	}
	if d := f.Directives.ForName(base.QueryDirectiveName); d != nil {
		return memberSelect, directiveArg(d, base.ArgName)
	}
	return memberSelect, f.Name
}

// sharedMemberDescription is the default for a per-object member of a shared
// query type, worded by the shared kind and the member subtype.
func sharedMemberDescription(kind sharedKind, f *ast.FieldDefinition) string {
	sub, obj := sharedMemberOf(f)
	switch kind {
	case sharedJoin:
		switch sub {
		case memberBucket:
			return "Bucket aggregation of " + obj + " records joined to the query"
		case memberAgg:
			return "Aggregation of " + obj + " records joined to the query"
		default:
			return "The " + obj + " records joined to the query"
		}
	case sharedSpatial:
		switch sub {
		case memberBucket:
			return "Spatial bucket aggregation of " + obj + " records"
		case memberAgg:
			return "Spatial aggregation of " + obj + " records"
		default:
			return "The " + obj + " records for spatial queries"
		}
	case sharedH3:
		switch sub {
		case memberBucket:
			return "H3 bucket aggregation of " + obj + " records"
		case memberAgg:
			return "H3 aggregation of " + obj + " records"
		default:
			return "The " + obj + " records for H3 spatial queries"
		}
	}
	return ""
}

// enrichMemberArgs fills the empty descriptions of the structural arguments a
// synthetic query member carries — the mutation filter/data inputs and the
// unique-select view args the shape builders construct inline and leave bare.
// The standard query arguments (order_by/limit/offset/distinct_on/inner/
// nested_*/similarity/semantic) are built by the shared rules.* arg profiles
// with base.Desc* descriptions already, so they are non-empty and untouched
// here — this only backfills the canonical description onto the inline-built
// ones, keyed by the same constants for a single source of truth.
func enrichMemberArgs(f *ast.FieldDefinition) {
	for _, a := range f.Arguments {
		if a.Description != "" {
			continue
		}
		switch a.Name {
		case "filter":
			a.Description = base.DescFilter
		case "data":
			a.Description = base.DescMutationData
		case "args":
			a.Description = base.DescArgs
		}
	}
}

// directiveArg returns a directive argument's raw value, or "" when absent.
func directiveArg(d *ast.Directive, name string) string {
	if a := d.Arguments.ForName(name); a != nil && a.Value != nil {
		return a.Value.Raw
	}
	return ""
}
