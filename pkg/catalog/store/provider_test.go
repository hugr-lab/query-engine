//go:build duckdb_arrow

package store

import (
	"sort"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The store satisfies the full read-only Provider contract.
var _ base.Provider = (*Store)(nil)

// TestProviderTypesParity is the drift guard for the schema enumeration: the
// type set the store's Types() serves must equal the reference schema's
// reachable type set, on the multi-source fixture (prefix + AsModule +
// read-only + extension). Both sides are measured by the SAME reachability walk
// from the SAME seeds (roots + shared + system prelude), so the comparison is
// apples-to-apples: the reference is a flat store of every generated type
// including orphans no query can reach, but reachability excludes those on both
// sides. A mismatch means the two schemas expose a different reachable surface
// — a real generation divergence, not an enumeration artifact.
func TestProviderTypesParity(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)
	ref := goldenRefSources(t, genMultiFixtures)

	seeds := store.schemaSeeds(ctx)
	got := toSet(reachableTypeNames(ctx, store, seeds))
	want := toSet(reachableTypeNames(ctx, ref, seeds))

	assert.Equal(t, missingFrom(want, got), []string(nil),
		"reachable in the reference but not served by the store")
	assert.Equal(t, missingFrom(got, want), []string(nil),
		"served by the store but not reachable in the reference")
}

func toSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

// missingFrom returns the sorted keys of want that are absent from have.
func missingFrom(want, have map[string]struct{}) []string {
	var out []string
	for name := range want {
		if _, ok := have[name]; !ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// TestProviderRootsAndDirectives covers the trivial Provider surfaces: the
// operation roots resolve through synthesis and the directive set is delegated
// to the static prelude.
func TestProviderRootsAndDirectives(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	require.NotNil(t, store.QueryType(ctx), "Query root synthesized")
	require.NotNil(t, store.MutationType(ctx), "Mutation root synthesized")

	// Directive delegation: a well-known system directive resolves.
	require.NotNil(t, store.DirectiveForName(ctx, base.CatalogDirectiveName),
		"catalog directive delegated to the static prelude")

	var directiveCount int
	for range store.DirectiveDefinitions(ctx) {
		directiveCount++
	}
	assert.Greater(t, directiveCount, 0, "directive set delegated to the static prelude")

	assert.Empty(t, store.Description(ctx), "no root schema description")
}
