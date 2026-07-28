//go:build duckdb_arrow

package store

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The store satisfies the full read-only Provider contract.
var _ base.Provider = (*Store)(nil)

// reachableGolden is the frozen reachable surface of genMultiFixtures.
const reachableGolden = "multi_reachable.txt"

// TestProviderTypesParity is the drift guard for the schema enumeration: the
// reachable type set the store serves on the multi-source fixture (prefix +
// AsModule + read-only + extension) must equal the frozen list.
//
// The list is not an arbitrary snapshot of the store. Until design-036 deleted
// the GENERATE rules this test compared the store against the COMPILED
// reference, walking both from the same seeds, and it passed — so the set
// frozen here is the reference's reachable surface, captured at the last commit
// where both implementations existed. What it guards is unchanged: a type that
// silently appears in, or disappears from, what a query can reach.
func TestProviderTypesParity(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	got := reachableTypeNames(ctx, store, store.schemaSeeds(ctx))
	sort.Strings(got)

	path := filepath.Join(goldenDir, reachableGolden)
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(path, []byte(strings.Join(got, "\n")+"\n"), 0o644))
	}
	raw, err := os.ReadFile(path)
	require.NoErrorf(t, err, "read %s (UPDATE_GOLDEN=1 to create)", path)
	want := strings.Split(strings.TrimSpace(string(raw)), "\n")

	assert.Equal(t, missingFrom(toSet(want), toSet(got)), []string(nil),
		"in the frozen surface but no longer served by the store")
	assert.Equal(t, missingFrom(toSet(got), toSet(want)), []string(nil),
		"served by the store but not in the frozen surface")
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
