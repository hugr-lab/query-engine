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
	"github.com/vektah/gqlparser/v2/ast"
)

// The live-corpus check (Ш4 final): real-world SDL schemas mirrored from the
// live dev instance into fixtures/schemas/<source>/ (git-ignored, copied by
// hand — no hard dependency on the neighbour repos) go through both paths
// with the live options (engine/prefix/as_module/read_only), and EVERY type
// of the compiled reference must be served identically by the store.
// Self-described sources (op2023, osm.*, taxi, ice_demo, …) need their live
// backends and are out of scope. The test skips when the corpus is absent.

// liveCorpusDir is the repo-local corpus root, relative to this package.
var liveCorpusDir = filepath.Join("..", "..", "..", "fixtures", "schemas")

// liveCorpusSource mirrors one row of the live core.data_sources registry;
// the SDL lives in fixtures/schemas/<name>/*.graphql.
type liveCorpusSource struct {
	name       string
	engineType string
	prefix     string
	readOnly   bool
}

var liveCorpus = []liveCorpusSource{
	{"azure_wh", "mssql", "az", false},
	{"northwind", "postgres", "nw", false},
	{"hr_crm", "mysql", "hr", false},
	{"adventureworks", "mssql", "aw", false},
	{"tf", "postgres", "tf", false},
	{"openweathermap", "http", "owm", false},
}

// loadLiveCorpus reads each source's SDL (URISource semantics: *.graphql of
// the source directory ordered by filename). ok=false when anything is
// missing.
func loadLiveCorpus(t *testing.T) ([]fixtureSource, bool) {
	t.Helper()
	var out []fixtureSource
	for _, src := range liveCorpus {
		dir := filepath.Join(liveCorpusDir, src.name)
		if _, err := os.Stat(dir); err != nil {
			t.Logf("live corpus source %s: %v — skipping the corpus test", src.name, err)
			return nil, false
		}
		files, err := filepath.Glob(filepath.Join(dir, "*.graphql"))
		require.NoError(t, err)
		if len(files) == 0 {
			t.Logf("live corpus source %s: no *.graphql in %s — skipping the corpus test", src.name, dir)
			return nil, false
		}
		sort.Strings(files)
		var parts []string
		for _, f := range files {
			content, err := os.ReadFile(f)
			require.NoError(t, err)
			parts = append(parts, string(content))
		}
		out = append(out, fixtureSource{
			name:       src.name,
			schema:     strings.Join(parts, "\n"),
			engineType: src.engineType,
			prefix:     src.prefix,
			asModule:   true,
			readOnly:   src.readOnly,
		})
	}
	return out, true
}

func TestGenGoldenLiveCorpus(t *testing.T) {
	fixtures, ok := loadLiveCorpus(t)
	if !ok {
		t.Skip("live SDL corpus not present on this machine")
	}
	store, ctx := storeForSources(t, fixtures)
	ref := goldenRefSources(t, fixtures)

	// Full sweep: every non-builtin type the reference compiled must be
	// served identically.
	schema := ref.Schema()
	names := make([]string, 0, len(schema.Types))
	for name, def := range schema.Types {
		if def.BuiltIn {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	t.Logf("live corpus: %d sources, %d reference types", len(fixtures), len(names))

	diffs, supersets, oldPathQuirks := 0, 0, 0
	for _, name := range names {
		want := schema.Types[name]
		// Cache parity: a repeat read serves the SAME definition (cached
		// classes by pointer identity, static classes are stable anyway).
		cached := store.ForName(ctx, name)
		if cached == nil {
			diffs++
			assert.NotNil(t, cached, "store must serve %s", name)
			continue
		}
		if cached2 := store.ForName(ctx, name); cached2 != cached {
			diffs++
			assert.Fail(t, "cache parity", "second read of %s is a different definition", name)
		}
		// SDL parity is against the PRE-FINALISE generation: the store enriches
		// synthetic-member descriptions beyond the retiring compiler, which is
		// verified separately (descriptions_test.go), not by this oracle.
		got := store.forNameRaw(ctx, name)
		if got == nil {
			diffs++
			assert.NotNil(t, got, "store must serve %s", name)
			continue
		}
		// Old-path quirks tolerated on the REFERENCE side (logged, never
		// silent): @wfs* directives (intentionally dropped from the store —
		// the surface moves to a separate service), duplicated @catalog tags
		// and lost input-field defaults on residual types.
		want, oldPathQuirks = normalizeOldPathQuirks(want, got, oldPathQuirks)
		// Documented superset semantics (requirements §5.1): the compiler
		// creates sub-aggregation relation members and insert-relation
		// subqueries lazily in declaration order; the store serves the
		// deterministic full shape. Extra store members on those types are
		// allowed — everything the reference HAS must still match exactly.
		if strings.HasSuffix(name, "_sub_aggregation") || strings.HasSuffix(name, mutInputDataSuffix) {
			restricted, dropped := restrictToFields(got, want)
			supersets += dropped
			got = restricted
		}
		if !assert.Equal(t, goldenSDL(want), goldenSDL(got), "definition parity for %s", name) {
			diffs++
			if diffs >= 12 {
				t.Fatalf("stopping after %d diffs", diffs)
			}
		}
	}
	t.Logf("superset members tolerated: %d; old-path quirks normalized: %d", supersets, oldPathQuirks)
}

// normalizeOldPathQuirks strips known old-compiled-path artifacts from the
// reference definition: @wfs/@wfs_field/@wfs_exclude (dropped by design),
// duplicate identical directives (double @catalog tagging), and input-field
// defaults the compiled path loses on residual inputs (the store keeps them —
// dropped from BOTH sides for the compare).
func normalizeOldPathQuirks(want, got *ast.Definition, count int) (*ast.Definition, int) {
	w := *want
	w.Directives = nil
	for _, d := range want.Directives {
		if strings.HasPrefix(d.Name, "wfs") || d.Name == "feature" {
			count++
			continue
		}
		w.Directives = append(w.Directives, d)
	}
	if want.Kind == ast.InputObject {
		w.Fields = make(ast.FieldList, len(want.Fields))
		for i, f := range want.Fields {
			w.Fields[i] = f
			if f.DefaultValue == nil {
				if gf := got.Fields.ForName(f.Name); gf != nil && gf.DefaultValue != nil {
					nf := *gf
					count++
					w.Fields[i] = &nf
				}
			}
		}
	}
	// Under prefixing the old path LOSES the m2m projection @references and
	// the @filter_list_input marker (their field extensions resolve through
	// @original_name, the directive stamping does not) — adopt the store's
	// extra ones so the loss doesn't mask other diffs.
	wantKeys := make(map[string]bool, len(w.Directives))
	for _, d := range w.Directives {
		wantKeys[d.Name+"\x1f"+directiveKey(d)] = true
	}
	for _, d := range got.Directives {
		if d.Name != base.ReferencesDirectiveName && d.Name != base.FilterListInputDirectiveName {
			continue
		}
		if d.Name == base.ReferencesDirectiveName && base.DirectiveArgString(d, base.ArgIsM2M) != "true" {
			continue
		}
		if !wantKeys[d.Name+"\x1f"+directiveKey(d)] {
			count++
			w.Directives = append(w.Directives, d)
		}
	}
	return &w, count
}

// restrictToFields drops got's fields absent from want (returns the dropped
// count so the tolerance is visible, never silent).
func restrictToFields(got, want *ast.Definition) (*ast.Definition, int) {
	keep := make(map[string]bool, len(want.Fields))
	for _, f := range want.Fields {
		keep[f.Name] = true
	}
	d := *got
	d.Fields = nil
	dropped := 0
	for _, f := range got.Fields {
		if keep[f.Name] {
			d.Fields = append(d.Fields, f)
		} else {
			dropped++
		}
	}
	return &d, dropped
}
