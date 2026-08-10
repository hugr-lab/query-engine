//go:build duckdb_arrow

package entity_test

import (
	"context"
	"strings"
	"testing"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// _search — the engine's own logical-model search, the thing MCP's
// catalog-search used to reimplement out of GraphQL round trips.
//
// Two properties are worth testing here rather than in a unit test, because
// both only exist end to end: the ranking read really runs against the
// core.catalog.* views (a column that does not exist there produces a silent
// lexical fallback, which is indistinguishable from "no embedder configured"
// unless you read lexicalReason), and the permission filter really is the same
// predicate _dataObject uses.

// searchRoot is the shape every test here selects: enough to identify a hit
// without pulling the drill-down.
const searchRoot = `lexical lexicalReason hasMore filteredOut limit offset
	items { kind matchedOn name moduleName dataSourceName score objectName type hugrType refObjectName }`

type searchHit struct {
	Kind          string  `json:"kind"`
	Name          string  `json:"name"`
	ModuleName    string  `json:"moduleName"`
	DataSource    string  `json:"dataSourceName"`
	Score         float64 `json:"score"`
	ObjectName    string  `json:"objectName"`
	MatchedOn     string  `json:"matchedOn"`
	GQLType       string  `json:"type"`
	HugrType      string  `json:"hugrType"`
	RefObjectName string  `json:"refObjectName"`
}

type searchPage struct {
	Lexical       bool        `json:"lexical"`
	LexicalReason string      `json:"lexicalReason"`
	HasMore       bool        `json:"hasMore"`
	FilteredOut   int         `json:"filteredOut"`
	Limit         int         `json:"limit"`
	Offset        int         `json:"offset"`
	Items         []searchHit `json:"items"`
}

func runSearch(t testing.TB, ctx context.Context, svc *hugr.Service, args string) searchPage {
	t.Helper()
	res, err := svc.Query(ctx, "{ _search("+args+") { "+searchRoot+" } }", nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	defer res.Close()
	var page searchPage
	require.NoError(t, res.ScanData("_search", &page))
	return page
}

func hitNamesOf(page searchPage) []string {
	out := make([]string, 0, len(page.Items))
	for _, h := range page.Items {
		out = append(out, h.Name)
	}
	return out
}

// assertTracksOrdered is the ordering contract of a page. Scores are ordered
// WITHIN a track and never across: the two scales do not compare, which is
// why BOTH concatenates the name block before the meaning block instead of
// merge-sorting. A page from a single-track search degenerates to the plain
// "ordered by score" assertion.
func assertTracksOrdered(t testing.TB, page searchPage) {
	t.Helper()
	meaningStarted := false
	last := map[string]float64{"NAME": 2, "MEANING": 2}
	for _, h := range page.Items {
		require.Contains(t, last, h.MatchedOn, "hit %s carries an unknown track", h.Name)
		if h.MatchedOn == "MEANING" {
			meaningStarted = true
		} else {
			assert.False(t, meaningStarted, "a NAME hit after the MEANING block: %s", h.Name)
		}
		assert.LessOrEqual(t, h.Score, last[h.MatchedOn],
			"scores ordered within the %s track (hit %s)", h.MatchedOn, h.Name)
		last[h.MatchedOn] = h.Score
	}
}

// hitIdentity is the cross-track identity of a hit, mirroring candidateKey in
// the engine: a function is (module, name) — same-named functions in
// different modules are different functions — and a field's identity is its
// owning object.
func hitIdentity(h searchHit) string {
	return h.Kind + "/" + h.ModuleName + "/" + h.ObjectName + "/" + h.Name
}

// adminCtx is what the endpoint middleware installs for an admin caller.
func adminCtx(t testing.TB) context.Context {
	t.Helper()
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Role: "admin", UserId: "admin", UserName: "admin", AuthType: "test", AuthProvider: "test",
	})
	return perm.CtxWithPerm(ctx, &perm.RolePermissions{Name: "admin"})
}

func roleCtx(t testing.TB, p *perm.RolePermissions) context.Context {
	t.Helper()
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Role: p.Name, UserId: p.Name, UserName: p.Name, AuthType: "test", AuthProvider: "test",
	})
	return perm.CtxWithPerm(ctx, p)
}

// TestSearchRankingQueryMatchesCatalogViews is the guard against a ranking
// query that silently never runs. With a vector size configured the catalog
// views carry the index columns and the ranking read must reach them: the
// ONLY acceptable fallback reason is the missing embedder model. Pinning the
// one good reason instead of a list of bad ones is what lets this catch a
// break at ANY layer — validation, a view whose SQL lost a column, a missing
// relation. (The same guard lives in TestCatalogSearchRankingReachesTheViews
// on the MCP side; keep them in step.)
func TestSearchRankingQueryMatchesCatalogViews(t *testing.T) {
	svc, _ := mcpService(t, 8, "")
	page := runSearch(t, adminCtx(t), svc, `query: "data sources", limit: 20`)

	require.NotEmpty(t, page.LexicalReason, "no embedder is configured here, so the vector path must fall back")
	assert.Contains(t, page.LexicalReason, "_system_embedder",
		"the ranking read fails against the catalog views for a reason other than the missing embedder: %s",
		page.LexicalReason)
}

// TestSearchLexicalAnswers — without an embedder the lexical path must still
// answer, and must say so.
func TestSearchLexicalAnswers(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	page := runSearch(t, adminCtx(t), svc, `query: "data sources", limit: 50`)

	require.True(t, page.Lexical, "no embedder configured: the answer must be marked lexical")
	require.NotEmpty(t, page.LexicalReason, "a fallback must say why")
	require.NotEmpty(t, page.Items, "lexical search answers on the entity storage")
	assert.Contains(t, hitNamesOf(page), "core_data_sources")

	// The default match is BOTH, so the page is two blocks; global score
	// ordering is deliberately NOT an invariant here.
	assertTracksOrdered(t, page)
	for _, h := range page.Items {
		assert.NotEmpty(t, h.Kind)
	}
}

// TestSearchLexicalReachesFields is the capability MCP's fallback never had:
// a structural walk has no field enumeration that is not per object, so field
// hits used to vanish whenever the embedder did. core.catalog.fields is one
// table.
func TestSearchLexicalReachesFields(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	page := runSearch(t, adminCtx(t), svc, `query: "description", kinds: [FIELD], limit: 50`)

	require.True(t, page.Lexical)
	require.NotEmpty(t, page.Items, "the lexical path must reach fields")
	for _, h := range page.Items {
		assert.Equal(t, "FIELD", h.Kind)
		assert.NotEmpty(t, h.ObjectName, "a field hit names its owning object")
		assert.NotEmpty(t, h.GQLType, "a field hit carries its GraphQL type")
		// Only these four can come out of the index: relation navigation
		// fields and @extra_field companions are generated when the GraphQL
		// type is built and are never stored, so they can never be a hit.
		assert.Contains(t, []string{"column", "calculated", "function", "select"}, h.HugrType,
			"field %s.%s", h.ObjectName, h.Name)
		if h.HugrType == "select" {
			assert.NotEmpty(t, h.RefObjectName, "a declared @join names where it leads")
		}
	}
}

// TestSearchDrillDown — a hit expands into the full logical entity through the
// ordinary _catalog resolvers, by selection set alone. That is what lets a
// client get ranked results AND their detail in one round trip.
func TestSearchDrillDown(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	res, err := svc.Query(adminCtx(t), `{
		_search(query: "data sources", kinds: [DATA_OBJECT], limit: 5) {
			items {
				name
				dataObject { name type moduleName primaryKey queries { name type } }
			}
		}
	}`, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	defer res.Close()

	var page struct {
		Items []struct {
			Name       string `json:"name"`
			DataObject *struct {
				Name       string   `json:"name"`
				Type       string   `json:"type"`
				ModuleName string   `json:"moduleName"`
				PrimaryKey []string `json:"primaryKey"`
				Queries    []struct {
					Name string `json:"name"`
					Type string `json:"type"`
				} `json:"queries"`
			} `json:"dataObject"`
		} `json:"items"`
	}
	require.NoError(t, res.ScanData("_search", &page))
	require.NotEmpty(t, page.Items)
	for _, it := range page.Items {
		require.NotNil(t, it.DataObject, "a DATA_OBJECT hit must resolve its object")
		assert.Equal(t, it.Name, it.DataObject.Name)
		assert.NotEmpty(t, it.DataObject.Queries, "the drill-down reaches the generated queries")
	}
}

// TestSearchFiltersHiddenObject — the index is read with full access, so a
// hidden object DOES rank; what must never happen is it reaching the caller.
// The admin half of the assertion is what makes this a filtering test rather
// than a "today's ranking put it elsewhere" test.
func TestSearchFiltersHiddenObject(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	const args = `query: "api keys", kinds: [DATA_OBJECT], limit: 200`

	full := runSearch(t, adminCtx(t), svc, args)
	require.Contains(t, hitNamesOf(full), "core_api_keys", "the index ranks the object unrestricted")

	restricted := runSearch(t, roleCtx(t, &perm.RolePermissions{
		Name: "search_vis",
		Permissions: []perm.Permission{
			{Object: "data-object:query", Field: "core_api_keys", Hidden: true},
		},
	}), svc, args)
	assert.NotContains(t, hitNamesOf(restricted), "core_api_keys",
		"a hidden object must not survive the filter")
	assert.Positive(t, restricted.FilteredOut,
		"filteredOut is how a caller tells 'nothing matches' from 'nothing you may see matches'")
}

// TestSearchNarrowingIsNotFiltering — the caller's own scoping must not be
// reported as a permission denial. Reading filteredOut as "there is data here
// you may not see" when the caller simply asked for less is the opposite of
// what happened.
func TestSearchNarrowingIsNotFiltering(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	page := runSearch(t, adminCtx(t), svc,
		`query: "data sources", kinds: [DATA_OBJECT], module: "definitely_not_a_module", limit: 50`)

	assert.Empty(t, page.Items, "nothing lives in that module")
	assert.Zero(t, page.FilteredOut, "module scoping is narrowing, not filtering")
}

// TestSearchArgumentValidation — a bad argument is an error, not an empty page.
func TestSearchArgumentValidation(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	ctx := adminCtx(t)

	for _, tc := range []struct{ name, args string }{
		{"empty query", `query: "   "`},
		{"unknown kind", `query: "x", kinds: [NOPE]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := svc.Query(ctx, "{ _search("+tc.args+") { hasMore } }", nil)
			if err == nil {
				defer res.Close()
				err = res.Err()
			}
			require.Error(t, err, "a bad argument must not produce an empty page")
		})
	}
}

// TestSearchLimitClamped — limit is clamped rather than rejected, and the page
// reports what was actually used.
func TestSearchLimitClamped(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	page := runSearch(t, adminCtx(t), svc, `query: "data", limit: 5000`)
	assert.Equal(t, 200, page.Limit, "limit is clamped to the maximum")
}

// TestMetaSurfaceSurvivesWildcardDeny is the @meta contract, and it is the
// reason the marker exists at all.
//
// A deployment that locks down with a wildcard row and grants back explicitly
// used to lose logical-model introspection AND standard __schema
// introspection, because the permission rule matched ("*","*") against every
// field the walker visits — including the meta-fields, which are resolved on
// the metadata path and were never part of the role-governed surface in the
// first place.
func TestMetaSurfaceSurvivesWildcardDeny(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	locked := roleCtx(t, &perm.RolePermissions{
		Name: "locked_down",
		Permissions: []perm.Permission{
			{Object: "*", Field: "*", Hidden: true, Disabled: true},
		},
	})

	t.Run("standard introspection", func(t *testing.T) {
		res, err := svc.Query(locked, `{ __schema { queryType { name } } }`, nil)
		require.NoError(t, err)
		require.NoError(t, res.Err(), "a wildcard deny must not take __schema with it")
		res.Close()
	})

	t.Run("logical model", func(t *testing.T) {
		res, err := svc.Query(locked, `{ _catalog { name } }`, nil)
		require.NoError(t, err)
		require.NoError(t, res.Err(), "a wildcard deny must not take _catalog with it")
		res.Close()
	})

	t.Run("search answers, filtered to nothing", func(t *testing.T) {
		page := runSearch(t, locked, svc, `query: "data sources", limit: 50`)
		// The entry point is reachable; the CONTENT is still filtered per role,
		// which is the whole point — exempting the meta surface from the rules
		// does not exempt what it returns.
		assert.Empty(t, page.Items, "every object is hidden from this role")
		assert.Positive(t, page.FilteredOut, "and the caller is told that something was dropped")
	})

	t.Run("data surface is still denied", func(t *testing.T) {
		res, err := svc.Query(locked, `{ core { data_sources { name } } }`, nil)
		if err == nil {
			defer res.Close()
			err = res.Err()
		}
		require.Error(t, err, "@meta must not leak into the data surface")
	})
}

// TestMetaSurfaceDisabledRoleStillRefused — the marker takes a surface out of
// the RULES, not out of authentication.
func TestMetaSurfaceDisabledRoleStillRefused(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	disabled := roleCtx(t, &perm.RolePermissions{Name: "off", Disabled: true})

	res, err := svc.Query(disabled, `{ _catalog { name } }`, nil)
	if err == nil {
		defer res.Close()
		err = res.Err()
	}
	require.Error(t, err, "a disabled role is refused everywhere, meta surface included")
}

// TestSearchVectorRanking is the one test that actually RANKS. Everything else
// survives without an embedder, which is exactly why the vector path could rot
// unnoticed: the fallback answers and nothing ever runs the query that matters.
func TestSearchVectorRanking(t *testing.T) {
	url, size := liveEmbedder(t)
	svc, _ := mcpService(t, size, url)

	page := runSearch(t, adminCtx(t), svc,
		`query: "where are the attached databases described", limit: 10`)

	require.Empty(t, page.LexicalReason, "vector ranking must be the path taken, not the fallback")
	require.False(t, page.Lexical)
	require.NotEmpty(t, page.Items)

	assertTracksOrdered(t, page)

	// The query shares no substring with the answer — that is the point. A
	// lexical ranker scores 0 for "where are the attached databases described"
	// against the data-source catalog; only an embedding connects them. The
	// assertion stays on the SUBJECT rather than one exact name, because
	// several entities describe attached sources and which one wins is the
	// embedder's business, not this test's.
	names := hitNamesOf(page)
	var found bool
	for _, n := range names {
		if strings.Contains(n, "data_source") {
			found = true
			break
		}
	}
	assert.True(t, found, "semantic search should reach the data-source catalog, got %v", names)
}

// TestSearchByName is the track the vector index cannot serve: embeddings are
// made from DESCRIPTIONS, so an identifier never enters them, and on a
// deployment WITH an embedder a caller who typed a name used to get whatever
// was described in similar words instead of the thing they named.
//
// The underscore is the point of the fixture, not decoration. hugr names are
// underscore-separated identifiers, and the ilike prefilter has no ESCAPE
// clause — escaping "_" as "\_" matched literally and returned nothing, which
// is invisible to any test whose query happens to be one plain word.
func TestSearchByName(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	ctx := adminCtx(t)

	exact := runSearch(t, ctx, svc, `query: "core_api_keys", kinds: [DATA_OBJECT], match: NAME, limit: 10`)
	require.NotEmpty(t, exact.Items, "an exact name must find its object")
	assert.Equal(t, "core_api_keys", exact.Items[0].Name)
	assert.Equal(t, "NAME", exact.Items[0].MatchedOn)
	assert.Equal(t, 1.0, exact.Items[0].Score, "an exact name is the top of the name scale")
	assert.False(t, exact.Lexical, "substring matching is the POINT of this track, not a fallback")

	partial := runSearch(t, ctx, svc, `query: "api_keys", kinds: [DATA_OBJECT], match: NAME, limit: 10`)
	assert.Contains(t, hitNamesOf(partial), "core_api_keys", "a fragment of the name must still find it")

	// Multi-word: every term must land in the NAME. The prefilter demands the
	// same (AND of terms), so a two-word query cannot fill its window with
	// rows matching one word and then score them all to zero.
	multi := runSearch(t, ctx, svc, `query: "api keys", kinds: [DATA_OBJECT], match: NAME, limit: 10`)
	assert.Contains(t, hitNamesOf(multi), "core_api_keys", "every term matches the name, so the name track must find it")

	// A name track ranks names. A word that appears only in descriptions is
	// not a name match, however well it would do semantically.
	none := runSearch(t, ctx, svc,
		`query: "who can log in and with what key", kinds: [DATA_OBJECT], match: NAME, limit: 10`)
	assert.Empty(t, none.Items, "prose is not an identifier")
}

// TestSearchBothMinScoreKeepsNames — minScore reads on the MEANING scale and
// binds only that track. A caller who tuned a semantic similarity bar AND
// typed an identifier fragment must not have the bar delete the very object
// they named: 0.9 comfortably kills every lexical meaning hit here, and the
// name hit must survive it.
func TestSearchBothMinScoreKeepsNames(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	page := runSearch(t, adminCtx(t), svc,
		`query: "api_keys", kinds: [DATA_OBJECT], match: BOTH, minScore: 0.9, limit: 10`)

	require.Contains(t, hitNamesOf(page), "core_api_keys",
		"a meaning-scale bar must not delete a name-track hit")
	for _, h := range page.Items {
		if h.MatchedOn == "MEANING" {
			assert.GreaterOrEqual(t, h.Score, 0.9, "the bar still binds the meaning track: %s", h.Name)
		}
	}
}

// TestSearchBothPutsNamesFirst — the two tracks rank on scales that do not
// compare (an exact identifier against an embedding distance), so BOTH
// concatenates them rather than merge-sorting. Blending is what buried an
// object under its own fields.
func TestSearchBothPutsNamesFirst(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	page := runSearch(t, adminCtx(t), svc, `query: "core_api_keys", match: BOTH, limit: 20`)

	require.NotEmpty(t, page.Items)
	first := page.Items[0]
	assert.Equal(t, "NAME", first.MatchedOn, "a name match leads")
	assert.Equal(t, "core_api_keys", first.Name)

	// Once a name hit covers an entity, the meaning track must not repeat it.
	seen := map[string]int{}
	for _, h := range page.Items {
		seen[hitIdentity(h)]++
	}
	for key, n := range seen {
		assert.Equal(t, 1, n, "duplicate across tracks: %s", key)
	}

	// And every hit says which track found it, blocks in order.
	assertTracksOrdered(t, page)
}

// TestSearchMeaningExcludesTheNameTrack — MCP pins match: MEANING, so the
// track has to stay pure: no name hits leaking in, whatever the query.
func TestSearchMeaningExcludesTheNameTrack(t *testing.T) {
	svc, _ := mcpService(t, 0, "")
	page := runSearch(t, adminCtx(t), svc, `query: "core_api_keys", match: MEANING, limit: 20`)
	require.NotEmpty(t, page.Items)
	for _, h := range page.Items {
		assert.Equal(t, "MEANING", h.MatchedOn, "hit %s", h.Name)
	}
}

// TestSearchBothWithEmbedder is TestSearchBothPutsNamesFirst against the REAL
// meaning track. Without an embedder both tracks are substring-driven over
// the same rows, so "names lead" and "no duplicates" hold trivially there;
// only a vector ranking can produce a semantic hit that would outrank or
// collide with a name hit, which is exactly what these assertions pin.
func TestSearchBothWithEmbedder(t *testing.T) {
	url, size := liveEmbedder(t)
	svc, _ := mcpService(t, size, url)
	page := runSearch(t, adminCtx(t), svc, `query: "core_api_keys", match: BOTH, limit: 20`)

	require.False(t, page.Lexical, "the meaning track must be the vector index, not the fallback")
	require.NotEmpty(t, page.Items)
	first := page.Items[0]
	assert.Equal(t, "NAME", first.MatchedOn, "a name match leads, however strong the semantic ranking")
	assert.Equal(t, "core_api_keys", first.Name)

	var meaningHits int
	seen := map[string]int{}
	for _, h := range page.Items {
		seen[hitIdentity(h)]++
		if h.MatchedOn == "MEANING" {
			meaningHits++
		}
	}
	assert.Positive(t, meaningHits, "the vector track fills the rest of the page")
	for key, n := range seen {
		assert.Equal(t, 1, n, "duplicate across tracks: %s", key)
	}
	assertTracksOrdered(t, page)
}

// TestSearchMeaningWithEmbedderExcludesNames — the purity of the MEANING
// track, checked against the vector ranking MCP actually runs on.
func TestSearchMeaningWithEmbedderExcludesNames(t *testing.T) {
	url, size := liveEmbedder(t)
	svc, _ := mcpService(t, size, url)
	page := runSearch(t, adminCtx(t), svc, `query: "core_api_keys", match: MEANING, limit: 20`)

	require.False(t, page.Lexical)
	require.NotEmpty(t, page.Items)
	for _, h := range page.Items {
		assert.Equal(t, "MEANING", h.MatchedOn, "hit %s", h.Name)
	}
}
