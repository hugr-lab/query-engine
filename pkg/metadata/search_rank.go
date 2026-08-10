package metadata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/base"
)

// Ranking is the half of _search that reads the INDEX, and it is the only half
// that reads privileged.
//
// The index lives in the core.catalog.* views, which carry no row-level
// permission filtering by design (they are an administrative surface — see the
// access note in runtime/core-db/schema_catalog_tmpl.graphql). A role may hold
// no rights on them at all; operators routinely hide the catalog from end-user
// roles. Refusing to rank for those roles would make search useless for
// exactly the callers it exists to serve, so the read runs with full access
// and NOTHING it returns reaches the client without passing the permission
// filter in search.go first.
//
// Two modes, one shape. With an embedder the views expose
// _distance_to_query(query:) and the ranking is semantic; without one the
// field does not exist, and the same per-kind query runs with an ilike
// prefilter instead, scored in Go. The result says which happened
// (_SearchResult.lexical / lexicalReason), because a silent fallback is
// indistinguishable from a broken ranking query and would stay broken
// unnoticed.

// candidate is one unverified ranking result: an identity plus a score. It
// carries only what the INDEX knows — a field's module, for instance, is not
// in core.catalog.fields (a field belongs to whatever module owns its object) and is
// filled in later, from the owner.
type candidate struct {
	kind        string
	name        string
	object      string // field candidates: the owning data object
	module      string
	dataSource  string
	description string
	score       float64
	// matchedOn is the track that produced this candidate — NAME or MEANING.
	// Scores are comparable within a track and not across them, which is why
	// the two are concatenated rather than merge-sorted.
	matchedOn string

	// Field candidates only, read straight off the index row. The writer
	// stored these properties; the reconstructed field definition is BUILT
	// from them, so classifying here rather than from the rebuilt AST costs
	// one relations query less per owner object and cannot disagree with it.
	gqlType   string
	hugrType  string
	refObject string
}

// entityView describes one kind's ranking query against the catalog views
// (core.catalog.*), which are the SQL half of the logical model.
type entityView struct {
	alias     string   // the view's field name under `core.catalog`
	selection []string // columns to read besides the ranking expression
	// nameCols are the text columns the lexical prefilter searches.
	nameCols []string
}

var entityViews = map[string]entityView{
	searchKindModule:     {"modules", []string{"name", "description"}, []string{"name", "description"}},
	searchKindDataSource: {"active_sources", []string{"name", "description"}, []string{"name", "description"}},
	searchKindDataObject: {"data_objects", []string{"name", "module", "data_source", "description"}, []string{"name", "description"}},
	searchKindFunction:   {"functions", []string{"name", "module", "data_source", "description"}, []string{"name", "description"}},
	// Fields read their stored PROPERTIES too: those say what the field is
	// (see fieldRole), so a hit needs neither the rebuilt definition nor a
	// relations query to be classified.
	searchKindField: {"fields",
		[]string{"type_name", "name", "field_type", "data_source", "description", fieldPropsSelection},
		[]string{"name", "description"}},
}

// fieldPropsSelection reads the stored discriminators of a field: what the
// writer recorded about it, which is exactly what says whether it is a stored
// column, a computed expression, a declared join or a function call.
const fieldPropsSelection = `properties {
	computed
	join { references_name }
	function_call { function { name } }
	table_function_call_join { function { name } }
}`

// entityRow is the union of the columns the five views contribute.
type entityRow struct {
	Name       string  `json:"name"`
	TypeName   string  `json:"type_name"`
	FieldType  string  `json:"field_type"`
	Module     string  `json:"module"`
	DataSource string  `json:"data_source"`
	Desc       string  `json:"description"`
	Distance   float64 `json:"_distance_to_query"`
	// The properties bag arrives with every branch PRESENT and its members
	// null — a selected struct field is an object, not an absent one — so the
	// discriminator is the inner value, never the pointer.
	Properties *struct {
		Computed bool `json:"computed"`
		Join     struct {
			ReferencesName string `json:"references_name"`
		} `json:"join"`
		FunctionCall          fieldFunctionBinding `json:"function_call"`
		TableFunctionCallJoin fieldFunctionBinding `json:"table_function_call_join"`
	} `json:"properties"`
}

type fieldFunctionBinding struct {
	Function struct {
		Name string `json:"name"`
	} `json:"function"`
}

// fieldRole maps a stored field row onto hugr's field vocabulary — the same
// values hugrFieldType derives from the rebuilt definition, from the same
// facts. Only these four can ever come out of the index: relation navigation
// fields and @extra_field companions are generated when the GraphQL type is
// built and are never stored, so search can never hit one.
func fieldRole(row entityRow) (hugrType, refObject string) {
	p := row.Properties
	if p == nil {
		return string(base.HugrTypeFieldColumn), ""
	}
	switch {
	case p.Join.ReferencesName != "":
		// A declared @join is a PATH: it lands on another data object's rows,
		// which is why it reads as a select rather than as a function.
		return string(base.HugrTypeFieldSelect), p.Join.ReferencesName
	case p.FunctionCall.Function.Name != "", p.TableFunctionCallJoin.Function.Name != "":
		return string(base.HugrTypeFieldFunction), ""
	case p.Computed:
		return string(base.HugrTypeFieldCalculated), ""
	}
	return string(base.HugrTypeFieldColumn), ""
}

var errNoQuerier = errors.New("logical-model search needs an engine to rank with, and none was provided")

// rankCandidates produces the ordered candidate list for the requested track.
// It returns the REASON the vector path was unusable ("" when it was used, and
// always "" for a name-only search, where substring matching is the point
// rather than a fallback).
//
// NAME and MEANING are CONCATENATED, never merge-sorted: an exact name match
// scores 1.0 on a scale that has nothing to do with an embedding distance, and
// interleaving them by score is what buried aw_Product under whatever happened
// to be described in similar words. Name hits come first because a caller who
// typed an identifier meant it.
func rankCandidates(ctx context.Context, q Querier, req searchRequest, limit int) ([]candidate, string, error) {
	if q == nil {
		return nil, "", errNoQuerier
	}
	switch req.match {
	case matchName:
		named, err := rankByName(ctx, q, req, limit)
		return named, "", err
	case matchMeaning:
		return rankByMeaning(ctx, q, req, limit)
	}

	// BOTH. The tracks are independent reads of the same index, so they run
	// concurrently: serially, the default search — the human-facing path —
	// would pay the embedder round trip ON TOP of the name query instead of
	// alongside it.
	var (
		named, meaning []candidate
		reason         string
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		named, err = rankByName(gctx, q, req, limit)
		return err
	})
	g.Go(func() error {
		var err error
		meaning, reason, err = rankByMeaning(gctx, q, req, limit)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, "", err
	}

	// A name hit already covers its entity, so the meaning track only
	// contributes what the name track did not find. Keeping the NAME
	// representative is safe against the caller's minScore because that bar
	// binds only the meaning track (see search.go): dropping the meaning
	// duplicate can never delete the entity from the page.
	out := named
	seen := make(map[string]struct{}, len(out))
	for _, c := range out {
		seen[candidateKey(c)] = struct{}{}
	}
	for _, c := range meaning {
		if _, dup := seen[candidateKey(c)]; dup {
			continue
		}
		out = append(out, c)
	}
	// limit caps the verification work per REQUEST (searchMaxCandidates), not
	// per track; two tracks do not get to double it.
	if len(out) > limit {
		out = out[:limit]
	}
	return out, reason, nil
}

// candidateKey is the identity a hit answers to across tracks. A function is
// (module, name) everywhere else in this package — same-named functions in
// different modules are DIFFERENT functions, and a stock deployment has them
// (`checkpoint` lives in core and in core.ducklake). Field candidates carry no
// module at ranking time; both tracks read the same index rows, so both agree
// on "" and their identity is the owning object.
func candidateKey(c candidate) string {
	return c.kind + "\x1f" + c.module + "\x1f" + c.object + "\x1f" + c.name
}

// rankByMeaning is the semantic track, degrading to substring matching over
// the SAME text (name and description) when the vector index is unusable.
func rankByMeaning(ctx context.Context, q Querier, req searchRequest, limit int) ([]candidate, string, error) {
	hits, err := rankByVector(ctx, q, req, limit)
	if err == nil {
		return hits, "", nil
	}
	slog.Debug("_search: vector ranking unavailable, falling back to lexical", "error", err)
	hits, lexErr := rankLexically(ctx, q, req, limit)
	if lexErr != nil {
		return nil, "", fmt.Errorf("logical-model search: vector ranking failed (%v) and lexical ranking failed: %w", err, lexErr)
	}
	return hits, err.Error(), nil
}

// rankByVector orders each kind's view by embedding distance. One query, one
// alias per kind, so a five-kind search costs a single round trip.
func rankByVector(ctx context.Context, q Querier, req searchRequest, limit int) ([]candidate, error) {
	var body strings.Builder
	ordered := make([]string, 0, len(req.kinds))
	for _, kind := range req.kinds {
		view, ok := entityViews[kind]
		if !ok {
			continue
		}
		ordered = append(ordered, kind)
		fmt.Fprintf(&body, "%s: %s(%sorder_by: [{field: \"_distance_to_query\", direction: ASC}], limit: $limit) { %s _distance_to_query(query: $q) }\n",
			kind, view.alias, vectorFilterArg(kind, req), strings.Join(view.selection, " "))
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	vars := map[string]any{"q": req.query, "limit": limit}
	sig := "$q: String!, $limit: Int!"
	if objectFilterApplies(req) {
		vars["obj"] = req.object
		sig += ", $obj: String!"
	}

	batch := map[string][]entityRow{}
	if err := scanCatalogBatch(ctx, q, sig, body.String(), vars, &batch); err != nil {
		return nil, err
	}
	return collectCandidates(ordered, batch, matchMeaning, func(row entityRow) float64 {
		// Distance 0 is identical; a score is the other way round so a client
		// can reason about "higher is better" without knowing the metric.
		if row.Distance < 0 {
			return 0
		}
		return 1 - row.Distance
	}), nil
}

// rankLexically is the no-embedder path of the MEANING track. SQL only
// NARROWS — see rankBySubstring — and the actual scoring happens in Go, where
// a multi-word query narrows instead of widening.
//
// Doing the narrowing in the view rather than by walking the module tree is
// what lets this path reach FIELDS. A structural walk has no enumeration of
// fields that is not per object, which is why MCP's fallback dropped field
// hits entirely; core.catalog.fields is one table.
func rankLexically(ctx context.Context, q Querier, req searchRequest, limit int) ([]candidate, error) {
	return rankBySubstring(ctx, q, req, limit, substringTrack{
		matchedOn:     matchMeaning,
		prefilterCols: func(view entityView) []string { return view.nameCols },
		score: func(terms []string, row entityRow) float64 {
			return lexicalScore(terms, row.Name, row.Desc)
		},
	})
}

// rankByName matches the entity's NAME and nothing else.
//
// This is the track the vector index cannot serve at all: embeddings are made
// from DESCRIPTIONS, so an identifier never enters them, and on a deployment
// with an embedder a caller who typed aw_Product used to get whatever was
// described in similar words instead of the table they named.
//
// SQL narrows with ilike per term; the ordering is decided in Go, where an
// exact name outranks a prefix and a prefix outranks a substring.
func rankByName(ctx context.Context, q Querier, req searchRequest, limit int) ([]candidate, error) {
	return rankBySubstring(ctx, q, req, limit, substringTrack{
		matchedOn:     matchName,
		prefilterCols: func(view entityView) []string { return []string{"name"} },
		score: func(terms []string, row entityRow) float64 {
			return nameScore(terms, row.Name)
		},
	})
}

// substringTrack is what distinguishes the two substring pipelines: which
// columns the SQL prefilter searches, how a fetched row is scored, and which
// track the hits answer for. Everything else — term splitting, the tiered
// prefilter, scanning, zero-pruning, truncation — is shared in
// rankBySubstring, so a prefilter fix lands on both tracks or neither. (The
// ESCAPE bug lived as long as it did because the two copies could diverge
// silently.)
type substringTrack struct {
	matchedOn     string
	prefilterCols func(view entityView) []string
	score         func(terms []string, row entityRow) float64
}

// prefilterTier is one aliased sub-query of the substring prefilter, and the
// tiers are what makes a BOUNDED window safe. The views cannot rank a
// substring match, so a single wide ilike window on a large catalog fills
// with whatever arrives first and may not contain the very row the caller
// named. Narrow matches therefore arrive through windows of their own: an
// exact name cannot be crowded out of a window only exact names enter,
// however many weaker substring rows the wide tier over-fetches.
type prefilterTier struct {
	suffix string
	cond   func(view entityView) string
}

// rankBySubstring is the substring prefilter both tracks share: SQL narrows,
// Go scores. Every term must match SOME prefilter column — the same demand
// the scorer makes — so the window is not spent on rows the scorer will throw
// away. (An any-term prefilter let rows matching one word of a two-word query
// fill the whole window and then score 0, turning a full catalog into an
// empty page.) order_by name keeps each window deterministic across
// identical requests.
func rankBySubstring(ctx context.Context, q Querier, req searchRequest, limit int, track substringTrack) ([]candidate, error) {
	terms := strings.Fields(strings.ToLower(req.query))
	if len(terms) == 0 {
		return nil, nil
	}
	vars := map[string]any{"limit": limit}
	patterns := make([]string, len(terms))
	for i, term := range terms {
		name := fmt.Sprintf("t%d", i)
		vars[name] = likePattern(term)
		patterns[i] = "$" + name
	}
	sig := make([]string, 0, len(patterns)+4)
	sig = append(sig, "$limit: Int!")
	for _, p := range patterns {
		sig = append(sig, p+": String!")
	}

	var tiers []prefilterTier
	// A single-term query can be a whole identifier, and those get their own
	// windows: the term with no wildcards around it is the exact name (modulo
	// "_" matching any character — see likePattern), term% the prefixes. A
	// multi-term query cannot have every term as a prefix, so for those the
	// extra tiers would fetch nothing the wide one does not.
	if len(terms) == 1 {
		vars["tx"] = terms[0]
		vars["tp"] = terms[0] + "%"
		sig = append(sig, "$tx: String!", "$tp: String!")
		tiers = append(tiers,
			prefilterTier{"x", func(entityView) string { return "{name: {ilike: $tx}}" }},
			prefilterTier{"p", func(entityView) string { return "{name: {ilike: $tp}}" }},
		)
	}
	tiers = append(tiers, prefilterTier{"s", func(view entityView) string {
		cols := track.prefilterCols(view)
		perTerm := make([]string, len(patterns))
		for i, p := range patterns {
			if len(cols) == 1 {
				perTerm[i] = fmt.Sprintf("{%s: {ilike: %s}}", cols[0], p)
				continue
			}
			match := make([]string, len(cols))
			for j, col := range cols {
				match[j] = fmt.Sprintf("{%s: {ilike: %s}}", col, p)
			}
			perTerm[i] = "{_or: [" + strings.Join(match, ", ") + "]}"
		}
		return "{_and: [" + strings.Join(perTerm, ", ") + "]}"
	}})

	var body strings.Builder
	ordered := make([]string, 0, len(req.kinds))
	for _, kind := range req.kinds {
		view, ok := entityViews[kind]
		if !ok {
			continue
		}
		ordered = append(ordered, kind)
		for _, tier := range tiers {
			cond := tier.cond(view)
			if kind == searchKindField && objectFilterApplies(req) {
				cond = "{_and: [" + cond + ", {type_name: {eq: $obj}}]}"
			}
			fmt.Fprintf(&body, "%s_%s: %s(filter: %s, order_by: [{field: \"name\", direction: ASC}], limit: $limit) { %s }\n",
				kind, tier.suffix, view.alias, cond, strings.Join(view.selection, " "))
		}
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	if objectFilterApplies(req) {
		vars["obj"] = req.object
		sig = append(sig, "$obj: String!")
	}

	batch := map[string][]entityRow{}
	if err := scanCatalogBatch(ctx, q, strings.Join(sig, ", "), body.String(), vars, &batch); err != nil {
		return nil, err
	}

	// Merge the tiers back into one window per kind; a row can arrive through
	// several of them.
	merged := make(map[string][]entityRow, len(ordered))
	for _, kind := range ordered {
		seen := map[string]struct{}{}
		for _, tier := range tiers {
			for _, row := range batch[kind+"_"+tier.suffix] {
				id := row.TypeName + "\x1f" + row.Module + "\x1f" + row.Name
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}
				merged[kind] = append(merged[kind], row)
			}
		}
	}

	hits := collectCandidates(ordered, merged, track.matchedOn, func(row entityRow) float64 {
		return track.score(terms, row)
	})
	// The prefilter accepts wildcard over-matches; the score demands the real
	// thing.
	kept := hits[:0]
	for _, h := range hits {
		if h.score > 0 {
			kept = append(kept, h)
		}
	}
	if len(kept) > limit {
		kept = kept[:limit]
	}
	return kept, nil
}

// nameScore ranks a name match by how much of the name the query accounts for.
// An exact name is 1, a prefix is close behind, a substring further back, and
// a name missing any term scores 0 — a multi-word query narrows.
func nameScore(terms []string, name string) float64 {
	lname := strings.ToLower(name)
	if len(terms) == 1 && lname == terms[0] {
		return 1
	}
	var score float64
	for _, term := range terms {
		switch {
		case strings.HasPrefix(lname, term):
			score += 0.9
		case strings.Contains(lname, term):
			score += 0.6
		default:
			return 0
		}
	}
	return min(0.99, score/float64(len(terms)))
}

// vectorFilterArg narrows a kind's vector query where the request allows it.
// Only field hits can be object-scoped, and doing it in SQL keeps the
// overfetch window spent on rows that can still make the page.
func vectorFilterArg(kind string, req searchRequest) string {
	if kind == searchKindField && objectFilterApplies(req) {
		return "filter: {type_name: {eq: $obj}}, "
	}
	return ""
}

func objectFilterApplies(req searchRequest) bool {
	if req.object == "" {
		return false
	}
	for _, k := range req.kinds {
		if k == searchKindField {
			return true
		}
	}
	return false
}

// collectCandidates flattens the per-kind batch into one list ordered by
// score, every candidate stamped with the track that found it. The stamp is a
// parameter rather than a caller-side loop so a ranking path CANNOT forget
// it: matchedOn serves a non-null enum, and a forgotten stamp would put ""
// into the response.
func collectCandidates(ordered []string, batch map[string][]entityRow, matchedOn string, score func(entityRow) float64) []candidate {
	var hits []candidate
	for _, kind := range ordered {
		for _, row := range batch[kind] {
			c := candidate{
				kind: kind, name: row.Name, module: row.Module,
				dataSource: row.DataSource, description: row.Desc,
				score: score(row), matchedOn: matchedOn,
			}
			if kind == searchKindField {
				c.object = row.TypeName
				c.gqlType = row.FieldType
				c.hugrType, c.refObject = fieldRole(row)
			}
			hits = append(hits, c)
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	return hits
}

// lexicalScore rewards a name match over a description match and an earlier
// match over a later one. Every term must appear somewhere, so a multi-word
// query narrows instead of widening. Deliberately dumb — no term weighting,
// no stemming — because a crude ranking that always works beats a good one
// that is sometimes absent.
func lexicalScore(terms []string, name, description string) float64 {
	lname, ldesc := strings.ToLower(name), strings.ToLower(description)
	var score float64
	for _, term := range terms {
		switch {
		case strings.HasPrefix(lname, term):
			score += 1
		case strings.Contains(lname, term):
			score += 0.7
		case strings.Contains(ldesc, term):
			score += 0.3
		default:
			return 0
		}
	}
	return min(1, score/float64(len(terms)))
}

// likePattern wraps a term for the ilike PREFILTER.
//
// It deliberately does NOT escape the LIKE wildcards. The engine's ilike has
// no ESCAPE clause, so a backslash is matched literally: escaping "_" turned
// "%core\_api\_keys%" into a pattern that matches nothing, and since hugr
// names are underscore-separated identifiers that silently broke every name
// search. (It went unnoticed because the lexical path only runs without an
// embedder, and the tests that exercised it used queries with no underscores.)
//
// Leaving the wildcards in is safe because SQL only NARROWS here: "_" matching
// any single character and "%" matching anything can over-fetch, never
// under-fetch, and the Go scorer that follows does the real matching with
// plain substring tests. What keeps the over-fetch from CROWDING the window —
// a1b-style rows arriving for a_b until the true row no longer fits — is the
// tier structure in rankBySubstring: exact and prefix matches come through
// windows of their own.
func likePattern(term string) string {
	return "%" + term + "%"
}

// scanCatalogBatch is the ONE place the views' address lives: the GraphQL
// wrapper and the scan path move together or not at all. The previous move of
// these views had to chase the mount through every literal, and a missed one
// degrades into the lexical fallback that is indistinguishable from a missing
// embedder.
func scanCatalogBatch(ctx context.Context, q Querier, sig, body string, vars map[string]any, target any) error {
	gql := "query(" + sig + ") { core { catalog {\n" + body + "} } }"
	return scanAdmin(ctx, q, gql, vars, "core.catalog", target)
}

// scanAdmin runs a catalog query with FULL ACCESS and scans it. This is the
// privileged read; see the package note at the top of this file for why it is
// safe, and search.go for the filter that makes it so.
func scanAdmin(ctx context.Context, q Querier, gql string, vars map[string]any, path string, target any) error {
	res, err := q.Query(auth.ContextWithFullAccess(ctx), gql, vars)
	if err != nil {
		return err
	}
	// Check Err() BEFORE deferring Close: on an engine error the Response can
	// carry a half-built data tree whose Close panics.
	if rerr := res.Err(); rerr != nil {
		return rerr
	}
	defer res.Close()
	return res.ScanData(path, target)
}
