package metadata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/base"
)

// Ranking is the half of _search that reads the INDEX, and it is the only half
// that reads privileged.
//
// The index lives in the core.entity_* views, which carry no row-level
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
// in entity_fields (a field belongs to whatever module owns its object) and is
// filled in later, from the owner.
type candidate struct {
	kind        string
	name        string
	object      string // field candidates: the owning data object
	module      string
	dataSource  string
	description string
	score       float64

	// Field candidates only, read straight off the index row. The writer
	// stored these properties; the reconstructed field definition is BUILT
	// from them, so classifying here rather than from the rebuilt AST costs
	// one relations query less per owner object and cannot disagree with it.
	gqlType   string
	hugrType  string
	refObject string
}

// entityView describes one kind's ranking query against the entity views.
type entityView struct {
	alias     string   // the view's field name under `core`
	selection []string // columns to read besides the ranking expression
	// nameCols are the text columns the lexical prefilter searches.
	nameCols []string
}

var entityViews = map[string]entityView{
	searchKindModule:     {"entity_modules", []string{"name", "description"}, []string{"name", "description"}},
	searchKindDataSource: {"entity_data_sources", []string{"name", "description"}, []string{"name", "description"}},
	searchKindDataObject: {"entity_data_objects", []string{"name", "module", "data_source", "description"}, []string{"name", "description"}},
	searchKindFunction:   {"entity_functions", []string{"name", "module", "data_source", "description"}, []string{"name", "description"}},
	// Fields read their stored PROPERTIES too: those say what the field is
	// (see fieldRole), so a hit needs neither the rebuilt definition nor a
	// relations query to be classified.
	searchKindField: {"entity_fields",
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

// rankCandidates produces the ordered candidate list: semantically when the
// index has vectors, lexically otherwise. It returns the REASON the vector
// path was unusable ("" when it was used).
//
// Both paths failing is an error, never an empty page: "nothing matched" and
// "the search is broken" must not look the same to a client.
func rankCandidates(ctx context.Context, q Querier, req searchRequest, limit int) ([]candidate, string, error) {
	if q == nil {
		return nil, "", errNoQuerier
	}
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
	gql := "query(" + sig + ") { core {\n" + body.String() + "} }"
	if err := scanAdmin(ctx, q, gql, vars, "core", &batch); err != nil {
		return nil, err
	}
	return collectCandidates(ordered, batch, func(row entityRow) float64 {
		// Distance 0 is identical; a score is the other way round so a client
		// can reason about "higher is better" without knowing the metric.
		if row.Distance < 0 {
			return 0
		}
		return 1 - row.Distance
	}), nil
}

// rankLexically is the no-embedder path. SQL only NARROWS — any term appearing
// in a name or a description makes a row a candidate — and the actual scoring
// happens in Go, where a multi-word query narrows instead of widening.
//
// Doing the narrowing in the view rather than by walking the module tree is
// what lets this path reach FIELDS. A structural walk has no enumeration of
// fields that is not per object, which is why MCP's fallback dropped field
// hits entirely; entity_fields is one table.
func rankLexically(ctx context.Context, q Querier, req searchRequest, limit int) ([]candidate, error) {
	terms := strings.Fields(strings.ToLower(req.query))
	if len(terms) == 0 {
		return nil, nil
	}
	vars := map[string]any{"limit": limit}
	patterns := make([]string, len(terms))
	for i, term := range terms {
		name := fmt.Sprintf("t%d", i)
		vars[name] = "%" + escapeLike(term) + "%"
		patterns[i] = "$" + name
	}
	sig := make([]string, 0, len(patterns)+2)
	sig = append(sig, "$limit: Int!")
	for _, p := range patterns {
		sig = append(sig, p+": String!")
	}

	var body strings.Builder
	ordered := make([]string, 0, len(req.kinds))
	for _, kind := range req.kinds {
		view, ok := entityViews[kind]
		if !ok {
			continue
		}
		ordered = append(ordered, kind)
		var terms []string
		for _, col := range view.nameCols {
			for _, p := range patterns {
				terms = append(terms, fmt.Sprintf("{%s: {ilike: %s}}", col, p))
			}
		}
		filter := fmt.Sprintf("{_or: [%s]", strings.Join(terms, ", "))
		if kind == searchKindField && objectFilterApplies(req) {
			filter += ", type_name: {eq: $obj}"
		}
		filter += "}"
		fmt.Fprintf(&body, "%s: %s(filter: %s, limit: $limit) { %s }\n",
			kind, view.alias, filter, strings.Join(view.selection, " "))
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	if objectFilterApplies(req) {
		vars["obj"] = req.object
		sig = append(sig, "$obj: String!")
	}

	batch := map[string][]entityRow{}
	gql := "query(" + strings.Join(sig, ", ") + ") { core {\n" + body.String() + "} }"
	if err := scanAdmin(ctx, q, gql, vars, "core", &batch); err != nil {
		return nil, err
	}
	hits := collectCandidates(ordered, batch, func(row entityRow) float64 {
		return lexicalScore(terms, row.Name, row.Desc)
	})
	// The prefilter accepts ANY term; the score demands all of them.
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

// collectCandidates flattens the per-kind batch into one list ordered by score.
func collectCandidates(ordered []string, batch map[string][]entityRow, score func(entityRow) float64) []candidate {
	var hits []candidate
	for _, kind := range ordered {
		for _, row := range batch[kind] {
			c := candidate{
				kind: kind, name: row.Name, module: row.Module,
				dataSource: row.DataSource, description: row.Desc,
				score: score(row),
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

// escapeLike neutralises the LIKE wildcards a user's words may contain, so a
// query for "a_b" does not silently match "axb".
func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
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
