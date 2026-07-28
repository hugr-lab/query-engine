package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

// catalog-search answers "what is relevant" — the rung an agent takes when it
// knows WHAT it wants but not what this deployment calls it.
//
// It is the ONE tool that reads privileged. Ranking happens over the
// annotation index, which lives in the catalog views, and a caller may hold no
// rights on those views at all — operators routinely hide the catalog from
// end-user roles. Refusing to rank for them would make MCP useless for exactly
// the roles it is meant to serve. So the index is read with full access and
// the raw result NEVER leaves this function: every candidate passes an
// explicit check against the caller's own rights first — re-resolved through
// the meta-query family, so the ENGINE decides and MCP keeps no copy of the
// access policy. Search cannot surface something catalog-describe would then
// refuse to show, because it is the same resolver answering both.
//
// Without vectors — no embedder, or the compiled catalog storage, whose views
// carry no index — it degrades to lexical matching over the SAME structural
// enumeration catalog-list uses, which runs in the caller's context and needs
// no filtering at all. Ranking quality drops; the ladder still works.

const (
	kindField = "field"

	// searchOverfetch is how many candidates are ranked per requested item.
	// The permission filter runs AFTER ranking, so the page has to be drawn
	// from a surplus; without one, a role that may see little would get an
	// empty page from a full index.
	searchOverfetch = 6
	// searchMaxCandidates bounds that surplus regardless of the requested page.
	searchMaxCandidates = 1000
)

// searchKinds is catalog-search's vocabulary: the catalog-list kinds plus
// field, which only search ranks over.
var searchKinds = append(append([]string{}, catalogKinds...), kindField)

// Field kinds. A data object's fields are not all columns, and the difference
// decides what an agent does with a hit: a relation hit is a PATH to another
// object, an extra field is computed, a column is a value.
const (
	fieldKindColumn   = "column"
	fieldKindRelation = "relation"
	fieldKindExtra    = "extra"
)

var fieldKinds = []string{fieldKindColumn, fieldKindRelation, fieldKindExtra}

// SearchHit is one ranked result.
type SearchHit struct {
	Kind        string  `json:"kind"        jsonschema_description:"module | data_source | data_object | function | field"`
	Name        string  `json:"name"`
	Module      string  `json:"module,omitempty"      jsonschema_description:"Owning module — REQUIRED to nest the GraphQL query"`
	DataSource  string  `json:"data_source,omitempty"`
	Description string  `json:"description,omitempty"`
	Score       float64 `json:"score"       jsonschema_description:"0-1, higher is better. Lexical fallback scores are coarse"`

	// Field hits only.
	Object    string `json:"object,omitempty"     jsonschema_description:"The data object this field belongs to"`
	FieldKind string `json:"field_kind,omitempty" jsonschema_description:"column | relation | extra"`
	HugrType  string `json:"hugr_type,omitempty"  jsonschema_description:"The field's GraphQL type"`
	RefObject string `json:"ref_object,omitempty" jsonschema_description:"For a relation field: the data object it navigates TO — selecting this field is how you get there"`

	// NextCall names the rung that turns this hit into something callable.
	NextCall string `json:"next_call,omitempty"`
}

// SearchResultPage is the search envelope. It carries no total: the filter
// runs after ranking, so an honest total would mean scanning the whole index
// for every query. Filtered reports what the permission pass removed from the
// candidates actually examined — enough to tell "nothing matches" from
// "nothing you may see matches".
type SearchResultPage struct {
	Items    []SearchHit `json:"items"`
	Limit    int         `json:"limit"`
	Offset   int         `json:"offset"`
	HasMore  bool        `json:"has_more"`
	Filtered int         `json:"filtered_out,omitempty" jsonschema_description:"Candidates dropped because the caller may not see them"`
	Lexical  bool        `json:"lexical,omitempty"      jsonschema_description:"true when ranking fell back to substring matching (no vector index): results are cruder, so prefer exact terms"`
	// LexicalReason says WHY the vector index was unusable. A silent fallback
	// is indistinguishable from a broken ranking query, and would stay broken
	// unnoticed; this makes the difference observable to operators and tests.
	LexicalReason string `json:"lexical_reason,omitempty"`
}

func (s *Server) catalogSearch(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := strings.TrimSpace(req.GetString("query", ""))
	if query == "" {
		return toolResultError("query is required — describe what you are looking for"), nil
	}
	kinds := req.GetStringSlice("kinds", nil)
	if len(kinds) == 0 {
		kinds = searchKinds
	}
	for _, k := range kinds {
		if !slices.Contains(searchKinds, k) {
			return toolResultError(fmt.Sprintf("unknown kind %q — valid: %s", k, strings.Join(searchKinds, ", "))), nil
		}
	}
	fKinds := req.GetStringSlice("field_kinds", nil)
	if len(fKinds) == 0 {
		fKinds = fieldKinds
	}
	module := req.GetString("module", "")
	limit, offset := pageArgsOf(req)
	minScore := req.GetFloat("min_score", 0)

	candidates := min(searchMaxCandidates, (limit+offset)*searchOverfetch)

	hits, reason, err := s.rankCandidates(ctx, query, kinds, module, candidates)
	if err != nil {
		return toolResultError(err.Error()), nil
	}

	kept, filtered, exhausted := s.filterHits(ctx, hits, fKinds, module, offset+limit)
	if minScore > 0 {
		kept = slices.DeleteFunc(kept, func(h SearchHit) bool { return h.Score < minScore })
	}

	start := min(offset, len(kept))
	end := min(start+limit, len(kept))
	page := kept[start:end]
	if page == nil {
		page = []SearchHit{}
	}
	return toolResultJSON(SearchResultPage{
		Items: page, Limit: limit, Offset: start,
		// Unverified candidates left over count as more: the next page may
		// still come back empty, which ends the walk honestly, whereas a false
		// "no more" ends it early and silently.
		HasMore:  end < len(kept) || !exhausted,
		Filtered: filtered,
		Lexical:  reason != "", LexicalReason: reason,
	}), nil
}

// rankCandidates produces the ordered candidate list, semantically when the
// index has vectors and lexically otherwise. The vector read runs with FULL
// ACCESS — see the package comment; nothing it returns reaches the client
// without passing filterHits.
// It returns the REASON the vector path was unusable ("" when it was used).
func (s *Server) rankCandidates(ctx context.Context, query string, kinds []string, module string, limit int) ([]SearchHit, string, error) {
	hits, err := s.rankByVector(ctx, query, kinds, module, limit)
	if err == nil {
		return hits, "", nil
	}
	slog.Debug("MCP catalog-search: vector ranking unavailable, falling back to lexical", "error", err)
	hits, lexErr := s.rankLexically(ctx, query, kinds, module, limit)
	return hits, err.Error(), lexErr
}

// vectorSource describes one kind's ranking query over the entity views.
type vectorSource struct {
	kind      string
	alias     string
	selection string
}

var vectorSources = map[string]vectorSource{
	kindModule:     {kindModule, "entity_modules", "name description"},
	kindDataSource: {kindDataSource, "entity_data_sources", "name description"},
	kindDataObject: {kindDataObject, "entity_data_objects", "name module data_source description"},
	kindFunction:   {kindFunction, "entity_functions", "name module data_source description"},
	kindField:      {kindField, "entity_fields", "type_name name data_source description"},
}

type vectorRow struct {
	Name       string  `json:"name"`
	TypeName   string  `json:"type_name"`
	Module     string  `json:"module"`
	DataSource string  `json:"data_source"`
	Desc       string  `json:"description"`
	Distance   float64 `json:"_distance_to_query"`
}

func (s *Server) rankByVector(ctx context.Context, query string, kinds []string, module string, limit int) ([]SearchHit, error) {
	var body strings.Builder
	ordered := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		src, ok := vectorSources[kind]
		if !ok {
			continue
		}
		ordered = append(ordered, kind)
		fmt.Fprintf(&body, `%s: %s(order_by: [{field: "_distance_to_query", direction: ASC}], limit: $limit) { %s _distance_to_query(query: $q) }`+"\n",
			kind, src.alias, src.selection)
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	gql := "query($q: String!, $limit: Int!) { core {\n" + body.String() + "} }"

	batch := map[string][]vectorRow{}
	if err := s.queryScanAdmin(ctx, gql, map[string]any{"q": query, "limit": limit}, "core", &batch); err != nil {
		return nil, err
	}

	var hits []SearchHit
	for _, kind := range ordered {
		for _, row := range batch[kind] {
			hit := SearchHit{
				Kind: kind, Name: row.Name, Module: row.Module,
				DataSource: row.DataSource, Description: row.Desc,
				Score: distanceToScore(row.Distance),
			}
			if kind == kindField {
				hit.Object = row.TypeName
			}
			hits = append(hits, hit)
		}
	}
	hits = slices.DeleteFunc(hits, func(h SearchHit) bool { return !inModule(h, module) })
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	return hits, nil
}

// rankLexically is the no-vector path: enumerate through the CALLER's context
// (so no filtering is owed) and score by where the term lands. Deliberately
// dumb — substring position and length, no term weighting — because a crude
// ranking that always works beats a good one that is sometimes absent.
func (s *Server) rankLexically(ctx context.Context, query string, kinds []string, module string, limit int) ([]SearchHit, error) {
	needle := strings.ToLower(query)
	terms := strings.Fields(needle)
	var hits []SearchHit

	// Fields have no structural enumeration that is not per object; without
	// vectors they are out of reach, and dropping them beats walking every
	// object in the deployment.
	structural := slices.DeleteFunc(slices.Clone(kinds), func(k string) bool { return k == kindField })
	// ONE walk for every kind asked for — the tree-backed kinds share it.
	items, _, err := s.catalogItemsOf(ctx, structural, module)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		score := lexicalScore(terms, it.Name, it.Description)
		if score == 0 {
			continue
		}
		hits = append(hits, SearchHit{
			Kind: it.Kind, Name: it.Name, Module: it.Module,
			DataSource: it.DataSource, Description: it.Description, Score: score,
		})
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}

// lexicalScore rewards a name match over a description match and an earlier
// match over a later one. Every term must appear somewhere, so a multi-word
// query narrows instead of widening.
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

// distanceToScore converts an embedding distance (0 = identical) into a 0-1
// score where higher is better, which is what a client can reason about.
func distanceToScore(d float64) float64 {
	if d < 0 {
		return 0
	}
	return 1 - d
}

// inModule scopes a hit to a module SUBTREE. "" is the root module, and every
// other module is a submodule of it, so scoping by "" is the whole tree —
// stated here rather than left to the caller's nil-check.
//
// A FIELD hit cannot be scoped here: the index has no module for a field
// (entity_fields carries none — a field belongs to whatever module owns its
// data object), so its Module is empty until enrichField reads it off the
// owner. Scoping it at this point would drop every field hit the moment a
// module is given. inModuleSubtree does the same test after enrichment.
func inModule(h SearchHit, module string) bool {
	if module == "" {
		return true
	}
	switch h.Kind {
	case kindModule:
		return h.Name == module || strings.HasPrefix(h.Name, module+".")
	case kindDataSource:
		return true // a source is not module-scoped; leave it to the caller
	case kindField:
		return true // deferred: the module comes from the owner object
	}
	return inModuleSubtree(h.Module, module)
}

// inModuleSubtree reports whether owner is module or one of its submodules.
func inModuleSubtree(owner, module string) bool {
	return owner == module || strings.HasPrefix(owner, module+".")
}

// filterHits is what makes the privileged read safe: every candidate is
// re-resolved through the meta-query family in the CALLER's context, and only
// what the engine itself serves back survives. The engine's resolver IS the
// filter — MCP keeps no copy of the access policy, so the two cannot drift.
//
// It is also where a field hit gets what only the schema knows: its type, its
// kind, and for a relation the object it navigates to.
//
// Candidates are verified in ranked order, a chunk at a time, and the walk
// stops as soon as the page is full — a role that may see everything pays for
// one chunk, not for the whole overfetched window.
//
// The count it returns is DENIALS only. Narrowing the caller asked for
// (field_kinds, module) drops hits too, but reporting those as filtered_out
// would read as "there is data here you may not see" — the opposite of what
// happened.
//
// It also reports whether the candidate list was EXHAUSTED. Stopping early
// leaves unverified candidates behind, and a page that happens to end exactly
// on the last verified hit would otherwise claim there is nothing more.
func (s *Server) filterHits(ctx context.Context, hits []SearchHit, fKinds []string, module string, need int) (
	kept []SearchHit, filtered int, exhausted bool,
) {
	kept = make([]SearchHit, 0, min(need, len(hits)))
	start := 0
	for ; start < len(hits) && len(kept) < need; start += verifyChunk {
		chunk := hits[start:min(start+verifyChunk, len(hits))]
		survived, denied, err := s.verifyChunk(ctx, chunk, fKinds, module)
		if err != nil {
			// A verification failure must not become an unfiltered answer. It
			// counts as filtered: the answer IS short, and silently pretending
			// otherwise is what the count exists to prevent.
			slog.Warn("MCP catalog-search: verification failed, dropping the chunk", "error", err)
			filtered += len(chunk)
			continue
		}
		filtered += denied
		kept = append(kept, survived...)
	}
	return kept, filtered, start >= len(hits)
}

// verifyChunk is the batch: one aliased meta query per chunk, in the caller's
// context. Anything the engine answers with null is invisible to this caller.
const verifyChunk = 40

type verifyModule struct {
	Name        string `json:"name"`
	DataObjects []struct {
		Name string `json:"name"`
	} `json:"dataObjects"`
	Functions []struct {
		Name string `json:"name"`
	} `json:"functions"`
	Modules []struct {
		Name string `json:"name"`
	} `json:"modules"`
}

type verifyObject struct {
	Name       string `json:"name"`
	ModuleName string `json:"moduleName"`
	Fields     []struct {
		Name       string      `json:"name"`
		McpExclude bool        `json:"mcp_exclude"`
		Type       *gqlTypeRef `json:"type"`
		// Args is selected for ONE reason: a field that takes arguments is
		// computed, and that is what tells an extra field from a column. The
		// classifier is shared with catalog-object_fields, so both surfaces
		// must feed it the same inputs.
		Args []struct {
			Name string `json:"name"`
		} `json:"args"`
	} `json:"fields"`
	Relations []struct {
		FieldName  string `json:"fieldName"`
		DataObject *struct {
			Name string `json:"name"`
		} `json:"dataObject"`
	} `json:"relations"`
}

// verdict is why a candidate did or did not make the page. Denied and
// narrowed are counted differently — see filterHits.
type verdict int

const (
	verdictKept verdict = iota
	// verdictDenied — the engine did not serve it to this caller.
	verdictDenied
	// verdictNarrowed — the caller's own field_kinds/module scoping removed it.
	verdictNarrowed
)

func (s *Server) verifyChunk(ctx context.Context, hits []SearchHit, fKinds []string, module string) ([]SearchHit, int, error) {
	var sig, body strings.Builder
	vars := map[string]any{}
	// Field hits are verified through their OWNER object, deduplicated: many
	// fields of one object cost one probe.
	owners := map[string]int{}
	arg := func(v string) string {
		i := len(vars)
		name := fmt.Sprintf("$v%d", i)
		vars[name[1:]] = v
		if sig.Len() > 0 {
			sig.WriteString(", ")
		}
		fmt.Fprintf(&sig, "%s: String!", name)
		return name
	}

	for i, hit := range hits {
		switch hit.Kind {
		case kindModule:
			fmt.Fprintf(&body, "h%d: _module(name: %s) { name dataObjects { name } functions { name } modules { name } }\n", i, arg(hit.Name))
		case kindDataSource:
			fmt.Fprintf(&body, "h%d: _dataSource(name: %s) { name }\n", i, arg(hit.Name))
		case kindDataObject:
			fmt.Fprintf(&body, "h%d: _dataObject(name: %s) { name moduleName }\n", i, arg(hit.Name))
		case kindFunction:
			fmt.Fprintf(&body, "h%d: _function(module: %s, name: %s) { name }\n", i, arg(hit.Module), arg(hit.Name))
		case kindField:
			if _, seen := owners[hit.Object]; seen {
				continue
			}
			owners[hit.Object] = i
			fmt.Fprintf(&body, "h%d: _dataObject(name: %s) { name moduleName fields { name mcp_exclude args { name } %s } relations { fieldName dataObject { name } } }\n",
				i, arg(hit.Object), typeRefSelection)
		}
	}
	if body.Len() == 0 {
		return nil, 0, nil
	}

	batch := map[string]json.RawMessage{}
	gql := "query(" + sig.String() + ") {\n" + body.String() + "}"
	if err := s.queryScan(ctx, gql, vars, "", &batch); err != nil {
		return nil, 0, err
	}

	objects := map[string]*verifyObject{}
	for name, i := range owners {
		raw, ok := batch[fmt.Sprintf("h%d", i)]
		if !ok || len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var obj verifyObject
		if err := json.Unmarshal(raw, &obj); err != nil {
			continue
		}
		objects[name] = &obj
	}

	out := make([]SearchHit, 0, len(hits))
	denied := 0
	for i, hit := range hits {
		if hit.Kind == kindField {
			enriched, v := enrichField(hit, objects[hit.Object], fKinds, module)
			switch v {
			case verdictDenied:
				denied++
			case verdictKept:
				out = append(out, enriched)
			}
			continue
		}
		raw, ok := batch[fmt.Sprintf("h%d", i)]
		if !ok || len(raw) == 0 || string(raw) == "null" {
			denied++
			continue
		}
		if hit.Kind == kindModule {
			var mod verifyModule
			if err := json.Unmarshal(raw, &mod); err != nil {
				denied++
				continue
			}
			// A direct module lookup resolves even when everything inside is
			// hidden — listings are what omit empty modules. Ask the engine
			// for the contents and treat "nothing visible" as invisible.
			if len(mod.DataObjects)+len(mod.Functions)+len(mod.Modules) == 0 {
				denied++
				continue
			}
		}
		out = append(out, withNextCall(hit))
	}
	return out, denied, nil
}

// enrichField turns a raw field hit into a readable one — or drops it when the
// owner is invisible, the field is not in the object the caller sees (which
// covers @exclude_mcp and field-level rules alike), or the caller's own
// narrowing removed it.
//
// This is also where a field hit finally gets a MODULE: the index has none,
// because a field belongs to whatever module owns its data object. So the
// module scope is applied here rather than at ranking time.
func enrichField(hit SearchHit, obj *verifyObject, fKinds []string, module string) (SearchHit, verdict) {
	if obj == nil {
		return hit, verdictDenied
	}
	var found bool
	var args int
	for _, f := range obj.Fields {
		if f.Name != hit.Name {
			continue
		}
		if f.McpExclude {
			return hit, verdictDenied
		}
		found = true
		hit.HugrType = f.Type.String()
		args = len(f.Args)
		break
	}
	if !found {
		return hit, verdictDenied
	}
	hit.Module = obj.ModuleName
	for _, r := range obj.Relations {
		if r.FieldName == hit.Name && r.DataObject != nil {
			hit.RefObject = r.DataObject.Name
			break
		}
	}
	hit.FieldKind = classifyFieldKind(hit.HugrType, args, hit.RefObject != "")

	if module != "" && !inModuleSubtree(hit.Module, module) {
		return hit, verdictNarrowed
	}
	if !slices.Contains(fKinds, hit.FieldKind) {
		return hit, verdictNarrowed
	}
	return withNextCall(hit), verdictKept
}

// classifyFieldKind is the ONE classifier both field surfaces use, so the same
// field never comes back as a column from search and an extra from
// catalog-object_fields.
//
// A declared relation is a PATH. Anything that takes ARGUMENTS is computed —
// a @function_call or @table_function_call_join field, an aggregation — and so
// is a list-typed field that is not a relation. Everything else is a stored
// value.
//
// Note for the entity storage: generated members (the _aggregation fields,
// _join) are not in catalog.fields and therefore never in the vector index, so
// search can only ever hit a stored column, a declared virtual field or a
// relation leg. The classifier still has to agree on all of them, because
// catalog-object_fields walks the COMPILED type, where the generated members
// are present.
func classifyFieldKind(hugrType string, args int, isRelation bool) string {
	switch {
	case isRelation:
		return fieldKindRelation
	case args > 0 || strings.HasPrefix(hugrType, "["):
		return fieldKindExtra
	}
	return fieldKindColumn
}

// withNextCall names the rung that turns a hit into something callable.
func withNextCall(hit SearchHit) SearchHit {
	switch hit.Kind {
	case kindModule:
		hit.NextCall = fmt.Sprintf("catalog-list(kind: data_object, module: %q)", hit.Name)
	case kindDataSource:
		hit.NextCall = fmt.Sprintf("catalog-describe(kind: data_source, names: [%q])", hit.Name)
	case kindDataObject:
		hit.NextCall = fmt.Sprintf("catalog-describe(kind: data_object, names: [%q])", hit.Name)
	case kindFunction:
		hit.NextCall = fmt.Sprintf("catalog-describe(kind: function, module: %q, names: [%q])", hit.Module, hit.Name)
	case kindField:
		if hit.RefObject != "" {
			hit.NextCall = fmt.Sprintf("catalog-describe(kind: data_object, names: [%q])", hit.RefObject)
		} else {
			hit.NextCall = fmt.Sprintf("catalog-object_fields(object: %q)", hit.Object)
		}
	}
	return hit
}
