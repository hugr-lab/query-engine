package mcp

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/mark3labs/mcp-go/mcp"
)

// catalog-search answers "what is relevant" — the rung an agent takes when it
// knows WHAT it wants but not what this deployment calls it.
//
// The mechanism now lives in the engine, behind the _search meta query, and
// this file is the adapter. That is a straight deletion of the hardest code
// MCP had: ranking used to read the annotation index with FULL ACCESS (the
// index lives in views a caller may hold no rights on) and then re-ask the
// engine, forty candidates at a time, which of them the caller was allowed to
// see. The engine does both halves in-process now, against the very predicates
// that decide what _catalog shows, so there is nothing left here to get wrong —
// and pkg/mcp no longer elevates anywhere.
//
// Three behaviours changed with the move, all improvements:
//   - field hits now appear in the lexical fallback too (the old structural
//     walk had no field enumeration that was not per object, so they vanished
//     whenever the embedder did);
//   - filtered_out is exact rather than chunk-rounded;
//   - one round trip instead of one plus ⌈candidates/40⌉.
//
// What stays MCP's business is the LADDER: next_call, naming the rung that
// turns a hit into something callable.

const kindField = "field"

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

// searchQuery is the whole ranking pipeline, as one call in the CALLER's
// context. includeMcpExcluded: false is how @exclude_mcp keeps being honoured
// without the engine adopting an AI-tooling policy as an access rule.
// match: MEANING is pinned, not defaulted. An agent describes the data it
// wants in its own words — that is a question for the semantic track — and the
// engine's default is BOTH, which serves a human who may be typing an
// identifier. Naming it here keeps this tool's behaviour where it was when the
// engine's default changes.
const searchQuery = `query($q: String!, $kinds: [_SearchKind!],
	$module: String, $limit: Int!, $offset: Int!, $minScore: Float) {
	_search(query: $q, kinds: $kinds, module: $module, match: MEANING,
		limit: $limit, offset: $offset, minScore: $minScore, includeMcpExcluded: false) {
		limit offset hasMore filteredOut lexical lexicalReason
		items {
			kind name moduleName dataSourceName description score
			objectName type hugrType refObjectName
		}
	}
}`

type metaSearchHit struct {
	Kind        string  `json:"kind"`
	Name        string  `json:"name"`
	Module      string  `json:"moduleName"`
	DataSource  string  `json:"dataSourceName"`
	Description string  `json:"description"`
	Score       float64 `json:"score"`
	Object      string  `json:"objectName"`
	GQLType     string  `json:"type"`
	HugrType    string  `json:"hugrType"`
	RefObject   string  `json:"refObjectName"`
}

type metaSearchPage struct {
	Items         []metaSearchHit `json:"items"`
	Limit         int             `json:"limit"`
	Offset        int             `json:"offset"`
	HasMore       bool            `json:"hasMore"`
	FilteredOut   int             `json:"filteredOut"`
	Lexical       bool            `json:"lexical"`
	LexicalReason string          `json:"lexicalReason"`
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
	for _, k := range fKinds {
		if !slices.Contains(fieldKinds, k) {
			return toolResultError(fmt.Sprintf("unknown field kind %q — valid: %s", k, strings.Join(fieldKinds, ", "))), nil
		}
	}
	limit, offset := pageArgsOf(req)

	var page metaSearchPage
	err := s.queryScan(ctx, searchQuery, map[string]any{
		"q":        query,
		"kinds":    enumsOf(kinds),
		"module":   req.GetString("module", ""),
		"limit":    limit,
		"offset":   offset,
		"minScore": req.GetFloat("min_score", 0),
	}, "_search", &page)
	if err != nil {
		return toolResultError(err.Error()), nil
	}

	// field_kinds narrows the PAGE, not the search. The engine does not filter
	// on it because the distinction is MCP's own (see mcpFieldKind), and the
	// index holds so few non-column field kinds that pushing the filter down
	// would buy nothing but a second vocabulary in the SDL.
	items := make([]SearchHit, 0, len(page.Items))
	for _, h := range page.Items {
		if h.Kind == "FIELD" && !slices.Contains(fKinds, mcpFieldKind(h.HugrType, h.RefObject)) {
			continue
		}
		items = append(items, withNextCall(SearchHit{
			Kind: strings.ToLower(h.Kind), Name: h.Name, Module: h.Module,
			DataSource: h.DataSource, Description: h.Description, Score: h.Score,
			Object: h.Object, FieldKind: mcpFieldKind(h.HugrType, h.RefObject),
			HugrType: h.GQLType, RefObject: h.RefObject,
		}))
	}
	return toolResultJSON(SearchResultPage{
		Items: items, Limit: page.Limit, Offset: page.Offset,
		HasMore: page.HasMore, Filtered: page.FilteredOut,
		Lexical: page.Lexical, LexicalReason: page.LexicalReason,
	}), nil
}

// mcpFieldKind maps hugr's field vocabulary onto the three-way one this tool
// publishes. The two are not the same question — hugr says what a field IS
// (column | calculated | function | select | …), MCP says what an agent should
// DO with it — so the mapping lives here rather than in the engine, and MCP's
// contract stays MCP's to keep.
//
// Note what the index can actually hold: relation navigation fields and
// @extra_field companions are generated when the GraphQL type is built, so a
// search hit is never either of them. A declared @join is the one path a hit
// can be, and it comes back as "select" with refObject set.
func mcpFieldKind(hugrType, refObject string) string {
	switch {
	case refObject != "":
		return fieldKindRelation
	case hugrType == "" || hugrType == string(base.HugrTypeFieldColumn):
		return fieldKindColumn
	}
	return fieldKindExtra
}

// enumsOf lifts MCP's lower-case vocabulary to the GraphQL enum. The two are
// the same words in different cases, and keeping MCP's spelling in the tool
// schema matters more than matching the engine's: it is a published contract.
func enumsOf(values []string) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = strings.ToUpper(v)
	}
	return out
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
