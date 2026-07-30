package metadata

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// _search: rank the logical model, then show only what the caller may see.
//
// Ranking (search_rank.go) reads the index with full access. This file is the
// half that makes that safe: every candidate is resolved against the logical
// model and put through the SAME predicates the rest of this package uses to
// decide what _catalog shows — dataObjectVisible, functionVisible,
// moduleHasVisibleContent, dataSourceVisible, introspectionFieldVisible. There
// is no second copy of the access policy anywhere: search cannot surface
// something _dataObject would then refuse to show, because it is the same
// predicate answering both.
//
// The filter runs AFTER ranking, so the page is drawn from an overfetched
// window; without one, a role that may see little would get an empty page out
// of a full index.

const (
	searchKindModule     = "module"
	searchKindDataSource = "data_source"
	searchKindDataObject = "data_object"
	searchKindFunction   = "function"
	searchKindField      = "field"
)

var (
	searchKinds = []string{searchKindModule, searchKindDataSource, searchKindDataObject, searchKindFunction, searchKindField}

	// searchKindEnum maps the GraphQL enum to the index's vocabulary. The two
	// differ in case only, but going through a table keeps a typo in the SDL
	// from silently producing an empty search.
	searchKindEnum = map[string]string{
		"MODULE": searchKindModule, "DATA_SOURCE": searchKindDataSource,
		"DATA_OBJECT": searchKindDataObject, "FUNCTION": searchKindFunction,
		"FIELD": searchKindField,
	}
	searchKindGQL = map[string]string{
		searchKindModule: "MODULE", searchKindDataSource: "DATA_SOURCE",
		searchKindDataObject: "DATA_OBJECT", searchKindFunction: "FUNCTION",
		searchKindField: "FIELD",
	}
)

const (
	searchDefaultLimit = 50
	searchMaxLimit     = 200

	// searchOverfetch is how many candidates are ranked per requested item,
	// and searchMaxCandidates bounds that surplus regardless of the page. The
	// permission filter runs after ranking, so the page has to be drawn from a
	// surplus.
	searchOverfetch     = 6
	searchMaxCandidates = 1000
)

type searchRequest struct {
	query              string
	kinds              []string
	module             string
	object             string
	limit              int
	offset             int
	minScore           float64
	includeMcpExcluded bool
}

// hit is a candidate that survived the filter, carrying what the filter
// already had to resolve — so the response resolvers never look anything up
// twice.
type hit struct {
	// candidate already carries what the index knew, including a field's
	// gqlType / hugrType / refObject — the writer stored those properties, so
	// there is nothing to re-derive here.
	candidate

	moduleInfo *catalog.ModuleInfo
	object_    *sdl.Object
	function   *catalog.FunctionEntry
	source     *catalog.DataSourceInfo
	fieldDef   *ast.FieldDefinition
}

func processSearchQuery(ctx context.Context, provider catalog.Provider, querier Querier, field *ast.Field, maxDepth int, vars map[string]any) (any, error) {
	req, err := parseSearchArgs(field, vars)
	if err != nil {
		return nil, err
	}
	lm, err := catalog.LogicalModelFromProvider(provider)
	if err != nil {
		return nil, err
	}

	need := req.offset + req.limit
	candidates, lexicalReason, err := rankCandidates(ctx, querier, req, min(searchMaxCandidates, need*searchOverfetch))
	if err != nil {
		return nil, err
	}
	// minScore narrows the CANDIDATES, before anything is verified.
	//
	// The page comes out the same either way — candidates are score-ordered,
	// so the leading survivors are the qualifying ones — but two things do
	// not. Verifying a candidate costs a definition reconstruction, and there
	// is no reason to spend one on a hit the caller has already excluded. And
	// filteredOut would otherwise count denials among sub-threshold
	// candidates, reporting "there is data here you may not see" about rows
	// the caller's own bar removed — the narrowing-is-not-filtering rule the
	// verdicts exist to keep.
	if req.minScore > 0 {
		candidates = slices.DeleteFunc(candidates, func(c candidate) bool { return c.score < req.minScore })
	}

	kept, filtered, exhausted := filterCandidates(ctx, lm, provider, candidates, req, need)

	start := min(req.offset, len(kept))
	end := min(start+req.limit, len(kept))
	page := kept[start:end]

	return searchResultResolver(ctx, lm, provider, field.SelectionSet, maxDepth, searchResult{
		items:  page,
		limit:  req.limit,
		offset: start,
		// Unverified candidates left over count as more: the next page may
		// still come back empty, which ends a walk honestly, whereas a false
		// "no more" ends it early and silently.
		hasMore:       end < len(kept) || !exhausted,
		filteredOut:   filtered,
		lexical:       lexicalReason != "",
		lexicalReason: lexicalReason,
	})
}

// --- arguments ---

func parseSearchArgs(field *ast.Field, vars map[string]any) (searchRequest, error) {
	req := searchRequest{
		kinds:              searchKinds,
		limit:              searchDefaultLimit,
		includeMcpExcluded: true,
	}
	args := field.ArgumentMap(vars)

	q, _ := args["query"].(string)
	req.query = strings.TrimSpace(q)
	if req.query == "" {
		return req, fmt.Errorf("_search: query is required — describe what you are looking for")
	}

	if raw, ok := args["kinds"]; ok && raw != nil {
		kinds, err := enumList(raw, searchKindEnum, "kind")
		if err != nil {
			return req, err
		}
		if len(kinds) > 0 {
			req.kinds = kinds
		}
	}
	if v, ok := args["module"].(string); ok {
		req.module = v
	}
	if v, ok := args["object"].(string); ok {
		req.object = v
	}
	if v, ok := intArg(args["limit"]); ok {
		req.limit = v
	}
	if req.limit < 1 {
		req.limit = 1
	}
	if req.limit > searchMaxLimit {
		req.limit = searchMaxLimit
	}
	if v, ok := intArg(args["offset"]); ok && v > 0 {
		req.offset = v
	}
	if v, ok := args["minScore"].(float64); ok {
		req.minScore = v
	}
	if v, ok := args["includeMcpExcluded"].(bool); ok {
		req.includeMcpExcluded = v
	}
	return req, nil
}

func enumList(raw any, vocabulary map[string]string, what string) ([]string, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("_search: %ss must be a list", what)
	}
	out := make([]string, 0, len(list))
	for _, v := range list {
		name, _ := v.(string)
		mapped, ok := vocabulary[name]
		if !ok {
			return nil, fmt.Errorf("_search: unknown %s %q", what, name)
		}
		if !slices.Contains(out, mapped) {
			out = append(out, mapped)
		}
	}
	return out, nil
}

// intArg accepts the shapes a GraphQL Int arrives in: int64 from a literal,
// float64 from a JSON variable.
func intArg(v any) (int, bool) {
	switch n := v.(type) {
	case int64:
		return int(n), true
	case int:
		return n, true
	case float64:
		return int(n), true
	}
	return 0, false
}

// --- the permission filter ---

// verdict is why a candidate did or did not make the page.
type verdict int

const (
	verdictKept verdict = iota
	// verdictDenied — the caller may not see it.
	verdictDenied
	// verdictNarrowed — the caller's OWN scoping (module, object,
	// includeMcpExcluded) removed it. Counted separately: reporting narrowing
	// as filtering would read as "there is data here you may not see", the
	// opposite of what happened.
	verdictNarrowed
)

// filterCandidates verifies candidates in ranked order and stops as soon as
// the page is full, so a role that may see everything pays for the page rather
// than for the whole overfetched window. It reports whether the candidate list
// was EXHAUSTED: stopping early leaves candidates behind, and a page that ends
// exactly on the last verified hit would otherwise claim there is nothing more.
func filterCandidates(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider,
	candidates []candidate, req searchRequest, need int,
) (kept []hit, filtered int, exhausted bool) {
	kept = make([]hit, 0, min(need, len(candidates)))
	i := 0
	for ; i < len(candidates) && len(kept) < need; i++ {
		h, v := verifyCandidate(ctx, lm, provider, candidates[i], req)
		switch v {
		case verdictKept:
			kept = append(kept, h)
		case verdictDenied:
			filtered++
		}
	}
	return kept, filtered, i >= len(candidates)
}

func verifyCandidate(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider,
	c candidate, req searchRequest,
) (hit, verdict) {
	h := hit{candidate: c}
	switch c.kind {
	case searchKindModule:
		// A module is not module-scoped by its owner but by ITSELF: the
		// request's module scope selects a subtree, and a module belongs to
		// that subtree when it is the root of it or sits below.
		if req.module != "" && !inModuleSubtree(c.name, req.module) {
			return h, verdictNarrowed
		}
		info := lm.Module(ctx, c.name)
		if info == nil {
			return h, verdictDenied
		}
		// A direct lookup resolves even when everything inside is hidden;
		// listings are what omit empty modules. Treat "nothing visible" as
		// invisible, so search agrees with the tree.
		if !moduleHasVisibleContent(ctx, lm, provider, info) {
			return h, verdictDenied
		}
		h.moduleInfo = info
		h.description = info.Description
		return h, verdictKept

	case searchKindDataSource:
		// A source is not a member of a module — it CONTRIBUTES to several —
		// so a module scope does not narrow it.
		ds := lm.DataSource(ctx, c.name)
		if ds == nil || !dataSourceVisible(ctx, lm, ds) {
			return h, verdictDenied
		}
		h.source = ds
		h.description = ds.Description
		h.module = ""
		return h, verdictKept

	case searchKindDataObject:
		obj := lm.DataObject(ctx, c.name)
		if obj == nil || !dataObjectVisible(ctx, obj) {
			return h, verdictDenied
		}
		h.module = sdl.ObjectModule(obj.Definition())
		if req.module != "" && !inModuleSubtree(h.module, req.module) {
			return h, verdictNarrowed
		}
		h.object_ = obj
		h.dataSource = obj.Catalog
		h.description = obj.Definition().Description
		return h, verdictKept

	case searchKindFunction:
		entry := lm.Function(ctx, c.module, c.name)
		if entry == nil || !functionVisible(ctx, entry) {
			return h, verdictDenied
		}
		if req.module != "" && !inModuleSubtree(entry.Module, req.module) {
			return h, verdictNarrowed
		}
		h.function = entry
		h.module = entry.Module
		h.dataSource = entry.DataSource
		h.description = entry.Field.Description
		return h, verdictKept

	case searchKindField:
		return verifyField(ctx, lm, provider, h, req)
	}
	return h, verdictDenied
}

// verifyField resolves a field hit through its OWNER, which is what decides
// visibility: a hidden object hides its fields, and a field hidden in its own
// right is absent from the object the caller sees. What the field IS came off
// the index row already (fieldRole) — the owner is consulted for permissions,
// not for classification.
//
// This is also where a field hit finally gets a MODULE. The index has none —
// a field belongs to whatever module owns its data object — so the module
// scope cannot be applied at ranking time and is applied here instead.
func verifyField(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider,
	h hit, req searchRequest,
) (hit, verdict) {
	if req.object != "" && h.object != req.object {
		return h, verdictNarrowed
	}
	owner := lm.DataObject(ctx, h.object)
	if owner == nil || !dataObjectVisible(ctx, owner) {
		return h, verdictDenied
	}
	def := owner.Definition()
	fd := def.Fields.ForName(h.name)
	if fd == nil {
		return h, verdictDenied
	}
	if !introspectionFieldVisible(ctx, provider, def, fd) {
		return h, verdictDenied
	}
	if !req.includeMcpExcluded && fd.Directives.ForName(base.FieldExcludeMCPDirectiveName) != nil {
		return h, verdictNarrowed
	}

	h.module = sdl.ObjectModule(def)
	if req.module != "" && !inModuleSubtree(h.module, req.module) {
		return h, verdictNarrowed
	}
	h.fieldDef = fd
	h.description = fd.Description
	return h, verdictKept
}

// inModuleSubtree reports whether owner is module or one of its submodules.
// "" is the root module and every other module is below it, so scoping by ""
// is the whole tree — which is why the callers test req.module != "" first.
func inModuleSubtree(owner, module string) bool {
	return owner == module || strings.HasPrefix(owner, module+".")
}

// --- response resolvers ---

type searchResult struct {
	items         []hit
	limit         int
	offset        int
	hasMore       bool
	filteredOut   int
	lexical       bool
	lexicalReason string
}

func searchResultResolver(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider,
	ss ast.SelectionSet, maxDepth int, res searchResult,
) (map[string]any, error) {
	value := func(v any) fieldResolverFunc {
		return func(ctx context.Context, field *ast.Field, onType string) (any, error) { return v, nil }
	}
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"items": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			items := make([]map[string]any, 0, len(res.items))
			for i := range res.items {
				data, err := searchHitResolver(ctx, lm, provider, &res.items[i], field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				items = append(items, data)
			}
			return items, nil
		},
		"limit":         value(res.limit),
		"offset":        value(res.offset),
		"hasMore":       value(res.hasMore),
		"filteredOut":   value(res.filteredOut),
		"lexical":       value(res.lexical),
		"lexicalReason": nullableText(res.lexicalReason),
	}, "_SearchResult")
}

func searchHitResolver(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider,
	h *hit, ss ast.SelectionSet, maxDepth int,
) (map[string]any, error) {
	value := func(v any) fieldResolverFunc {
		return func(ctx context.Context, field *ast.Field, onType string) (any, error) { return v, nil }
	}
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename":     typeNameResolver,
		"kind":           value(searchKindGQL[h.kind]),
		"name":           value(h.name),
		"moduleName":     value(h.module),
		"dataSourceName": nullableText(h.dataSource),
		"description":    nullableText(h.description),
		"score":          value(h.score),
		"objectName":     nullableText(h.object),
		"type":           nullableText(h.gqlType),
		"hugrType":       nullableText(h.hugrType),
		"refObjectName":  nullableText(h.refObject),

		// Drill-down. Exactly one is non-nil, matching kind — a client pays
		// for detail only by selecting it, and the detail comes from the
		// ordinary _catalog resolvers, so it is filtered the same way.
		"module": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if h.moduleInfo == nil {
				return nil, nil
			}
			return moduleResolver(ctx, lm, provider, h.moduleInfo, field.SelectionSet, maxDepth)
		},
		"dataObject": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if h.object_ == nil {
				return nil, nil
			}
			return dataObjectResolver(ctx, lm, provider, h.object_, field.SelectionSet, maxDepth)
		},
		"function": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if h.function == nil {
				return nil, nil
			}
			return functionResolver(ctx, lm, provider, h.function, field.SelectionSet, maxDepth)
		},
		"dataSource": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if h.source == nil {
				return nil, nil
			}
			return dataSourceResolver(ctx, h.source, field.SelectionSet)
		},
		"field": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if h.fieldDef == nil {
				return nil, nil
			}
			var owner *ast.Definition
			if h.object_ != nil {
				owner = h.object_.Definition()
			}
			return fieldResolver(ctx, provider, owner, h.fieldDef, field.SelectionSet, maxDepth)
		},
	}, "_SearchHit")
}
