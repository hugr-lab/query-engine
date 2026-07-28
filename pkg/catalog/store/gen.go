package store

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// The generation layer produces COMPILED artifacts on the fly from the entity
// storage — the on-demand counterpart of the compiler's GENERATE phase. It is
// organised as six registries (design/034 requirements-onfly-compiler.md §3),
// one per extension axis, so each kind of change lands in exactly one place:
//
//   - new query/mutation kind      → queryShapes
//   - new root-field argument      → argDecorators (argProfiles hold the bases)
//   - new field on existing types  → fieldRules
//   - new derived type suffix      → derivedRules
//   - new shared system type       → sharedTypeRules
//   - new module-root contribution → rootRules
//
// The registries read ONLY store models through genContext, never SQL. That was
// once meant to allow moving this file next to the compiler's GENERATE rules;
// design-036 deleted those instead, so generation lives here for good — the
// discipline is still worth keeping, since it is what makes a rule testable
// without a database.

// genContext is the READ SESSION of one resolution request: the narrow
// surface the generation registries work against, with the request's reader
// memoization and the touched-source set the definition cache indexes the
// result under. Readers return errors Go-style — absent rows are nil results
// WITHOUT an error — and the whole generation chain propagates them up to the
// Provider surface (ForName / the logical model), which logs and serves empty
// because those interfaces have no error channel. A session runs on a single
// goroutine — the maps are unsynchronized by design.
type genContext struct {
	s *Store

	// derivedBase is the base entity name when THIS session resolved a derived
	// type (filter / mutation input / aggregation) — recorded by
	// resolveDerivedType from the matched rule, consumed by the description
	// finalisation to inherit the base's field descriptions. Only derived rules
	// set it, so the inheritance chain is structurally one level deep.
	derivedBase string

	// touched is the set of data sources whose ROWS the session read —
	// exactly the sources whose rewrite can change the produced definition
	// (row-level provenance; absence dependencies are closed on the write
	// side, see affectedByStored).
	touched map[string]struct{}

	// Per-request memoization (one ForName reads activeSources and the base
	// object rows many times across the generation rules). Only successful
	// reads are memoized; absent rows memoize as nil entries.
	sources     map[string]activeSource
	haveSources bool
	objects     map[string]*dataObject // nil value = known absent
	fieldRows   map[string][]*field
	relSrc      map[string][]*relation
	relDst      map[string][]*relationEdge
	nameRows    map[string][]string         // queryNames, by SQL text
	moduleInfos map[string][]*moduleInfoRow // queryModuleInfos, by SQL text
	dataSources map[string][]*dataSourceRow // readDataSources, by SQL text
	functions   map[string][]*function      // readFunctions, by module
	function    map[string]*function        // readFunction, by module\x1fname (nil = absent)
	typeRows    map[string]*typeRow         // readType, by name (nil = absent)
}

// typeRow memoizes a catalog.types row as its STORED text: readType parses a
// fresh AST per call because callers mutate the returned definition
// (attachCatalog, applyScalarFieldArguments).
type typeRow struct {
	definition string
	dataSource string
}

func (s *Store) genCtx() *genContext {
	return &genContext{s: s, touched: map[string]struct{}{}}
}

// touch records the data sources of rows a reader returned.
func (g *genContext) touch(sources ...string) {
	for _, src := range sources {
		if src != "" {
			g.touched[src] = struct{}{}
		}
	}
}

// sourceList returns the touched sources in deterministic order.
func (g *genContext) sourceList() []string { return sortedKeys(g.touched) }

// queryShape (§3.1) declares one per-object query/mutation kind: select,
// select_one (by pk / by unique), aggregate, bucket_agg, insert, update,
// delete. Consumed by the def-level @query/@mutation marker emission on the
// data object (reconstructDataObject) and by module-root synthesis (rootRules,
// Ш4.7 adds the root-field builders). New query kind = one entry.
type queryShape struct {
	kind string
	// rootKind routes the root field: query shapes land on Query/_module_*_query,
	// mutation shapes on Mutation/_module_*_mutation.
	rootKind sdl.ModuleObjectType
	// matches reports whether the object gets this shape (views take no
	// mutations, by-pk needs a PK on a non-m2m object, …).
	matches func(t *objectTraits) bool
	// markers emits the def-level @query/@mutation directives; queryName is
	// the AsModule-aware base (original name on module roots).
	markers func(t *objectTraits, queryName string) []*ast.Directive
	// root instantiates the shape's fields on the module root (most shapes
	// yield one; @unique yields one per constraint).
	root func(t *objectTraits) []*ast.FieldDefinition
}

var queryShapes = []queryShape{
	selectShape,
	selectOnePKShape,
	selectOneUniqueShape,
	aggregateShape,
	bucketAggShape,
	insertShape,
	updateShape,
	deleteShape,
}

// The argProfiles/argDecorators axis (§3.2) lives in gen_args.go, which used to
// be shared with the compiled path and is now this package's own:
// QueryArgsWithViewArgs (Root, view args prepended), SubQueryArgsWithViewArgs,
// AggRefArgs, AggSubRefArgs, JoinObject*/SpatialObject* profiles and the
// VectorSearchArgs decorator (similarity/semantic). A new argument = one
// decorator function there, appended by the shape/field/shared rule that owns
// the member.

// fieldRule (§3.3) contributes generated members to the reconstructed data
// object: nav fields from relations, subquery args on virtual fields, the
// `_join` field, extra scalar fields, `_distance_to_query`, aggregation twins.
// Rules run in registration order after the stored base fields are assembled.
// New field on existing types = one rule.
type fieldRule struct {
	name  string
	apply func(ctx context.Context, g *genContext, t *objectTraits, def *ast.Definition) error
}

var fieldRules = []fieldRule{
	scalarArgsFieldRule,
	cubeHypertableFieldRule,
	joinFieldsRule,
	navFieldsRule,
	extraFieldsRule,
	embeddingsFieldRule,
	sharedFieldsRule,
}

// derivedRule (§3.4) serves one derived-type suffix (class 5): filter,
// list_filter, mut_input_data, mut_data, aggregation, aggregation_bucket,
// sub_aggregation. New derived type = one rule.
type derivedRule struct {
	name string
	// match classifies a type name and extracts the candidate base-object
	// name. Suffixes overlap (X_list_filter also parses as X_list + _filter),
	// so registration order puts the more specific suffix first and build
	// returning nil (base is not a data object) falls through to later rules.
	match func(name string) (baseName string, ok bool)
	build func(ctx context.Context, g *genContext, baseName, name string) (*ast.Definition, error)
}

// derivedRules in match order — ONE place: a more specific suffix registers
// before its overlap (X_list_filter also parses as X_list + _filter).
var derivedRules = []derivedRule{
	aggregationRule,
	listFilterRule,
	filterRule,
	mutInputDataRule,
	mutDataRule,
}

// resolveDerivedType (5) generates a derived type from its base data object.
// The successful rule's base name is recorded on the session — the description
// finalisation inherits the base's field descriptions from it.
func resolveDerivedType(ctx context.Context, g *genContext, name string) (*ast.Definition, error) {
	for i := range derivedRules {
		r := &derivedRules[i]
		baseName, ok := r.match(name)
		if !ok {
			continue
		}
		def, err := r.build(ctx, g, baseName, name)
		if err != nil {
			return nil, err
		}
		if def != nil {
			g.derivedBase = baseName
			return def, nil
		}
	}
	return nil, nil
}

// sharedTypeRule (§3.5) synthesizes one per-schema shared system type from
// the ACTIVE data objects: _join, _join_aggregation, _spatial,
// _spatial_aggregation, _h3_data_query. New system type = one rule (+ a
// static stub when the planner needs the name to always exist).
type sharedTypeRule struct {
	name  string
	build func(ctx context.Context, g *genContext) (*ast.Definition, error)
}

var sharedTypeRules = []sharedTypeRule{
	{name: "_join", build: buildJoinShared},
	{name: "_join_aggregation", build: buildJoinAggShared},
	{name: "_spatial", build: buildSpatialShared},
	{name: "_spatial_aggregation", build: buildSpatialAggShared},
	{name: "_h3_data_query", build: buildH3DataShared},
}

// resolveSharedType (6) serves shared system types. It runs BEFORE the static
// layer: the prelude holds only stubs for these names, the real member fields
// come from the active objects.
func resolveSharedType(ctx context.Context, g *genContext, name string) (*ast.Definition, error) {
	for i := range sharedTypeRules {
		if sharedTypeRules[i].name != name {
			continue
		}
		return sharedTypeRules[i].build(ctx, g)
	}
	return nil, nil
}

// rootRule (§3.6) contributes members to a synthesized module root: data
// object query shapes (AsModule original-name rule), module functions (+
// aggregation twins), child-module gateway fields, root-only extras. Rules
// run in registration order over an empty root definition.
type rootRule struct {
	name  string
	apply func(ctx context.Context, g *genContext, sc *rootScope, def *ast.Definition) error
}

// moduleRootRef is one syntactic parse of a module-root type name. Module is
// the UNDERSCORED path as written in the name (`sales_reports`); mapping it
// back to the dotted module (`sales.reports` vs a literal `sales_reports`)
// needs the modules table and happens in the dispatcher.
type moduleRootRef struct {
	Module string // "" = top-level root (Query, Mutation, …)
	Kind   sdl.ModuleObjectType
}

// topLevelRoots maps the five top-level root names to their kinds.
var topLevelRoots = map[string]sdl.ModuleObjectType{
	base.QueryBaseName:            base.ModuleQuery,
	base.MutationBaseName:         base.ModuleMutation,
	base.FunctionTypeName:         base.ModuleFunction,
	base.FunctionMutationTypeName: base.ModuleMutationFunction,
	base.SubscriptionBaseName:     base.ModuleSubscription,
}

// moduleRootSuffixes in sdl.ModuleTypeName order of specificity:
// _mut_function must precede _function (suffix overlap).
var moduleRootSuffixes = []struct {
	suffix string
	kind   sdl.ModuleObjectType
}{
	{"_mut_function", base.ModuleMutationFunction},
	{"_function", base.ModuleFunction},
	{"_mutation", base.ModuleMutation},
	{"_subscription", base.ModuleSubscription},
	{"_query", base.ModuleQuery},
}

// classifyModuleRootName returns every syntactic parse of a module-root name
// (the inverse of sdl.ModuleTypeName). Underscores are ambiguous — a module
// named `x_mut` with a Function root and a module `x` with a MutationFunction
// root both produce `_module_x_mut_function` — so ALL candidates are returned
// and the dispatcher picks the one whose module actually exists.
func classifyModuleRootName(name string) []moduleRootRef {
	if kind, ok := topLevelRoots[name]; ok {
		return []moduleRootRef{{Kind: kind}}
	}
	rest, ok := strings.CutPrefix(name, "_module_")
	if !ok {
		return nil
	}
	var out []moduleRootRef
	for _, s := range moduleRootSuffixes {
		if m, found := strings.CutSuffix(rest, s.suffix); found && m != "" {
			out = append(out, moduleRootRef{Module: m, Kind: s.kind})
		}
	}
	return out
}
