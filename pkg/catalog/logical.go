package catalog

import (
	"context"
	"iter"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// moduleKinds lists all module root kinds in a fixed iteration order.
var moduleKinds = []sdl.ModuleObjectType{
	sdl.ModuleQuery,
	sdl.ModuleMutation,
	sdl.ModuleFunction,
	sdl.ModuleMutationFunction,
	sdl.ModuleSubscription,
}

// ModuleInfo describes one node of the logical module tree.
type ModuleInfo struct {
	Name string // full dotted name; "" for the root module
	// Description is the CURATED text where a curation exists, the stored one
	// otherwise — the same overlay the entity views apply. LongDescription
	// exists only as curation and is empty when the storage has no curation
	// layer (the compiled provider).
	Description     string
	LongDescription string
	// RootTypes maps each PRESENT root kind to its type name
	// (sdl.ModuleTypeName). Key presence carries the information — a module
	// exists when at least one kind is present; the definition is resolved
	// LAZILY via Type(ctx, name) only when a consumer actually needs it (the
	// entity store synthesizes root types on demand — building five of them
	// per module lookup would defeat the on-the-fly model).
	RootTypes   map[sdl.ModuleObjectType]string
	DataSources []string // distinct data sources of direct members, sorted
}

// FunctionEntry describes one callable member of a module: a query function,
// a mutation function, or a subscription field.
type FunctionEntry struct {
	Field      *ast.FieldDefinition
	Kind       sdl.ModuleObjectType // ModuleFunction | ModuleMutationFunction | ModuleSubscription
	Module     string
	DataSource string
	IsTable    bool
	// LongDescription is the curated long form; empty without a curation
	// layer. The short description lives on Field, already overlaid.
	LongDescription string
}

// DataSourceInfo describes one data source as a LOGICAL entity: what it is
// and what it contributes, never how to connect to it (the registry's
// type/path are connection details — path is a DSN — and stay out of
// introspection).
//
// The three flags are pointers because not every catalog storage RECORDS
// them: the entity storage keeps them in data_source_meta, the walk adapter
// over a compiled schema cannot recover them and leaves them nil. Reporting
// nil ("this storage does not know") is the honest answer — claiming
// ReadOnly=false for a read-only source would invite a mutation that fails.
type DataSourceInfo struct {
	Name            string
	Engine          string // the source's engine type string (@catalog engine)
	Description     string
	LongDescription string
	ReadOnly        *bool
	AsModule        *bool
	IsExtension     *bool
	Modules         []string // modules this source contributes members to, sorted; "" = root
}

type RelationDirection string

const (
	RelationForward RelationDirection = "FORWARD"
	RelationBack    RelationDirection = "BACK"
)

type RelationKind string

const (
	RelationFK   RelationKind = "FK"
	RelationM2M  RelationKind = "M2M"
	RelationJoin RelationKind = "JOIN"
)

// RelationInfo is one perspective on a logical edge between two data objects.
// FK and M2M edges are visible from both ends (FORWARD from the declaring
// side, BACK from the referenced side); JOIN edges are one-directional.
type RelationInfo struct {
	Name            string
	Direction       RelationDirection
	Kind            RelationKind
	FieldName       string // the field ON the viewed object materializing the edge
	Description     string // per-endpoint description (same perspective as FieldName)
	DataObject      string // far data object type name (M2M: the far leg, not the junction)
	Through         string // M2M junction type name; "" otherwise
	SourceKeys      []string
	DestinationKeys []string
	DataSource      string // declaring data source
}

// LogicalModel is the narrow logical-model surface consumed by the _catalog
// introspection resolvers. It returns the UNFILTERED model — permission
// filtering is applied by consumers from the request context. The walk-based
// adapter below implements it over any Provider; the future entity-storage
// provider implements it natively.
type LogicalModel interface {
	// Module resolves a module by full dotted name ("" = root); nil when absent.
	Module(ctx context.Context, name string) *ModuleInfo
	// Modules iterates the DIRECT child modules of parent.
	Modules(ctx context.Context, parent string) iter.Seq[*ModuleInfo]
	// DataObject resolves a data object by GraphQL type name; nil for
	// non-data-object types.
	DataObject(ctx context.Context, name string) *sdl.Object
	// DataObjects iterates the data objects that are members of a module.
	DataObjects(ctx context.Context, module string) iter.Seq[*sdl.Object]
	// Function resolves a callable member of a module by field name, probing
	// the function, mutation-function and subscription roots in that order.
	Function(ctx context.Context, module, name string) *FunctionEntry
	// Functions iterates all callable members of a module (all three kinds).
	Functions(ctx context.Context, module string) iter.Seq[*FunctionEntry]
	// Relations iterates the logical edges of a data object, both directions.
	Relations(ctx context.Context, object string) iter.Seq[*RelationInfo]
	// DataSource resolves an ACTIVE data source by name; nil when absent.
	DataSource(ctx context.Context, name string) *DataSourceInfo
	// DataSources iterates the active data sources, ordered by name.
	DataSources(ctx context.Context) iter.Seq[*DataSourceInfo]
	// Type resolves a GraphQL type definition by name; nil when absent.
	// (Entity storage: the ForName resolution chain.)
	Type(ctx context.Context, name string) *ast.Definition
	// SourceTypes iterates the residual base types DEFINED BY SOURCES —
	// structural objects, inputs (incl. @args), enums, interfaces, unions
	// that are not data objects, module roots, or compiler-generated
	// helpers (filters, mutation inputs, aggregations). This is exactly
	// the content of the future entity-storage types table.
	SourceTypes(ctx context.Context) iter.Seq2[string, *ast.Definition]
	// SystemTypes iterates engine-defined types: scalars, the introspection
	// and hugr system SDL, scalar filter/aggregation inputs — the future
	// binary-owned static prelude. Compiler-DERIVED types (filters,
	// aggregations, mutation inputs, module roots) belong to neither set.
	SystemTypes(ctx context.Context) iter.Seq2[string, *ast.Definition]
}

// LogicalModelFromProvider adapts a compiled-schema Provider to the
// LogicalModel interface via top-down point lookups (module root types →
// fields → base definitions) — no full-type enumeration on any path.
// A provider that already implements LogicalModel natively (the future
// entity-storage catalog) is returned as-is, without the walk adapter.
func LogicalModelFromProvider(p Provider) LogicalModel {
	if lp, ok := p.(LogicalModel); ok {
		return lp
	}
	return &providerLogicalModel{p: p}
}

type providerLogicalModel struct {
	p Provider
}

var _ LogicalModel = (*providerLogicalModel)(nil)

func (m *providerLogicalModel) Module(ctx context.Context, name string) *ModuleInfo {
	// Probe the compiled root types per kind; expose only the NAMES — the
	// resolved definitions stay local (description fallback below).
	roots := map[sdl.ModuleObjectType]string{}
	var defs []*ast.Definition
	for _, kind := range moduleKinds {
		if def := m.p.ForName(ctx, sdl.ModuleTypeName(name, kind)); def != nil {
			roots[kind] = def.Name
			defs = append(defs, def)
		}
	}
	if len(roots) == 0 {
		return nil
	}
	info := &ModuleInfo{Name: name, RootTypes: roots}
	if name == "" {
		info.Description = m.p.Description(ctx)
	}
	if info.Description == "" {
		for _, def := range defs {
			if def.Description != "" {
				info.Description = def.Description
				break
			}
		}
	}
	info.DataSources = m.moduleDataSources(ctx, name)
	return info
}

func (m *providerLogicalModel) Modules(ctx context.Context, parent string) iter.Seq[*ModuleInfo] {
	return func(yield func(*ModuleInfo) bool) {
		seen := map[string]struct{}{}
		var children []string
		for _, kind := range moduleKinds {
			root := m.p.ForName(ctx, sdl.ModuleTypeName(parent, kind))
			if root == nil {
				continue
			}
			for _, f := range root.Fields {
				child := m.submoduleName(ctx, f)
				if child == "" {
					continue
				}
				if _, ok := seen[child]; ok {
					continue
				}
				seen[child] = struct{}{}
				children = append(children, child)
			}
		}
		// Deterministic listing order: the compiled root-type field order is
		// not stable across compilations (unsorted map iteration in the
		// assembler), so sort by name — matching the future entity-storage
		// ORDER BY contract.
		slices.Sort(children)
		for _, child := range children {
			info := m.Module(ctx, child)
			if info == nil {
				continue
			}
			if !yield(info) {
				return
			}
		}
	}
}

// submoduleName returns the full dotted module name when the field navigates
// to a submodule root type, "" otherwise.
func (m *providerLogicalModel) submoduleName(ctx context.Context, f *ast.FieldDefinition) string {
	if f.Type.NamedType == "" || strings.HasPrefix(f.Name, "__") {
		return ""
	}
	td := m.p.ForName(ctx, f.Type.NamedType)
	if td == nil {
		return ""
	}
	mi := sdl.ModuleRootInfo(td)
	if mi == nil {
		return ""
	}
	return mi.Name
}

func (m *providerLogicalModel) DataObject(ctx context.Context, name string) *sdl.Object {
	def := m.p.ForName(ctx, name)
	if def == nil || !sdl.IsDataObject(def) {
		return nil
	}
	return sdl.DataObjectInfo(def)
}

func (m *providerLogicalModel) DataObjects(ctx context.Context, module string) iter.Seq[*sdl.Object] {
	return func(yield func(*sdl.Object) bool) {
		root := m.p.ForName(ctx, sdl.ModuleTypeName(module, sdl.ModuleQuery))
		if root == nil {
			return
		}
		seen := map[string]struct{}{}
		var names []string
		for _, f := range root.Fields {
			if !sdl.IsSelectQueryDefinition(f) {
				continue
			}
			tn := f.Type.Name()
			if _, ok := seen[tn]; ok {
				continue
			}
			seen[tn] = struct{}{}
			names = append(names, tn)
		}
		// Deterministic listing order (see Modules).
		slices.Sort(names)
		for _, tn := range names {
			info := m.DataObject(ctx, tn)
			if info == nil {
				continue
			}
			if !yield(info) {
				return
			}
		}
	}
}

func (m *providerLogicalModel) Function(ctx context.Context, module, name string) *FunctionEntry {
	for entry := range m.Functions(ctx, module) {
		if entry.Field.Name == name {
			return entry
		}
	}
	return nil
}

func (m *providerLogicalModel) Functions(ctx context.Context, module string) iter.Seq[*FunctionEntry] {
	return func(yield func(*FunctionEntry) bool) {
		// Deterministic listing order (see Modules): fixed kind order, fields
		// sorted by name within each kind.
		yieldSorted := func(entries []*FunctionEntry) bool {
			slices.SortFunc(entries, func(a, b *FunctionEntry) int {
				return strings.Compare(a.Field.Name, b.Field.Name)
			})
			for _, entry := range entries {
				if !yield(entry) {
					return false
				}
			}
			return true
		}
		for _, kind := range []sdl.ModuleObjectType{sdl.ModuleFunction, sdl.ModuleMutationFunction} {
			root := m.p.ForName(ctx, sdl.ModuleTypeName(module, kind))
			if root == nil {
				continue
			}
			var entries []*FunctionEntry
			for _, f := range root.Fields {
				if !sdl.IsFunction(f) {
					continue
				}
				fi, err := sdl.FunctionInfo(f)
				if err != nil {
					continue
				}
				entries = append(entries, &FunctionEntry{
					Field:      f,
					Kind:       kind,
					Module:     module,
					DataSource: fi.Catalog,
					IsTable:    fi.ReturnsTable,
				})
			}
			if !yieldSorted(entries) {
				return
			}
		}
		root := m.p.ForName(ctx, sdl.ModuleTypeName(module, sdl.ModuleSubscription))
		if root == nil {
			return
		}
		var entries []*FunctionEntry
		for _, f := range root.Fields {
			if !isSubscriptionField(f) || m.submoduleName(ctx, f) != "" {
				continue
			}
			entries = append(entries, &FunctionEntry{
				Field:      f,
				Kind:       sdl.ModuleSubscription,
				Module:     module,
				DataSource: base.FieldDefCatalog(f),
				IsTable:    f.Type.NamedType == "",
			})
		}
		yieldSorted(entries)
	}
}

// isSubscriptionField filters out placeholder and system fields of a
// subscription root type (same skip set the module assembler uses).
func isSubscriptionField(f *ast.FieldDefinition) bool {
	if strings.HasPrefix(f.Name, "__") {
		return false
	}
	return f.Name != "_stub" && f.Name != "_placeholder" && f.Name != "query"
}

func (m *providerLogicalModel) moduleDataSources(ctx context.Context, module string) []string {
	seen := map[string]struct{}{}
	for o := range m.DataObjects(ctx, module) {
		if o.Catalog != "" {
			seen[o.Catalog] = struct{}{}
		}
	}
	for f := range m.Functions(ctx, module) {
		if f.DataSource != "" {
			seen[f.DataSource] = struct{}{}
		}
	}
	res := make([]string, 0, len(seen))
	for s := range seen {
		res = append(res, s)
	}
	slices.Sort(res)
	return res
}

func (m *providerLogicalModel) DataSource(ctx context.Context, name string) *DataSourceInfo {
	if name == "" {
		return nil
	}
	for ds := range m.DataSources(ctx) {
		if ds.Name == name {
			return ds
		}
	}
	return nil
}

// DataSources derives the source set from the module tree — in a compiled
// schema the @catalog directive on every generated member is the only record
// of provenance, so the tree walk IS the enumeration. Engine comes from the
// source's data objects; the load-state flags and the curated descriptions
// live in the catalog storage, not in the schema, and stay nil/empty here
// (see DataSourceInfo).
func (m *providerLogicalModel) DataSources(ctx context.Context) iter.Seq[*DataSourceInfo] {
	return func(yield func(*DataSourceInfo) bool) {
		acc := map[string]*DataSourceInfo{}
		note := func(source, engine, module string) {
			if source == "" {
				return
			}
			info, ok := acc[source]
			if !ok {
				info = &DataSourceInfo{Name: source}
				acc[source] = info
			}
			if info.Engine == "" {
				info.Engine = engine
			}
			if !slices.Contains(info.Modules, module) {
				info.Modules = append(info.Modules, module)
			}
		}
		var walk func(module string)
		walk = func(module string) {
			for o := range m.DataObjects(ctx, module) {
				note(o.Catalog, base.DefinitionCatalogEngine(o.Definition()), module)
			}
			for f := range m.Functions(ctx, module) {
				note(f.DataSource, "", module)
			}
			for child := range m.Modules(ctx, module) {
				walk(child.Name)
			}
		}
		walk("")

		names := make([]string, 0, len(acc))
		for name := range acc {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			info := acc[name]
			slices.Sort(info.Modules)
			if !yield(info) {
				return
			}
		}
	}
}

func (m *providerLogicalModel) Type(ctx context.Context, name string) *ast.Definition {
	return m.p.ForName(ctx, name)
}

func (m *providerLogicalModel) SourceTypes(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for name, def := range m.p.Types(ctx) {
			if !isSourceDefinedType(def) {
				continue
			}
			if !yield(name, def) {
				return
			}
		}
	}
}

func (m *providerLogicalModel) SystemTypes(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for name, def := range m.p.Types(ctx) {
			if !sdl.IsSystemType(def) {
				continue
			}
			if !yield(name, def) {
				return
			}
		}
	}
}

// isSourceDefinedType reports whether a definition is a residual base type
// contributed by a source — what the entity-storage types table will hold.
// Excluded: system types (scalars, __*, roots, @system incl. the scalar
// registry), data objects, module root types, and compiler-generated helpers
// (filter/mutation inputs, aggregation types).
func isSourceDefinedType(def *ast.Definition) bool {
	if sdl.IsSystemType(def) || sdl.IsDataObject(def) || sdl.ModuleRootInfo(def) != nil {
		return false
	}
	return def.Directives.ForName(base.FilterInputDirectiveName) == nil &&
		def.Directives.ForName(base.FilterListInputDirectiveName) == nil &&
		def.Directives.ForName(base.DataInputDirectiveName) == nil &&
		def.Directives.ForName(base.ObjectAggregationDirectiveName) == nil
}

// Relations derives the logical edges of a data object from the compiled
// schema:
//   - the object's own @references directives → FORWARD FK edges (and this
//     side's M2M leg: the compiler rewrites junction @field_references into
//     @references(is_m2m, m2m_name) on BOTH endpoints);
//   - generated @references_query fields that do NOT materialize an own
//     declaration → BACK projections of the far object's @references;
//   - @join fields targeting a data object → one-directional JOIN edges.
//
// Keys stay in the relation's canonical source→destination orientation
// regardless of direction (one logical edge, two projections). Join SQL is
// never surfaced.
func (m *providerLogicalModel) Relations(ctx context.Context, object string) iter.Seq[*RelationInfo] {
	return func(yield func(*RelationInfo) bool) {
		def := m.p.ForName(ctx, object)
		if def == nil || !sdl.IsDataObject(def) {
			return
		}
		ownerSource := sdl.CatalogName(def)
		// Keyed by the navigation FIELD the own declaration materializes, not
		// by the relation name: a relation name is unique per DECLARING object,
		// which is not the same thing as unique here. A self-reference declares
		// one relation and carries both its legs (parent / children), and two
		// different objects referencing this one through a like-named column
		// produce the same default relation name. Keying by name dropped the
		// back leg in both cases.
		ownFields := map[string]struct{}{}
		var rels []*RelationInfo

		for _, d := range def.Directives.ForNames(base.ReferencesDirectiveName) {
			ref := sdl.ReferencesInfo(d)
			if ref == nil {
				continue
			}
			ownField := ref.Query
			if ownField == "" {
				ownField = ref.ReferencesName
			}
			ownFields[ownField] = struct{}{}
			rel := &RelationInfo{
				Name:            ref.Name,
				Direction:       RelationForward,
				Kind:            RelationFK,
				FieldName:       ref.Query,
				Description:     ref.Description,
				DataObject:      ref.ReferencesName,
				SourceKeys:      ref.SourceFields(),
				DestinationKeys: ref.ReferencesFields(),
				DataSource:      ownerSource,
			}
			if ref.IsM2M {
				rel.Kind = RelationM2M
				rel.Through = ref.M2MName
			}
			// Extension-declared references attribute their navigation field.
			if rel.FieldName != "" {
				if f := def.Fields.ForName(rel.FieldName); f != nil {
					if c := base.FieldDefCatalog(f); c != "" {
						rel.DataSource = c
					}
				}
			}
			rels = append(rels, rel)
		}

		for _, f := range def.Fields {
			switch {
			case sdl.IsReferencesSubquery(f):
				name := base.FieldDefDirectiveArgString(f, base.FieldReferencesQueryDirectiveName, base.ArgName)
				if _, ok := ownFields[f.Name]; ok {
					// This field IS an own declaration's navigation field,
					// already emitted above as the forward leg.
					continue
				}
				// BACK projection: the declaring object is the field's element type.
				farName := f.Type.Name()
				far := m.p.ForName(ctx, farName)
				if far == nil {
					continue
				}
				var ref *sdl.References
				for _, d := range far.Directives.ForNames(base.ReferencesDirectiveName) {
					if ri := sdl.ReferencesInfo(d); ri != nil && ri.Name == name {
						ref = ri
						break
					}
				}
				if ref == nil {
					continue
				}
				rel := &RelationInfo{
					Name:            ref.Name,
					Direction:       RelationBack,
					Kind:            RelationFK,
					FieldName:       f.Name,
					Description:     ref.ReferencesDescription,
					DataObject:      farName,
					SourceKeys:      ref.SourceFields(),
					DestinationKeys: ref.ReferencesFields(),
					DataSource:      base.FieldDefCatalog(f),
				}
				if ref.IsM2M {
					rel.Kind = RelationM2M
					rel.Through = ref.M2MName
				}
				rels = append(rels, rel)

			case sdl.IsJoinSubqueryDefinition(f):
				d := f.Directives.ForName(base.JoinDirectiveName)
				target := base.DirectiveArgString(d, base.ArgReferencesName)
				targetDef := m.p.ForName(ctx, target)
				if targetDef == nil || !sdl.IsDataObject(targetDef) {
					continue
				}
				rel := &RelationInfo{
					Name:            f.Name,
					Direction:       RelationForward,
					Kind:            RelationJoin,
					FieldName:       f.Name,
					Description:     f.Description,
					DataObject:      target,
					SourceKeys:      base.DirectiveArgStrings(d, base.ArgSourceFields),
					DestinationKeys: base.DirectiveArgStrings(d, base.ArgReferencesFields),
					DataSource:      base.FieldDefCatalog(f),
				}
				rels = append(rels, rel)
			}
		}

		// Deterministic listing order (see Modules).
		slices.SortFunc(rels, func(a, b *RelationInfo) int {
			if c := strings.Compare(a.Name, b.Name); c != 0 {
				return c
			}
			if c := strings.Compare(string(a.Kind), string(b.Kind)); c != 0 {
				return c
			}
			if c := strings.Compare(string(a.Direction), string(b.Direction)); c != 0 {
				return c
			}
			return strings.Compare(a.FieldName, b.FieldName)
		})
		for _, rel := range rels {
			if !yield(rel) {
				return
			}
		}
	}
}
