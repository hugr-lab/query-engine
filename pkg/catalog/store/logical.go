package store

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// The store implements catalog.LogicalModel NATIVELY from the catalog.* tables
// (no compiled-schema walk): modules from catalog.modules (activity via the
// module_data_sources closure), data objects / functions from their entity rows
// through the reconstruction layer, relations synthesized from the physical fk
// legs (see relations.go). Listing order follows the interface contract:
// ORDER BY name (functions: kind order, then name).
//
// The interface has no error channel — each surface method resolves through an
// error-returning session (genContext) and, on a read failure, logs it here
// and serves EMPTY (never a partial result).
var _ catalog.LogicalModel = (*Store)(nil)

// Module resolves a module by full dotted name ("" = root); nil when absent or
// backed by no active source. ONE query: the root kinds come from a flat
// bool_or aggregate over the module's CLOSURE rows (the hierarchy was
// flattened at save time — buildModuleClosure), with the mutation kinds gated
// by data_source_meta.read_only (all-read-only sources → no mutations and no
// mutation functions).
func (s *Store) Module(ctx context.Context, name string) *catalog.ModuleInfo {
	g := s.genCtx()
	rows, err := g.readModuleInfo(ctx, name)
	if err != nil {
		logReadErr("module "+name, err)
		return nil
	}
	if len(rows) == 0 {
		if name != "" {
			return nil
		}
		// The root module exists even in an empty catalog.
		return &catalog.ModuleInfo{Name: "", RootTypes: map[sdl.ModuleObjectType]string{
			sdl.ModuleQuery: sdl.ModuleTypeName("", sdl.ModuleQuery),
		}}
	}
	info, err := g.moduleInfo(ctx, rows[0])
	if err != nil {
		logReadErr("module "+name, err)
		return nil
	}
	if err := g.curateModules(ctx, []*catalog.ModuleInfo{info}); err != nil {
		logReadErr("module "+name, err)
	}
	return info
}

// Modules iterates the DIRECT child modules of parent — children, activity and
// root kinds all come from ONE grouped query.
func (s *Store) Modules(ctx context.Context, parent string) iter.Seq[*catalog.ModuleInfo] {
	return func(yield func(*catalog.ModuleInfo) bool) {
		g := s.genCtx()
		rows, err := g.readChildModuleInfos(ctx, parent)
		if err != nil {
			logReadErr("modules of "+parent, err)
			return
		}
		infos := make([]*catalog.ModuleInfo, 0, len(rows))
		for _, row := range rows {
			info, err := g.moduleInfo(ctx, row)
			if err != nil {
				logReadErr("modules of "+parent, err)
				return
			}
			infos = append(infos, info)
		}
		// One curation read for the whole listing, not one per module.
		if err := g.curateModules(ctx, infos); err != nil {
			logReadErr("modules of "+parent, err)
		}
		for _, info := range infos {
			if !yield(info) {
				return
			}
		}
	}
}

// curateModules applies the annotation overlay to a batch of modules in
// place. A module's SDL description is often absent (the source declares
// none), which is exactly why the curation is the description that matters —
// and why the logical-model surface must serve it rather than the raw row.
func (g *genContext) curateModules(ctx context.Context, infos []*catalog.ModuleInfo) error {
	if len(infos) == 0 {
		return nil
	}
	keys := make([]string, 0, len(infos))
	for _, info := range infos {
		keys = append(keys, info.Name)
	}
	rows, err := g.readCurations(ctx, kindModule, keys)
	if err != nil {
		return err
	}
	for _, info := range infos {
		c, ok := rows[info.Name]
		if !ok {
			continue
		}
		if c.description != "" {
			info.Description = c.description
		}
		info.LongDescription = c.longDescription
	}
	return nil
}

// moduleInfoRow is one aggregated module row: identity + the subtree kind
// flags (mutation kinds already read_only-gated by the query).
type moduleInfoRow struct {
	Name             string
	Description      string
	HasDataObjects   bool
	HasMutations     bool
	HasFunctions     bool
	HasMutFunctions  bool
	HasSubscriptions bool
}

func (g *genContext) moduleInfo(ctx context.Context, row *moduleInfoRow) (*catalog.ModuleInfo, error) {
	roots := map[sdl.ModuleObjectType]string{}
	set := func(kind sdl.ModuleObjectType, present bool) {
		if present {
			roots[kind] = sdl.ModuleTypeName(row.Name, kind)
		}
	}
	set(sdl.ModuleQuery, row.HasDataObjects || row.Name == "")
	set(sdl.ModuleMutation, row.HasMutations)
	set(sdl.ModuleFunction, row.HasFunctions)
	set(sdl.ModuleMutationFunction, row.HasMutFunctions)
	set(sdl.ModuleSubscription, row.HasSubscriptions)
	sources, err := g.moduleMemberSources(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	return &catalog.ModuleInfo{
		Name:        row.Name,
		Description: row.Description,
		RootTypes:   roots,
		DataSources: sources,
	}, nil
}

// moduleKindFlags is the flat kind aggregate over closure rows: the hierarchy
// was flattened at save time, so no recursion here; disabling a source drops
// exactly its rows, and read_only gates the mutation kinds per source.
const moduleKindFlags = `bool_or(md.has_data_objects),
	bool_or(md.has_tables AND NOT ms.read_only),
	bool_or(md.has_functions),
	bool_or(md.has_mut_functions AND NOT ms.read_only),
	bool_or(md.has_subscriptions)`

// readModuleInfo aggregates ONE module row with its subtree kind flags ("" =
// the root, aggregated over the root closure rows). A module none of whose
// closure sources is active yields no row — the activity gate is the join.
func (g *genContext) readModuleInfo(ctx context.Context, name string) ([]*moduleInfoRow, error) {
	query := `SELECT mm.name, mm.description, ` + moduleKindFlags + `
		FROM core.catalog.modules mm
		JOIN core.catalog.module_data_sources md ON md.module = mm.name` +
		activeMeta("ms", "md.data_source") + `
		WHERE mm.name = ` + lit(name) + `
		GROUP BY mm.name, mm.description`
	if name == "" {
		// The root module has no modules row — aggregate its closure rows.
		query = `SELECT '', NULL, ` + moduleKindFlags + `
			FROM core.catalog.module_data_sources md` + activeMeta("ms", "md.data_source") + `
			WHERE md.module = ''`
	}
	return g.queryModuleInfos(ctx, query)
}

// readChildModuleInfos lists the DIRECT children of parent with their kind
// flags — children, activity and kinds in one grouped query.
func (g *genContext) readChildModuleInfos(ctx context.Context, parent string) ([]*moduleInfoRow, error) {
	cond := `mm.parent = ` + lit(parent)
	if parent == "" {
		cond = `mm.parent IS NULL`
	}
	return g.queryModuleInfos(ctx, `SELECT mm.name, mm.description, `+moduleKindFlags+`
		FROM core.catalog.modules mm
		JOIN core.catalog.module_data_sources md ON md.module = mm.name`+
		activeMeta("ms", "md.data_source")+`
		WHERE `+cond+`
		GROUP BY mm.name, mm.description
		ORDER BY mm.name`)
}

// queryModuleInfos runs one aggregated module query, memoized per session by
// the SQL text.
func (g *genContext) queryModuleInfos(ctx context.Context, query string) ([]*moduleInfoRow, error) {
	if out, ok := g.moduleInfos[query]; ok {
		return out, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read modules: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read modules: %w", err)
	}
	defer rows.Close()
	var out []*moduleInfoRow
	for rows.Next() {
		var r moduleInfoRow
		var desc sql.NullString
		var q, mut, fn, mfn, sub sql.NullBool
		if err := rows.Scan(&r.Name, &desc, &q, &mut, &fn, &mfn, &sub); err != nil {
			return nil, fmt.Errorf("read modules: %w", err)
		}
		if !q.Valid && r.Name == "" {
			continue // empty root aggregate — no active rows at all
		}
		r.Description = desc.String
		r.HasDataObjects = q.Bool
		r.HasMutations = mut.Bool
		r.HasFunctions = fn.Bool
		r.HasMutFunctions = mfn.Bool
		r.HasSubscriptions = sub.Bool
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read modules: %w", err)
	}
	if g.moduleInfos == nil {
		g.moduleInfos = map[string][]*moduleInfoRow{}
	}
	g.moduleInfos[query] = out
	return out, nil
}

// DataObject resolves a data object by type name (activity-gated), nil for
// anything else.
// The definition comes through ForName, NOT the bare reconstruction: ForName
// is where the description overlay is applied (and the result cached), so the
// logical model serves the curated text the entity views serve.
func (s *Store) DataObject(ctx context.Context, name string) *sdl.Object {
	def := s.ForName(ctx, name)
	if def == nil || !sdl.IsDataObject(def) {
		return nil
	}
	obj := sdl.DataObjectInfo(def)
	if obj == nil {
		return nil
	}
	if err := s.genCtx().curateDataObjects(ctx, []*sdl.Object{obj}); err != nil {
		logReadErr("data object "+name, err)
	}
	return obj
}

// DataObjects iterates the data objects that are members of a module.
func (s *Store) DataObjects(ctx context.Context, module string) iter.Seq[*sdl.Object] {
	return func(yield func(*sdl.Object) bool) {
		g := s.genCtx()
		names, err := g.dataObjectNames(ctx, module)
		if err != nil {
			logReadErr("data objects of "+module, err)
			return
		}
		objects := make([]*sdl.Object, 0, len(names))
		for _, name := range names {
			def := s.ForName(ctx, name)
			if def == nil || !sdl.IsDataObject(def) {
				continue
			}
			if obj := sdl.DataObjectInfo(def); obj != nil {
				objects = append(objects, obj)
			}
		}
		if err := g.curateDataObjects(ctx, objects); err != nil {
			logReadErr("data objects of "+module, err)
		}
		for _, obj := range objects {
			if !yield(obj) {
				return
			}
		}
	}
}

// curateDataObjects fills the LONG descriptions of a batch of objects (the
// short ones already came overlaid through ForName) with one read.
func (g *genContext) curateDataObjects(ctx context.Context, objects []*sdl.Object) error {
	if len(objects) == 0 {
		return nil
	}
	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		keys = append(keys, obj.Definition().Name)
	}
	rows, err := g.readCurations(ctx, kindDataObject, keys)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if c, ok := rows[obj.Definition().Name]; ok {
			obj.LongDescription = c.longDescription
		}
	}
	return nil
}

// Function resolves a callable member of a module by name ((module, name) is
// the storage primary key — no root probing needed).
func (s *Store) Function(ctx context.Context, module, name string) *catalog.FunctionEntry {
	row, err := s.genCtx().readFunction(ctx, module, name)
	if err != nil {
		logReadErr("function "+module+"."+name, err)
		return nil
	}
	if row == nil {
		return nil
	}
	entry := functionEntry(row)
	if err := s.genCtx().curateFunctions(ctx, []*catalog.FunctionEntry{entry}); err != nil {
		logReadErr("function "+module+"."+name, err)
	}
	return entry
}

// Functions iterates all callable members of a module: function, mutation and
// subscription kinds in that order, by name within each kind.
func (s *Store) Functions(ctx context.Context, module string) iter.Seq[*catalog.FunctionEntry] {
	return func(yield func(*catalog.FunctionEntry) bool) {
		memoized, err := s.genCtx().readFunctions(ctx, module)
		if err != nil {
			logReadErr("functions of "+module, err)
			return
		}
		// Sort a copy — the memoized slice's ORDER BY name order is a reader
		// contract other consumers rely on.
		rows := slices.Clone(memoized)
		slices.SortFunc(rows, func(a, b *function) int {
			if c := functionKindRank(a.Kind) - functionKindRank(b.Kind); c != 0 {
				return c
			}
			return strings.Compare(a.Name, b.Name)
		})
		entries := make([]*catalog.FunctionEntry, 0, len(rows))
		for _, row := range rows {
			entries = append(entries, functionEntry(row))
		}
		if err := s.genCtx().curateFunctions(ctx, entries); err != nil {
			logReadErr("functions of "+module, err)
		}
		for _, entry := range entries {
			if !yield(entry) {
				return
			}
		}
	}
}

// curateFunctions applies the annotation overlay to a batch of callables in
// place, one read PER KIND: a function's kind is part of its annotation
// identity, because the same name may exist as a query function, a mutation
// and a subscription with three different descriptions.
func (g *genContext) curateFunctions(ctx context.Context, entries []*catalog.FunctionEntry) error {
	byKind := map[string][]*catalog.FunctionEntry{}
	for _, entry := range entries {
		kind, _ := functionAnnotationKind(functionKindName(entry.Kind))
		byKind[kind] = append(byKind[kind], entry)
	}
	for kind, group := range byKind {
		keys := make([]string, 0, len(group))
		for _, entry := range group {
			keys = append(keys, functionKey(entry.Module, entry.Field.Name))
		}
		rows, err := g.readCurations(ctx, kind, keys)
		if err != nil {
			return err
		}
		for _, entry := range group {
			c, ok := rows[functionKey(entry.Module, entry.Field.Name)]
			if !ok {
				continue
			}
			if c.description != "" {
				entry.Field.Description = c.description
			}
			entry.LongDescription = c.longDescription
		}
	}
	return nil
}

// functionKindName maps a root kind back to the stored kind string — the
// inverse of functionKindType, and the value that joins the annotation identity.
func functionKindName(kind sdl.ModuleObjectType) string {
	switch kind {
	case sdl.ModuleMutationFunction:
		return kindMutationFunction
	case sdl.ModuleSubscription:
		return kindSubscription
	default:
		return kindFunction
	}
}

func functionEntry(row *function) *catalog.FunctionEntry {
	return &catalog.FunctionEntry{
		Field:      functionField(row),
		Kind:       functionKindType(row.Kind),
		Module:     row.Module,
		DataSource: row.DataSource,
		IsTable:    row.IsTable,
	}
}

func functionKindType(kind string) sdl.ModuleObjectType {
	switch kind {
	case "mutation":
		return sdl.ModuleMutationFunction
	case "subscription":
		return sdl.ModuleSubscription
	default:
		return sdl.ModuleFunction
	}
}

func functionKindRank(kind string) int {
	switch kind {
	case "mutation":
		return 1
	case "subscription":
		return 2
	default:
		return 0
	}
}

// Relations iterates the logical edges of a data object (see
// genContext.relations for the synthesis rules).
func (s *Store) Relations(ctx context.Context, object string) iter.Seq[*catalog.RelationInfo] {
	return func(yield func(*catalog.RelationInfo) bool) {
		rels, err := s.genCtx().relations(ctx, object)
		if err != nil {
			logReadErr("relations of "+object, err)
			return
		}
		for _, rel := range rels {
			if !yield(rel) {
				return
			}
		}
	}
}

// relations synthesizes the logical edges of a data object from the physical
// legs (verified against the live etalon):
//   - own legs → FK FORWARD (a junction shows its two legs this way);
//   - a leg from an is_m2m junction → M2M FORWARD to each co-endpoint
//     (through = junction, keys reversed into endpoint→junction orientation,
//     field/description from the leg's destination side) — and no FK BACK;
//   - any other incoming leg → FK BACK (keys stay canonical);
//   - own declared @join fields targeting a data object → JOIN FORWARD.
func (g *genContext) relations(ctx context.Context, object string) ([]*catalog.RelationInfo, error) {
	var rels []*catalog.RelationInfo

	own, err := g.relationsBySource(ctx, object)
	if err != nil {
		return nil, err
	}
	for _, r := range own {
		rels = append(rels, &catalog.RelationInfo{
			Name:            r.Name,
			Direction:       catalog.RelationForward,
			Kind:            catalog.RelationFK,
			FieldName:       r.SourceField,
			Description:     r.SourceFieldDescription,
			DataObject:      r.Destination,
			SourceKeys:      r.SourceKeys,
			DestinationKeys: r.DestinationKeys,
			DataSource:      r.DataSource,
		})
	}

	incoming, err := g.relationsByDestination(ctx, object)
	if err != nil {
		return nil, err
	}
	for _, r := range incoming {
		if r.m2mJunction {
			cos, err := g.relationsBySource(ctx, r.Source)
			if err != nil {
				return nil, err
			}
			for _, co := range cos {
				if co.Name == r.Name {
					continue
				}
				rels = append(rels, &catalog.RelationInfo{
					Name:            r.Name,
					Direction:       catalog.RelationForward,
					Kind:            catalog.RelationM2M,
					FieldName:       orDefault(r.DestinationField, r.Source),
					Description:     r.DestinationFieldDescription,
					DataObject:      co.Destination,
					Through:         r.Source,
					SourceKeys:      r.DestinationKeys,
					DestinationKeys: r.SourceKeys,
					DataSource:      r.DataSource,
				})
			}
			continue
		}
		// An explicitly EMPTY references_query means the DEFAULT (the
		// declaring object name) — the compiler never distinguishes.
		rels = append(rels, &catalog.RelationInfo{
			Name:            r.Name,
			Direction:       catalog.RelationBack,
			Kind:            catalog.RelationFK,
			FieldName:       orDefault(r.DestinationField, r.Source),
			Description:     r.DestinationFieldDescription,
			DataObject:      r.Source,
			SourceKeys:      r.SourceKeys,
			DestinationKeys: r.DestinationKeys,
			DataSource:      r.DataSource,
		})
	}

	fields, err := g.readFields(ctx, object)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		j := f.Properties.Join
		if j == nil {
			continue
		}
		if exists, err := g.dataObjectExists(ctx, j.ReferencesName); err != nil {
			return nil, err
		} else if !exists {
			continue
		}
		rels = append(rels, &catalog.RelationInfo{
			Name:            f.Name,
			Direction:       catalog.RelationForward,
			Kind:            catalog.RelationJoin,
			FieldName:       f.Name,
			Description:     f.Description,
			DataObject:      j.ReferencesName,
			SourceKeys:      j.SourceFields,
			DestinationKeys: j.ReferencesFields,
			DataSource:      f.DataSource,
		})
	}

	slices.SortFunc(rels, func(a, b *catalog.RelationInfo) int {
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
	return rels, nil
}

// Type resolves a type definition by name — the ForName resolver chain.
func (s *Store) Type(ctx context.Context, name string) *ast.Definition {
	return s.ForName(ctx, name)
}

// SourceTypes iterates the residual source-defined base types — exactly the
// content of catalog.types (active sources, by name), with @catalog
// re-attached the same way ForName serves them.
func (s *Store) SourceTypes(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		g := s.genCtx()
		rows, err := g.readSourceTypes(ctx)
		if err != nil {
			logReadErr("source types", err)
			return
		}
		if len(rows) == 0 {
			return
		}
		engines, err := g.activeEngines(ctx)
		if err != nil {
			logReadErr("source types", err)
			return
		}
		for _, row := range rows {
			def := attachCatalog(parseStoredDefinition(row.Name, row.Definition), row.DataSource, engines)
			if def == nil {
				continue
			}
			if !yield(row.Name, def) {
				return
			}
		}
	}
}

// SystemTypes iterates the binary-owned static prelude.
func (s *Store) SystemTypes(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return s.static.Types(ctx)
}

// DataSource resolves ONE active data source; nil when absent or inactive.
func (s *Store) DataSource(ctx context.Context, name string) *catalog.DataSourceInfo {
	if name == "" {
		return nil
	}
	g := s.genCtx()
	rows, err := g.readDataSources(ctx, name)
	if err != nil || len(rows) == 0 {
		if err != nil {
			logReadErr("data source "+name, err)
		}
		return nil
	}
	info, err := g.dataSourceInfo(ctx, rows[0])
	if err != nil {
		logReadErr("data source "+name, err)
		return nil
	}
	return info
}

// DataSources iterates the active data sources, ordered by name.
func (s *Store) DataSources(ctx context.Context) iter.Seq[*catalog.DataSourceInfo] {
	return func(yield func(*catalog.DataSourceInfo) bool) {
		g := s.genCtx()
		rows, err := g.readDataSources(ctx, "")
		if err != nil {
			logReadErr("data sources", err)
			return
		}
		for _, row := range rows {
			info, err := g.dataSourceInfo(ctx, row)
			if err != nil {
				logReadErr("data sources", err)
				return
			}
			if !yield(info) {
				return
			}
		}
	}
}

// dataSourceRow is one active data_source_meta row with its curation overlay.
type dataSourceRow struct {
	Name            string
	Engine          string
	ReadOnly        bool
	AsModule        bool
	IsExtension     bool
	Description     string
	LongDescription string
}

func (g *genContext) dataSourceInfo(ctx context.Context, row *dataSourceRow) (*catalog.DataSourceInfo, error) {
	modules, err := g.dataSourceModules(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	return &catalog.DataSourceInfo{
		Name:            row.Name,
		Engine:          row.Engine,
		Description:     row.Description,
		LongDescription: row.LongDescription,
		ReadOnly:        row.ReadOnly,
		AsModule:        row.AsModule,
		IsExtension:     row.IsExtension,
		Modules:         modules,
	}, nil
}

// readDataSources reads the active sources with the data_source curation
// overlay applied — the same coalesce the entity_data_sources view does, so
// introspection and the view never disagree. name == "" reads all.
func (g *genContext) readDataSources(ctx context.Context, name string) ([]*dataSourceRow, error) {
	where := `WHERE m.loaded AND NOT m.disabled AND NOT m.suspended`
	if name != "" {
		where += ` AND m.data_source = ` + lit(name)
	}
	query := `SELECT m.data_source, m.engine, m.read_only, m.as_module, m.is_extension,
			coalesce(a.description, ''), coalesce(a.long_description, '')
		FROM core.catalog.data_source_meta m
		LEFT JOIN core.catalog.annotations a
			ON a.entity_kind = ` + lit(kindDataSource) + ` AND a.entity_key = m.data_source
		` + where + `
		ORDER BY m.data_source`
	if out, ok := g.dataSources[query]; ok {
		return out, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read data sources: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read data sources: %w", err)
	}
	defer rows.Close()
	var out []*dataSourceRow
	for rows.Next() {
		var r dataSourceRow
		if err := rows.Scan(&r.Name, &r.Engine, &r.ReadOnly, &r.AsModule, &r.IsExtension,
			&r.Description, &r.LongDescription); err != nil {
			return nil, fmt.Errorf("read data sources: %w", err)
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read data sources: %w", err)
	}
	if g.dataSources == nil {
		g.dataSources = map[string][]*dataSourceRow{}
	}
	g.dataSources[query] = out
	return out, nil
}

// dataSourceModules lists the modules the source places DIRECT members in
// (the inverse of moduleMemberSources) — not the closure, so a source is
// reported where its objects actually live, without their ancestors.
func (g *genContext) dataSourceModules(ctx context.Context, name string) ([]string, error) {
	return g.queryNames(ctx, `SELECT module FROM (
		SELECT o.module AS module FROM core.catalog.data_objects o
			WHERE o.data_source = `+lit(name)+`
		UNION
		SELECT f.module FROM core.catalog.functions f
			WHERE f.data_source = `+lit(name)+`
		) ORDER BY module`)
}

// --- list readers (all activity-gated, deterministic ORDER BY) ---

// moduleMemberSources lists the distinct data sources of the module's DIRECT
// members (objects + functions), sorted — the ModuleInfo.DataSources contract.
func (g *genContext) moduleMemberSources(ctx context.Context, module string) ([]string, error) {
	return g.queryNames(ctx, `SELECT src FROM (
		SELECT o.data_source AS src FROM core.catalog.data_objects o`+activeMeta("m1", "o.data_source")+`
			WHERE o.module = `+lit(module)+`
		UNION
		SELECT f.data_source FROM core.catalog.functions f`+activeMeta("m2", "f.data_source")+`
			WHERE f.module = `+lit(module)+`
		) ORDER BY src`)
}

func (g *genContext) dataObjectNames(ctx context.Context, module string) ([]string, error) {
	return g.queryNames(ctx, `SELECT o.name FROM core.catalog.data_objects o`+
		activeMeta("m", "o.data_source")+`
		WHERE o.module = `+lit(module)+` ORDER BY o.name`)
}

// dataObjectExists probes through readDataObject: memoized, applies the full
// visibility gate (source activity AND declared dependencies) and records the
// found object's sources as provenance — a definition that branched on this
// probe is indexed under the target's sources.
func (g *genContext) dataObjectExists(ctx context.Context, name string) (bool, error) {
	row, err := g.readDataObject(ctx, name)
	return row != nil, err
}

func (g *genContext) readFunctions(ctx context.Context, module string) ([]*function, error) {
	if out, ok := g.functions[module]; ok {
		return out, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read functions of %s: %w", module, err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT f.module, f.name, f.kind, f.data_source, f.returns, f.is_table,
		f.args::JSON::VARCHAR, f.properties::JSON::VARCHAR, f.deprecation_reason, f.description
		FROM core.catalog.functions f`+activeMeta("m", "f.data_source")+`
		WHERE f.module = `+lit(module)+` ORDER BY f.name, f.kind`)
	if err != nil {
		return nil, fmt.Errorf("read functions of %s: %w", module, err)
	}
	defer rows.Close()
	var out []*function
	for rows.Next() {
		row, err := scanFunction(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("read functions of %s: %w", module, err)
		}
		g.touch(row.DataSource)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read functions of %s: %w", module, err)
	}
	if g.functions == nil {
		g.functions = map[string][]*function{}
	}
	g.functions[module] = out
	return out, nil
}

func (g *genContext) readSourceTypes(ctx context.Context) ([]*sourceType, error) {
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read source types: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT t.name, t.kind, t.data_source, t.module, t.definition, t.description
		FROM core.catalog.types t`+activeMeta("m", "t.data_source")+` ORDER BY t.name`)
	if err != nil {
		return nil, fmt.Errorf("read source types: %w", err)
	}
	defer rows.Close()
	var out []*sourceType
	for rows.Next() {
		var r sourceType
		var desc sql.NullString
		if err := rows.Scan(&r.Name, &r.Kind, &r.DataSource, &r.Module, &r.Definition, &desc); err != nil {
			return nil, fmt.Errorf("read source types: %w", err)
		}
		r.Description = desc.String
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read source types: %w", err)
	}
	return out, nil
}

// queryNames runs one single-column name query, memoized per session by the
// SQL text (covers dataObjectExists / dataObjectNames / moduleKindSources /
// resolveModulePath / lastListReturningModuleFunction).
func (g *genContext) queryNames(ctx context.Context, query string) ([]string, error) {
	if out, ok := g.nameRows[query]; ok {
		return out, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read names: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read names: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read names: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read names: %w", err)
	}
	if g.nameRows == nil {
		g.nameRows = map[string][]string{}
	}
	g.nameRows[query] = out
	return out, nil
}
