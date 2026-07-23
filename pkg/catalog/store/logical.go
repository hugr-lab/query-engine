package store

import (
	"context"
	"database/sql"
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
var _ catalog.LogicalModel = (*Store)(nil)

// Module resolves a module by full dotted name ("" = root); nil when absent or
// backed by no active source. ONE query: the root kinds come from a flat
// bool_or aggregate over the module's CLOSURE rows (the hierarchy was
// flattened at save time — buildModuleClosure), with the mutation kinds gated
// by data_source_meta.read_only (all-read-only sources → no mutations and no
// mutation functions).
func (s *Store) Module(ctx context.Context, name string) *catalog.ModuleInfo {
	rows := s.readModuleInfo(ctx, name)
	if len(rows) == 0 {
		if name != "" {
			return nil
		}
		// The root module exists even in an empty catalog.
		return &catalog.ModuleInfo{Name: "", RootTypes: map[sdl.ModuleObjectType]string{
			sdl.ModuleQuery: sdl.ModuleTypeName("", sdl.ModuleQuery),
		}}
	}
	return s.moduleInfo(ctx, rows[0])
}

// Modules iterates the DIRECT child modules of parent — children, activity and
// root kinds all come from ONE grouped query.
func (s *Store) Modules(ctx context.Context, parent string) iter.Seq[*catalog.ModuleInfo] {
	return func(yield func(*catalog.ModuleInfo) bool) {
		for _, row := range s.readChildModuleInfos(ctx, parent) {
			if !yield(s.moduleInfo(ctx, row)) {
				return
			}
		}
	}
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

func (s *Store) moduleInfo(ctx context.Context, row *moduleInfoRow) *catalog.ModuleInfo {
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
	return &catalog.ModuleInfo{
		Name:        row.Name,
		Description: row.Description,
		RootTypes:   roots,
		DataSources: s.moduleMemberSources(ctx, row.Name),
	}
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
func (s *Store) readModuleInfo(ctx context.Context, name string) []*moduleInfoRow {
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
	return s.queryModuleInfos(ctx, query)
}

// readChildModuleInfos lists the DIRECT children of parent with their kind
// flags — children, activity and kinds in one grouped query.
func (s *Store) readChildModuleInfos(ctx context.Context, parent string) []*moduleInfoRow {
	cond := `mm.parent = ` + lit(parent)
	if parent == "" {
		cond = `mm.parent IS NULL`
	}
	return s.queryModuleInfos(ctx, `SELECT mm.name, mm.description, `+moduleKindFlags+`
		FROM core.catalog.modules mm
		JOIN core.catalog.module_data_sources md ON md.module = mm.name`+
		activeMeta("ms", "md.data_source")+`
		WHERE `+cond+`
		GROUP BY mm.name, mm.description
		ORDER BY mm.name`)
}

func (s *Store) queryModuleInfos(ctx context.Context, query string) []*moduleInfoRow {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []*moduleInfoRow
	for rows.Next() {
		var r moduleInfoRow
		var desc sql.NullString
		var q, mut, fn, mfn, sub sql.NullBool
		if err := rows.Scan(&r.Name, &desc, &q, &mut, &fn, &mfn, &sub); err != nil {
			return out
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
	return out
}

// DataObject resolves a data object by type name (activity-gated), nil for
// anything else.
func (s *Store) DataObject(ctx context.Context, name string) *sdl.Object {
	def := reconstructDataObject(ctx, s, name)
	if def == nil || !sdl.IsDataObject(def) {
		return nil
	}
	return sdl.DataObjectInfo(def)
}

// DataObjects iterates the data objects that are members of a module.
func (s *Store) DataObjects(ctx context.Context, module string) iter.Seq[*sdl.Object] {
	return func(yield func(*sdl.Object) bool) {
		for _, name := range s.dataObjectNames(ctx, module) {
			info := s.DataObject(ctx, name)
			if info == nil {
				continue
			}
			if !yield(info) {
				return
			}
		}
	}
}

// Function resolves a callable member of a module by name ((module, name) is
// the storage primary key — no root probing needed).
func (s *Store) Function(ctx context.Context, module, name string) *catalog.FunctionEntry {
	row, ok := s.readFunction(ctx, module, name)
	if !ok {
		return nil
	}
	return functionEntry(row)
}

// Functions iterates all callable members of a module: function, mutation and
// subscription kinds in that order, by name within each kind.
func (s *Store) Functions(ctx context.Context, module string) iter.Seq[*catalog.FunctionEntry] {
	return func(yield func(*catalog.FunctionEntry) bool) {
		rows := s.readFunctions(ctx, module)
		slices.SortFunc(rows, func(a, b *function) int {
			if c := functionKindRank(a.Kind) - functionKindRank(b.Kind); c != 0 {
				return c
			}
			return strings.Compare(a.Name, b.Name)
		})
		for _, row := range rows {
			if !yield(functionEntry(row)) {
				return
			}
		}
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

// Relations synthesizes the logical edges of a data object from the physical
// legs (verified against the live etalon):
//   - own legs → FK FORWARD (a junction shows its two legs this way);
//   - a leg from an is_m2m junction → M2M FORWARD to each co-endpoint
//     (through = junction, keys reversed into endpoint→junction orientation,
//     field/description from the leg's destination side) — and no FK BACK;
//   - any other incoming leg → FK BACK (keys stay canonical);
//   - own declared @join fields targeting a data object → JOIN FORWARD.
func (s *Store) Relations(ctx context.Context, object string) iter.Seq[*catalog.RelationInfo] {
	return func(yield func(*catalog.RelationInfo) bool) {
		var rels []*catalog.RelationInfo

		for _, r := range s.relationsBySource(ctx, object) {
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

		for _, r := range s.relationsByDestination(ctx, object) {
			if r.m2mJunction {
				for _, co := range s.relationsBySource(ctx, r.Source) {
					if co.Name == r.Name {
						continue
					}
					rels = append(rels, &catalog.RelationInfo{
						Name:            r.Name,
						Direction:       catalog.RelationForward,
						Kind:            catalog.RelationM2M,
						FieldName:       r.DestinationField,
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
			rels = append(rels, &catalog.RelationInfo{
				Name:            r.Name,
				Direction:       catalog.RelationBack,
				Kind:            catalog.RelationFK,
				FieldName:       r.DestinationField,
				Description:     r.DestinationFieldDescription,
				DataObject:      r.Source,
				SourceKeys:      r.SourceKeys,
				DestinationKeys: r.DestinationKeys,
				DataSource:      r.DataSource,
			})
		}

		for _, f := range s.readFields(ctx, object) {
			j := f.Properties.Join
			if j == nil || !s.dataObjectExists(ctx, j.ReferencesName) {
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
		for _, rel := range rels {
			if !yield(rel) {
				return
			}
		}
	}
}

// Type resolves a type definition by name — the ForName resolver chain.
func (s *Store) Type(ctx context.Context, name string) *ast.Definition {
	return s.ForName(ctx, name)
}

// SourceTypes iterates the residual source-defined base types — exactly the
// content of catalog.types (active sources, by name).
func (s *Store) SourceTypes(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for _, row := range s.readSourceTypes(ctx) {
			def := parseStoredDefinition(row.Name, row.Definition)
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

// --- list readers (all activity-gated, deterministic ORDER BY) ---

// moduleMemberSources lists the distinct data sources of the module's DIRECT
// members (objects + functions), sorted — the ModuleInfo.DataSources contract.
func (s *Store) moduleMemberSources(ctx context.Context, module string) []string {
	return s.queryNames(ctx, `SELECT src FROM (
		SELECT o.data_source AS src FROM core.catalog.data_objects o`+activeMeta("m1", "o.data_source")+`
			WHERE o.module = `+lit(module)+`
		UNION
		SELECT f.data_source FROM core.catalog.functions f`+activeMeta("m2", "f.data_source")+`
			WHERE f.module = `+lit(module)+`
		) ORDER BY src`)
}

func (s *Store) dataObjectNames(ctx context.Context, module string) []string {
	return s.queryNames(ctx, `SELECT o.name FROM core.catalog.data_objects o`+
		activeMeta("m", "o.data_source")+`
		WHERE o.module = `+lit(module)+` ORDER BY o.name`)
}

func (s *Store) dataObjectExists(ctx context.Context, name string) bool {
	return len(s.queryNames(ctx, `SELECT o.name FROM core.catalog.data_objects o`+
		activeMeta("m", "o.data_source")+` WHERE o.name = `+lit(name))) > 0
}

func (s *Store) readFunctions(ctx context.Context, module string) []*function {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT f.module, f.name, f.kind, f.data_source, f.returns, f.is_table,
		f.args::JSON::VARCHAR, f.properties::JSON::VARCHAR, f.deprecation_reason, f.description
		FROM core.catalog.functions f`+activeMeta("m", "f.data_source")+`
		WHERE f.module = `+lit(module)+` ORDER BY f.name`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []*function
	for rows.Next() {
		row, err := scanFunction(rows.Scan)
		if err != nil {
			return out
		}
		out = append(out, row)
	}
	return out
}

func (s *Store) readSourceTypes(ctx context.Context) []*sourceType {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT t.name, t.kind, t.data_source, t.module, t.definition, t.description
		FROM core.catalog.types t`+activeMeta("m", "t.data_source")+` ORDER BY t.name`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []*sourceType
	for rows.Next() {
		var r sourceType
		var desc sql.NullString
		if err := rows.Scan(&r.Name, &r.Kind, &r.DataSource, &r.Module, &r.Definition, &desc); err != nil {
			return out
		}
		r.Description = desc.String
		out = append(out, &r)
	}
	return out
}

func (s *Store) queryNames(ctx context.Context, query string) []string {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return out
		}
		out = append(out, name)
	}
	return out
}
