package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

// writerFormatVersion is mixed into the stored data_source_meta.version. Bump it
// when the row shapes / property bags change: every source then reads as
// "changed" once and is rewritten; an upgrade without a format change is free.
const writerFormatVersion = "f7"

const insertChunk = 200

// litEngine renders Go values to SQL literals — SQLValue already handles every
// type; the writer only shapes the INPUT (jsonOrNil for bag/JSON columns,
// nilIfEmpty for nullable text) so the same statement casts on both backends.
var litEngine = engines.NewDuckDB()

// SourceState is the load state of one data source. Version is the catalog
// content hash — an unchanged version makes writeSource a no-op. Engine is the
// source's engine type string (re-attached as @catalog(name, engine) on read);
// ReadOnly is the per-source option gating mutation generation. Prefix and
// AsModule are the compile options that shape names at read time. IsExtension
// marks an extension source — generated members of its objects carry
// @dependency(name: <source>).
type SourceState struct {
	Name         string
	Version      string
	Capabilities json.RawMessage
	Engine       string
	ReadOnly     bool
	Prefix       string
	AsModule     bool
	IsExtension  bool
	Loaded       bool
	Disabled     bool
	Suspended    bool
}

// storedVersion mixes the format version, the caller's content hash AND the
// compile options into the stored version — a meta-only change (engine string,
// read_only, prefix, as_module, is_extension) must rewrite and invalidate
// too, not slip through the content gate.
func (s SourceState) storedVersion() string {
	return fmt.Sprintf("%s|%s|%s|%t|%s|%t|%t",
		writerFormatVersion, s.Version, s.Engine, s.ReadOnly, s.Prefix, s.AsModule, s.IsExtension)
}

// writeSource persists one source's collected rows into the catalog namespace
// (the Add / Reload primitive). It is version-gated: if the stored version
// already matches, the call is a no-op after one metadata read (redeploys are
// free). Otherwise it rewrites the source's rows wholesale in a single
// transaction — delete by attribution, insert the collected rows, merge the
// module set, prune orphan modules — and stamps the meta. Returns whether the
// rows were rewritten (the caller scopes cache invalidation to it). NOTE:
// under an ambient context transaction pool.WithTx JOINS it (nesting counter),
// so atomicity then spans the CALLER's transaction, not this call alone.
func (s *Store) writeSource(ctx context.Context, d *desired, state SourceState) (bool, error) {
	stored, ok, err := s.sourceVersion(ctx, state.Name)
	if err != nil {
		return false, err
	}
	if ok && stored == state.storedVersion() {
		return false, nil // unchanged — nothing to do
	}

	txCtx, err := s.pool.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("catalog write %s: %w", state.Name, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.pool.Rollback(txCtx)
		}
	}()
	conn, err := s.pool.Conn(txCtx)
	if err != nil {
		return false, fmt.Errorf("catalog write %s: %w", state.Name, err)
	}
	defer conn.Close()

	// Virtual fields are attributed to the source their DATA comes from,
	// resolved by the declared reference at WRITE time — the read side then
	// takes @catalog straight from the field row and the standard activity
	// gate hides the field with that source.
	if err := resolveVirtualAttribution(txCtx, conn, d); err != nil {
		return false, fmt.Errorf("catalog write %s: %w", state.Name, err)
	}

	// The cross-source invalidation closure needs the OLD rows (about to be
	// deleted) and the incoming ones; a failure here never fails the write —
	// the cache then invalidates wholesale.
	affected, affectedErr := affectedObjects(txCtx, conn, state.Name, d)

	if err := deleteSourceRows(txCtx, conn, state.Name); err != nil {
		return false, err
	}
	if err := insertSourceRows(txCtx, conn, d); err != nil {
		return false, err
	}
	if err := mergeModules(txCtx, conn, d); err != nil {
		return false, err
	}
	if err := pruneOrphanModules(txCtx, conn); err != nil {
		return false, err
	}
	if err := upsertMeta(txCtx, conn, state); err != nil {
		return false, err
	}

	committed = true
	if err := s.pool.Commit(txCtx); err != nil {
		// The commit may have succeeded server-side despite the client error —
		// invalidate conservatively.
		s.invalidateAll()
		return false, fmt.Errorf("catalog write %s commit: %w", state.Name, err)
	}
	if affectedErr != nil {
		s.invalidateAll()
	} else {
		s.invalidateSource(state.Name, affected)
	}
	return true, nil
}

// resolveVirtualAttribution stamps each virtual field row with the data
// source its DATA comes from, resolved through the declared reference: the
// referenced FUNCTION's source (by module, name) for @function_call and
// @table_function_call_join, the referenced OBJECT's source for @join. The
// incoming batch resolves first, then the stored catalog. An unresolvable
// reference keeps the declared attribution — the engine writes an extension
// AFTER its dependencies, and a reload re-resolves.
func resolveVirtualAttribution(ctx context.Context, conn *db.Connection, d *desired) error {
	lookupFunc := func(module, name string) (string, error) {
		if fn, ok := d.functions[pkKey(module, name)]; ok {
			return fn.DataSource, nil
		}
		var ds string
		err := conn.QueryRow(ctx, `SELECT data_source FROM core.catalog.functions
			WHERE module = `+lit(module)+` AND name = `+lit(name)).Scan(&ds)
		if err == sql.ErrNoRows {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		return ds, nil
	}
	lookupObject := func(name string) (string, error) {
		if row, ok := d.dataObjects[name]; ok {
			return row.DataSource, nil
		}
		var ds string
		err := conn.QueryRow(ctx, `SELECT data_source FROM core.catalog.data_objects
			WHERE name = `+lit(name)).Scan(&ds)
		if err == sql.ErrNoRows {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		return ds, nil
	}
	for _, f := range d.fields {
		p := f.Properties
		if p == nil {
			continue
		}
		var src string
		var err error
		switch {
		case p.Join != nil:
			src, err = lookupObject(p.Join.ReferencesName)
		case p.FunctionCall != nil:
			src, err = lookupFunc(p.FunctionCall.Function.Module, p.FunctionCall.Function.Name)
		case p.TableFunctionCallJoin != nil:
			src, err = lookupFunc(p.TableFunctionCallJoin.Function.Module, p.TableFunctionCallJoin.Function.Name)
		default:
			continue
		}
		if err != nil {
			return err
		}
		if src != "" {
			f.DataSource = src
		}
	}
	return nil
}

// affectedObjects collects the names whose COMPILED shape can depend on the
// source's rows — the absence dependencies the read-side provenance index
// cannot see (a definition built BEFORE such rows existed recorded no
// dependency on this source). Both the STORED rows (about to be deleted /
// flag-flipped) and the incoming desired rows (nil for delete / flags)
// contribute:
//   - the source's relations: both endpoints gain/lose nav fields, markers
//     and derived-type members;
//   - relations of ANY source whose endpoint is one of this source's objects
//     (extension-declared legs live under the DECLARING source);
//   - the source's module functions returning object lists (they rename the
//     target's aggregation markers);
//   - the objects the source's fields extend (cross-source `extend type`);
//   - fields of ANY source TYPED with one of this source's object/type names
//     (@join targets — the field type IS the target — and structural member
//     references);
//   - fields of ANY source whose @function_call / @table_function_call_join
//     binding references one of this source's functions (module, name) — the
//     effective @catalog of such fields is computed from the function's
//     source (virtualFieldSource).
//
// All lookups read DECLARED rows (types, references_name, function bindings) —
// a reverse index computed at write time, no heuristics.
func affectedObjects(ctx context.Context, conn *db.Connection, dataSource string, d *desired) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	ds := lit(dataSource)

	scanInto := func(query string, cols int) error {
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			a, b := "", ""
			dest := []any{&a}
			if cols == 2 {
				dest = append(dest, &b)
			}
			if err := rows.Scan(dest...); err != nil {
				return err
			}
			out[a] = struct{}{}
			if cols == 2 {
				out[b] = struct{}{}
			}
		}
		return rows.Err()
	}

	// Rows OWNED by the source.
	if err := scanInto(`SELECT r.source, r.destination FROM core.catalog.relations r
		WHERE r.data_source = `+ds, 2); err != nil {
		return nil, err
	}
	if err := scanInto(`SELECT DISTINCT f.type_name FROM core.catalog.fields f
		WHERE f.data_source = `+ds+` OR f.dependency_data_source = `+ds, 1); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, `SELECT f.returns FROM core.catalog.functions f
		WHERE f.data_source = `+ds+` AND f.kind = 'function' AND f.module <> ''`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var returns string
		if err := rows.Scan(&returns); err != nil {
			rows.Close()
			return nil, err
		}
		if target, isList := listReturnTarget(&function{Returns: returns}); isList {
			out[target] = struct{}{}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// The source's entity NAMES (old stored + incoming) drive the reverse
	// lookups: rows of OTHER sources referencing them.
	names := map[string]struct{}{}
	if err := func() error {
		rows, err := conn.Query(ctx, `SELECT o.name FROM core.catalog.data_objects o WHERE o.data_source = `+ds+`
			UNION SELECT t.name FROM core.catalog.types t WHERE t.data_source = `+ds)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			names[name] = struct{}{}
		}
		return rows.Err()
	}(); err != nil {
		return nil, err
	}
	if d != nil {
		for name := range d.dataObjects {
			names[name] = struct{}{}
		}
		for name := range d.types {
			names[name] = struct{}{}
		}
		for _, r := range d.relations {
			out[r.Source] = struct{}{}
			out[r.Destination] = struct{}{}
		}
		for _, f := range d.fields {
			out[f.TypeName] = struct{}{}
		}
		for _, fn := range d.functions {
			if fn.Kind != "function" || fn.Module == "" {
				continue
			}
			if target, isList := listReturnTarget(fn); isList {
				out[target] = struct{}{}
			}
		}
	}
	if len(names) > 0 {
		list := make([]string, 0, len(names))
		for _, name := range sortedKeys(names) {
			list = append(list, lit(name))
		}
		in := strings.Join(list, ", ")
		if err := scanInto(`SELECT r.source, r.destination FROM core.catalog.relations r
			WHERE r.source IN (`+in+`) OR r.destination IN (`+in+`)`, 2); err != nil {
			return nil, err
		}
		// trim strips the list/non-null wrapping ([X!]! → X) — the field TYPE
		// is the declared reference target for @join and structural members.
		if err := scanInto(`SELECT DISTINCT f.type_name FROM core.catalog.fields f
			WHERE trim(f.field_type, '[]!') IN (`+in+`)`, 1); err != nil {
			return nil, err
		}
	}

	// The source's FUNCTIONS (old stored + incoming) drive the function-call
	// reverse lookup: @function_call / @table_function_call_join fields are
	// typed with the function's RETURN, so the type match above cannot see
	// them — match the declared (module, name) binding instead.
	functions := map[string]struct{}{}
	if err := func() error {
		rows, err := conn.Query(ctx, `SELECT f.module, f.name FROM core.catalog.functions f
			WHERE f.data_source = `+ds)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var module, name string
			if err := rows.Scan(&module, &name); err != nil {
				return err
			}
			functions[pkKey(module, name)] = struct{}{}
		}
		return rows.Err()
	}(); err != nil {
		return nil, err
	}
	if d != nil {
		for key := range d.functions {
			functions[key] = struct{}{}
		}
	}
	if len(functions) > 0 {
		rows, err := conn.Query(ctx, `SELECT f.type_name,
			json_extract_string(f.properties::JSON, '$.function_call.function.module'),
			json_extract_string(f.properties::JSON, '$.function_call.function.name'),
			json_extract_string(f.properties::JSON, '$.table_function_call_join.function.module'),
			json_extract_string(f.properties::JSON, '$.table_function_call_join.function.name')
			FROM core.catalog.fields f
			WHERE json_extract(f.properties::JSON, '$.function_call') IS NOT NULL
			OR json_extract(f.properties::JSON, '$.table_function_call_join') IS NOT NULL`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var typeName string
			var fcModule, fcName, tfModule, tfName sql.NullString
			if err := rows.Scan(&typeName, &fcModule, &fcName, &tfModule, &tfName); err != nil {
				return nil, err
			}
			if _, ok := functions[pkKey(fcModule.String, fcName.String)]; ok {
				out[typeName] = struct{}{}
				continue
			}
			if _, ok := functions[pkKey(tfModule.String, tfName.String)]; ok {
				out[typeName] = struct{}{}
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// deleteSource removes a source's rows, meta and dependencies (the unregister
// primitive). Orphan modules are pruned; modules still backed by other sources
// stay.
func (s *Store) deleteSource(ctx context.Context, dataSource string) error {
	txCtx, err := s.pool.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("catalog delete %s: %w", dataSource, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.pool.Rollback(txCtx)
		}
	}()
	conn, err := s.pool.Conn(txCtx)
	if err != nil {
		return fmt.Errorf("catalog delete %s: %w", dataSource, err)
	}
	defer conn.Close()

	affected, affectedErr := affectedObjects(txCtx, conn, dataSource, nil)

	if err := deleteSourceRows(txCtx, conn, dataSource); err != nil {
		return err
	}
	ds := lit(dataSource)
	for _, stmt := range []string{
		`DELETE FROM core.catalog.data_source_dependencies WHERE data_source = ` + ds,
		`DELETE FROM core.catalog.data_source_meta WHERE data_source = ` + ds,
	} {
		if _, err := conn.Exec(txCtx, stmt); err != nil {
			return fmt.Errorf("catalog delete %s: %w", dataSource, err)
		}
	}
	if err := pruneOrphanModules(txCtx, conn); err != nil {
		return err
	}
	committed = true
	if err := s.pool.Commit(txCtx); err != nil {
		// The commit may have succeeded server-side despite the client error —
		// invalidate conservatively.
		s.invalidateAll()
		return fmt.Errorf("catalog delete %s: %w", dataSource, err)
	}
	if affectedErr != nil {
		s.invalidateAll()
	} else {
		s.invalidateSource(dataSource, affected)
	}
	return nil
}

// setFlags mirrors a source's load / disable / suspend flags into the meta
// (unload, suspend, enable — the rows stay, hidden by the flags). A missing
// meta row (source never written) is a no-op.
func (s *Store) setFlags(ctx context.Context, dataSource string, loaded, disabled, suspended bool) error {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("catalog set flags %s: %w", dataSource, err)
	}
	defer conn.Close()
	// Flag flips change VISIBILITY, not rows — the affected closure reads the
	// (surviving) rows either before hiding or after unhiding.
	affected, affectedErr := affectedObjects(ctx, conn, dataSource, nil)
	_, err = conn.Exec(ctx, `UPDATE core.catalog.data_source_meta SET loaded = `+lit(loaded)+
		`, disabled = `+lit(disabled)+`, suspended = `+lit(suspended)+
		` WHERE data_source = `+lit(dataSource))
	if err != nil {
		return fmt.Errorf("catalog set flags %s: %w", dataSource, err)
	}
	if affectedErr != nil {
		s.invalidateAll()
	} else {
		s.invalidateSource(dataSource, affected)
	}
	return nil
}

// sourceVersion reads the stored meta version of a source (ok=false when absent).
func (s *Store) sourceVersion(ctx context.Context, dataSource string) (string, bool, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return "", false, fmt.Errorf("catalog read version %s: %w", dataSource, err)
	}
	defer conn.Close()
	var v string
	err = conn.QueryRow(ctx, `SELECT version FROM core.catalog.data_source_meta WHERE data_source = `+lit(dataSource)).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("catalog read version %s: %w", dataSource, err)
	}
	return v, true, nil
}

func deleteSourceRows(ctx context.Context, conn *db.Connection, dataSource string) error {
	// Strictly own attribution: a source touches only its own rows. Fields an
	// EXTENSION source added to another source's object carry their own
	// data_source, so reloading the base source never removes them (cross-source
	// dependents are refreshed by the engine's cascade reload).
	ds := lit(dataSource)
	for _, stmt := range []string{
		`DELETE FROM core.catalog.data_objects WHERE data_source = ` + ds,
		`DELETE FROM core.catalog.fields WHERE data_source = ` + ds,
		`DELETE FROM core.catalog.relations WHERE data_source = ` + ds,
		`DELETE FROM core.catalog.functions WHERE data_source = ` + ds,
		`DELETE FROM core.catalog.types WHERE data_source = ` + ds,
		`DELETE FROM core.catalog.module_data_sources WHERE data_source = ` + ds,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("catalog delete rows %s: %w", dataSource, err)
		}
	}
	return nil
}

// insertSourceRows inserts every entity row collected for one source.
func insertSourceRows(ctx context.Context, conn *db.Connection, d *desired) error {
	objs := make([][]any, 0, len(d.dataObjects))
	for _, k := range sortedKeys(d.dataObjects) {
		r := d.dataObjects[k]
		objs = append(objs, []any{r.Name, r.OriginalName, r.DataSource, r.Module, r.Kind, jsonOrNil(r.Properties), nilIfEmpty(r.Description)})
	}
	if err := insertRows(ctx, conn, "data_objects",
		[]string{"name", "original_name", "data_source", "module", "kind", "properties", "description"}, objs); err != nil {
		return err
	}

	fields := make([][]any, 0, len(d.fields))
	for _, k := range sortedKeys(d.fields) {
		r := d.fields[k]
		fields = append(fields, []any{r.TypeName, r.Name, r.FieldType, jsonOrNil(r.Properties), jsonOrNil(r.Args),
			r.DataSource, nilIfEmpty(r.DependencyDataSource), r.IsPK, r.Ordinal,
			nilIfEmpty(r.DeprecationReason), nilIfEmpty(r.Description)})
	}
	if err := insertRows(ctx, conn, "fields",
		[]string{"type_name", "name", "field_type", "properties", "args", "data_source",
			"dependency_data_source", "is_pk", "ordinal", "deprecation_reason", "description"}, fields); err != nil {
		return err
	}

	rels := make([][]any, 0, len(d.relations))
	for _, k := range sortedKeys(d.relations) {
		r := d.relations[k]
		rels = append(rels, []any{r.Source, r.Name, r.Kind, r.Destination, nilIfEmpty(r.M2MObject),
			jsonOrNil(r.SourceKeys), jsonOrNil(r.DestinationKeys), nilIfEmpty(r.SourceField),
			// destination_field keeps the EMPTY string so the verbatim
			// @field_references re-emission (fieldReferencesDirective) can
			// reproduce an explicit references_query: "" as declared.
			// Semantically "" means the DEFAULT — generation collapses it
			// via orDefault everywhere.
			nilIfEmpty(r.SourceFieldDescription), r.DestinationField,
			nilIfEmpty(r.DestinationFieldDescription), r.FieldDeclared, r.DataSource})
	}
	if err := insertRows(ctx, conn, "relations",
		[]string{"source", "name", "kind", "destination", "m2m_object", "source_keys", "destination_keys",
			"source_field", "source_field_description", "destination_field", "destination_field_description",
			"field_declared", "data_source"}, rels); err != nil {
		return err
	}

	fns := make([][]any, 0, len(d.functions))
	for _, k := range sortedKeys(d.functions) {
		r := d.functions[k]
		fns = append(fns, []any{r.Module, r.Name, r.Kind, r.DataSource, r.Returns, r.IsTable,
			jsonOrNil(r.Args), jsonOrNil(r.Properties), nilIfEmpty(r.DeprecationReason), nilIfEmpty(r.Description)})
	}
	if err := insertRows(ctx, conn, "functions",
		[]string{"module", "name", "kind", "data_source", "returns", "is_table", "args", "properties",
			"deprecation_reason", "description"}, fns); err != nil {
		return err
	}

	types := make([][]any, 0, len(d.types))
	for _, k := range sortedKeys(d.types) {
		r := d.types[k]
		types = append(types, []any{r.Name, r.Kind, r.DataSource, r.Module, r.Definition, nilIfEmpty(r.Description)})
	}
	return insertRows(ctx, conn, "types",
		[]string{"name", "kind", "data_source", "module", "definition", "description"}, types)
}

// mergeModules upserts the source's module rows and inserts its module→source
// closure (its old closure rows were removed by deleteSourceRows). Modules are
// shared across sources, so they are upserted, never attributed.
func mergeModules(ctx context.Context, conn *db.Connection, d *desired) error {
	moduleRows := make([][]any, 0, len(d.modules))
	for _, k := range sortedKeys(d.modules) {
		m := d.modules[k]
		moduleRows = append(moduleRows, []any{m.Name, nilIfEmpty(m.Parent), nilIfEmpty(m.Description)})
	}
	if err := upsertRows(ctx, conn, "modules",
		[]string{"name", "parent", "description"}, []string{"name"},
		[]string{"parent", "description"}, moduleRows); err != nil {
		return err
	}
	links := make([][]any, 0, len(d.moduleSources))
	for _, k := range sortedKeys(d.moduleSources) {
		ms := d.moduleSources[k]
		links = append(links, []any{ms.Module, ms.DataSource,
			ms.HasDataObjects, ms.HasTables, ms.HasFunctions, ms.HasMutFunctions, ms.HasSubscriptions})
	}
	return insertRows(ctx, conn, "module_data_sources",
		[]string{"module", "data_source", "has_data_objects", "has_tables",
			"has_functions", "has_mut_functions", "has_subscriptions"}, links)
}

// pruneOrphanModules removes modules no source backs anymore — with the closure,
// a live module always has at least one module_data_sources row.
func pruneOrphanModules(ctx context.Context, conn *db.Connection) error {
	_, err := conn.Exec(ctx, `DELETE FROM core.catalog.modules
		WHERE name NOT IN (SELECT DISTINCT module FROM core.catalog.module_data_sources)`)
	if err != nil {
		return fmt.Errorf("catalog prune modules: %w", err)
	}
	return nil
}

// upsertMeta stamps the source's version, capabilities and flags.
func upsertMeta(ctx context.Context, conn *db.Connection, state SourceState) error {
	stmt := `INSERT INTO core.catalog.data_source_meta
		(data_source, version, capabilities, engine, read_only, prefix, as_module, is_extension, loaded, disabled, suspended, loaded_at)
		VALUES (` + lit(state.Name) + `, ` + lit(state.storedVersion()) + `, ` + lit(capabilitiesText(state.Capabilities)) + `, ` +
		lit(state.Engine) + `, ` + lit(state.ReadOnly) + `, ` +
		lit(nilIfEmpty(state.Prefix)) + `, ` + lit(state.AsModule) + `, ` + lit(state.IsExtension) + `, ` +
		lit(state.Loaded) + `, ` + lit(state.Disabled) + `, ` + lit(state.Suspended) + `, CURRENT_TIMESTAMP)
		ON CONFLICT (data_source) DO UPDATE SET version = EXCLUDED.version,
		capabilities = EXCLUDED.capabilities, engine = EXCLUDED.engine, read_only = EXCLUDED.read_only,
		prefix = EXCLUDED.prefix, as_module = EXCLUDED.as_module, is_extension = EXCLUDED.is_extension,
		loaded = EXCLUDED.loaded, disabled = EXCLUDED.disabled, suspended = EXCLUDED.suspended, loaded_at = EXCLUDED.loaded_at`
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("catalog meta upsert %s: %w", state.Name, err)
	}
	return nil
}

// --- SQL building ---

func insertRows(ctx context.Context, conn *db.Connection, table string, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	head := `INSERT INTO core.catalog.` + table + ` (` + strings.Join(columns, ", ") + `) VALUES `
	for start := 0; start < len(rows); start += insertChunk {
		end := min(start+insertChunk, len(rows))
		var b strings.Builder
		b.WriteString(head)
		for i, r := range rows[start:end] {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(tuple(r))
		}
		if _, err := conn.Exec(ctx, b.String()); err != nil {
			return fmt.Errorf("catalog insert %s: %w", table, err)
		}
	}
	return nil
}

// upsertRows inserts with ON CONFLICT DO UPDATE of the given columns.
func upsertRows(ctx context.Context, conn *db.Connection, table string, columns, pkCols, updateCols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	sets := make([]string, len(updateCols))
	for i, c := range updateCols {
		sets[i] = c + " = EXCLUDED." + c
	}
	tail := ` ON CONFLICT (` + strings.Join(pkCols, ", ") + `) DO UPDATE SET ` + strings.Join(sets, ", ")
	head := `INSERT INTO core.catalog.` + table + ` (` + strings.Join(columns, ", ") + `) VALUES `
	for start := 0; start < len(rows); start += insertChunk {
		end := min(start+insertChunk, len(rows))
		var b strings.Builder
		b.WriteString(head)
		for i, r := range rows[start:end] {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(tuple(r))
		}
		b.WriteString(tail)
		if _, err := conn.Exec(ctx, b.String()); err != nil {
			return fmt.Errorf("catalog upsert %s: %w", table, err)
		}
	}
	return nil
}

// tuple renders a VALUES row via engines.DuckDB.SQLValue (values are pre-shaped
// by jsonOrNil / nilIfEmpty at the call site).
func tuple(vals []any) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = lit(v)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func lit(v any) string {
	if j, ok := v.(jsonText); ok {
		s, err := litEngine.SQLValue(string(j))
		if err != nil {
			return "NULL"
		}
		return s + "::JSON"
	}
	s, err := litEngine.SQLValue(v)
	if err != nil {
		return "NULL"
	}
	return s
}

// nilIfEmpty maps an empty string to a SQL NULL (nullable text columns).
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// jsonOrNil marshals a bag / keys value to plain JSON text (the STRUCT/JSONB
// columns take it via the implicit cast); empty content collapses to NULL.
// jsonText marks a value that must reach the backend AS JSON: lit() renders
// it with an explicit ::JSON cast. DuckDB's plain VARCHAR→STRUCT cast parses
// the text with its own struct-literal reader which EATS backslashes
// (`a\nb`→`anb`); VARCHAR→JSON(→STRUCT) preserves the escapes.
type jsonText string

func jsonOrNil(v any) any {
	if v == nil {
		return nil
	}
	s, err := marshalJSONText(v)
	if err != nil {
		return nil
	}
	switch s {
	case "null", "{}", "[]":
		return nil
	}
	// Re-marshal PADDED: the JSON→STRUCT cast is strict about missing keys,
	// so every struct key must be present (null when absent).
	s, err = marshalJSONText(padForStrictCast(reflect.ValueOf(v)))
	if err != nil {
		return nil
	}
	switch s {
	case "null", "{}", "[]":
		return nil
	default:
		return jsonText(s)
	}
}

func capabilitiesText(c json.RawMessage) any {
	if len(c) == 0 {
		return nil
	}
	return string(c)
}

// marshalJSONText marshals WITHOUT HTML escaping — the stored JSON must
// round-trip SQL text (`->` operators in @function(sql:) etc.) byte-exact.
func marshalJSONText(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

// padForStrictCast converts a bag value into a JSON-marshalable form carrying
// EVERY key of its Go struct shape (null for absent members) — DuckDB's
// JSON→STRUCT cast (the only escape-correct text path into the bag columns)
// rejects objects with missing keys. The Go struct stays the single source of
// truth for the shape.
func padForStrictCast(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return padForStrictCast(v.Elem())
	case reflect.Struct:
		out := make(map[string]any, v.NumField())
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			tag := t.Field(i).Tag.Get("json")
			name, _, _ := strings.Cut(tag, ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = t.Field(i).Name
			}
			out[name] = padForStrictCast(v.Field(i))
		}
		return out
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil
		}
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = padForStrictCast(v.Index(i))
		}
		return out
	default:
		return v.Interface()
	}
}
