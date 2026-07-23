package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

// writerFormatVersion is mixed into the stored data_source_meta.version. Bump it
// when the row shapes / property bags change: every source then reads as
// "changed" once and is rewritten; an upgrade without a format change is free.
const writerFormatVersion = "f3"

const insertChunk = 200

// litEngine renders Go values to SQL literals — SQLValue already handles every
// type; the writer only shapes the INPUT (jsonOrNil for bag/JSON columns,
// nilIfEmpty for nullable text) so the same statement casts on both backends.
var litEngine = engines.NewDuckDB()

// SourceState is the load state of one data source. Version is the catalog
// content hash — an unchanged version makes writeSource a no-op. Engine is the
// source's engine type string (re-attached as @catalog(name, engine) on read);
// ReadOnly is the per-source option gating mutation generation. Prefix and
// AsModule are the compile options that shape names at read time.
type SourceState struct {
	Name         string
	Version      string
	Capabilities json.RawMessage
	Engine       string
	ReadOnly     bool
	Prefix       string
	AsModule     bool
	Loaded       bool
	Disabled     bool
	Suspended    bool
}

func (s SourceState) storedVersion() string { return writerFormatVersion + "|" + s.Version }

// writeSource persists one source's collected rows into the catalog namespace
// (the Add / Reload primitive). It is version-gated: if the stored version
// already matches, the call is a no-op after one metadata read (redeploys are
// free). Otherwise it rewrites the source's rows wholesale in a single
// transaction — delete by attribution, insert the collected rows, merge the
// module set, prune orphan modules — and stamps the meta. Returns whether the
// rows were rewritten (the caller scopes cache invalidation to it).
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
		return false, fmt.Errorf("catalog write %s commit: %w", state.Name, err)
	}
	return true, nil
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
	return s.pool.Commit(txCtx)
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
	_, err = conn.Exec(ctx, `UPDATE core.catalog.data_source_meta SET loaded = `+lit(loaded)+
		`, disabled = `+lit(disabled)+`, suspended = `+lit(suspended)+
		` WHERE data_source = `+lit(dataSource))
	if err != nil {
		return fmt.Errorf("catalog set flags %s: %w", dataSource, err)
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
		fields = append(fields, []any{r.TypeName, r.Name, r.FieldType, jsonOrNil(r.Properties), r.DataSource,
			nilIfEmpty(r.DependencyDataSource), r.IsPK, r.Ordinal,
			nilIfEmpty(r.DeprecationReason), nilIfEmpty(r.Description)})
	}
	if err := insertRows(ctx, conn, "fields",
		[]string{"type_name", "name", "field_type", "properties", "data_source",
			"dependency_data_source", "is_pk", "ordinal", "deprecation_reason", "description"}, fields); err != nil {
		return err
	}

	rels := make([][]any, 0, len(d.relations))
	for _, k := range sortedKeys(d.relations) {
		r := d.relations[k]
		rels = append(rels, []any{r.Source, r.Name, r.Kind, r.Destination, nilIfEmpty(r.M2MObject),
			jsonOrNil(r.SourceKeys), jsonOrNil(r.DestinationKeys), nilIfEmpty(r.SourceField),
			nilIfEmpty(r.SourceFieldDescription), nilIfEmpty(r.DestinationField),
			nilIfEmpty(r.DestinationFieldDescription), r.DataSource})
	}
	if err := insertRows(ctx, conn, "relations",
		[]string{"source", "name", "kind", "destination", "m2m_object", "source_keys", "destination_keys",
			"source_field", "source_field_description", "destination_field", "destination_field_description",
			"data_source"}, rels); err != nil {
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
		(data_source, version, capabilities, engine, read_only, prefix, as_module, loaded, disabled, suspended, loaded_at)
		VALUES (` + lit(state.Name) + `, ` + lit(state.storedVersion()) + `, ` + lit(capabilitiesText(state.Capabilities)) + `, ` +
		lit(state.Engine) + `, ` + lit(state.ReadOnly) + `, ` +
		lit(nilIfEmpty(state.Prefix)) + `, ` + lit(state.AsModule) + `, ` +
		lit(state.Loaded) + `, ` + lit(state.Disabled) + `, ` + lit(state.Suspended) + `, CURRENT_TIMESTAMP)
		ON CONFLICT (data_source) DO UPDATE SET version = EXCLUDED.version,
		capabilities = EXCLUDED.capabilities, engine = EXCLUDED.engine, read_only = EXCLUDED.read_only,
		prefix = EXCLUDED.prefix, as_module = EXCLUDED.as_module,
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
func jsonOrNil(v any) any {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	switch s := string(b); s {
	case "null", "{}", "[]":
		return nil
	default:
		return s
	}
}

func capabilitiesText(c json.RawMessage) any {
	if len(c) == 0 {
		return nil
	}
	return string(c)
}
