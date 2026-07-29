package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/ingest"
	catsrc "github.com/hugr-lab/query-engine/pkg/catalog/sources"
)

// Compile-time check that Store implements catalog.CatalogManager —
// catalog.NewService auto-detects the manager on the provider it is given.
var _ catalog.CatalogManager = (*Store)(nil)

// ErrReadOnly is returned by every write entry point when the store was
// configured read-only (cluster workers read the catalog the writer node
// populated).
var ErrReadOnly = errors.New("catalog store: read-only mode")

// AddCatalog registers a catalog source and persists its entities. It is
// version-gated on the STORED version (content hash + compile options): when
// unchanged the rows are current and only the visibility flags are repaired —
// a redeploy costs one metadata read. Otherwise the source is partially
// compiled (VALIDATE + PREPARE against the live store), collected and written
// wholesale in one transaction. A content rewrite refreshes active dependents
// (their references may resolve differently against the new base), and any
// change retries suspended sources whose dependencies may now be satisfied.
func (s *Store) AddCatalog(ctx context.Context, name string, cat catsrc.Catalog) error {
	if s.isReadonly {
		return ErrReadOnly
	}
	rewritten, changed, err := s.addCatalog(ctx, name, cat)
	if err != nil {
		return err
	}
	if rewritten {
		s.refreshDependents(ctx, name, map[string]bool{name: true})
	}
	if changed {
		s.reactivateSuspended(ctx)
	}
	return nil
}

// addCatalog is the gate + write for ONE source (no cascade). Returns whether
// the rows were rewritten and whether anything observable changed at all
// (rows or flags).
func (s *Store) addCatalog(ctx context.Context, name string, cat catsrc.Catalog) (rewritten, changed bool, err error) {
	state, err := s.sourceState(ctx, name, cat)
	if err != nil {
		return false, false, err
	}
	meta, ok, err := s.sourceMeta(ctx, name)
	if err != nil {
		return false, false, err
	}
	if ok && meta.Version == state.storedVersion() {
		// Content and options unchanged — the stored rows are current. Repair
		// visibility only; the disabled flag is an operator decision and
		// survives redeploys. A source whose stored dependencies are not
		// satisfied stays (or becomes) suspended — reactivateSuspended picks
		// it up when the dependencies arrive.
		s.putCatalog(name, cat)
		satisfied, err := s.storedDepsSatisfied(ctx, name)
		if err != nil {
			return false, false, err
		}
		switch {
		case !satisfied && !meta.Suspended:
			if err := s.setFlags(ctx, name, meta.Loaded, meta.Disabled, true); err != nil {
				return false, false, err
			}
			s.bumpSchemaVersion(ctx, name)
			slog.Warn("catalog suspended: dependencies not satisfied", "catalog", name)
			return false, true, nil
		case satisfied && (meta.Suspended || !meta.Loaded):
			if err := s.setFlags(ctx, name, true, meta.Disabled, false); err != nil {
				return false, false, err
			}
			s.bumpSchemaVersion(ctx, name)
			slog.Info("catalog resumed (version unchanged)", "catalog", name, "version", state.Version)
			return false, true, nil
		}
		return false, false, nil
	}
	if ok {
		state.Disabled = meta.Disabled
	}
	if err := s.compileAndWrite(ctx, name, cat, &state); err != nil {
		return false, false, err
	}
	s.putCatalog(name, cat)
	s.bumpSchemaVersion(ctx, name)
	slog.Info("catalog stored", "catalog", name, "version", state.Version,
		"dependencies", state.Dependencies, "suspended", state.Suspended)
	return true, true, nil
}

// ReplacesCatalogOnAdd reports the store's write contract: writeSource deletes
// the source's rows and re-inserts them wholesale inside one transaction, so
// an entity the source stopped declaring cannot survive an add. A reload
// therefore needs no destructive pre-step — see catalog.ReplacingCatalogManager.
func (s *Store) ReplacesCatalogOnAdd() bool { return true }

// RemoveCatalog deletes a source's entity rows, metadata, dependencies and
// seed annotations (the unregister primitive) and drops the source handle.
// Sources depending on it are suspended first (recursively) — their rows stay
// and reactivateSuspended restores them when the dependency returns.
func (s *Store) RemoveCatalog(ctx context.Context, name string) error {
	if s.isReadonly {
		return ErrReadOnly
	}
	s.suspendDependents(ctx, name, map[string]bool{name: true})
	if err := s.deleteSource(ctx, name); err != nil {
		return err
	}
	s.dropCatalog(name)
	s.bumpSchemaVersion(ctx, name)
	slog.Info("catalog removed", "catalog", name)
	return nil
}

// ExistsCatalog reports whether the source is known — as a live handle or as
// stored metadata (registered by a previous process).
func (s *Store) ExistsCatalog(name string) bool {
	s.catMu.RLock()
	_, ok := s.catalogs[name]
	s.catMu.RUnlock()
	if ok {
		return true
	}
	_, ok, err := s.sourceMeta(context.Background(), name)
	if err != nil {
		logReadErr("ExistsCatalog", err)
		return false
	}
	return ok
}

// ReloadCatalog refreshes a source (Reload when supported) and re-runs the
// Add path — the version gate makes an unchanged catalog a cheap no-op, so
// there is no separate incremental path: a changed version rewrites the
// source's rows wholesale.
func (s *Store) ReloadCatalog(ctx context.Context, name string) error {
	if s.isReadonly {
		return ErrReadOnly
	}
	s.catMu.RLock()
	cat, ok := s.catalogs[name]
	s.catMu.RUnlock()
	if !ok {
		return fmt.Errorf("catalog %q not found", name)
	}
	if rc, ok := cat.(catsrc.ReloadableCatalog); ok {
		if err := rc.Reload(ctx); err != nil {
			return fmt.Errorf("reload source %q: %w", name, err)
		}
	}
	return s.AddCatalog(ctx, name, cat)
}

// SuspendCatalog hides a source without touching its rows — the read-side
// activity gate excludes suspended sources. A source that was never written is
// a no-op (mirrors setFlags).
func (s *Store) SuspendCatalog(ctx context.Context, name string) error {
	if s.isReadonly {
		return ErrReadOnly
	}
	meta, ok, err := s.sourceMeta(ctx, name)
	if err != nil {
		return err
	}
	if !ok || meta.Suspended {
		return nil
	}
	if err := s.setFlags(ctx, name, meta.Loaded, meta.Disabled, true); err != nil {
		return err
	}
	s.bumpSchemaVersion(ctx, name)
	slog.Info("catalog suspended", "catalog", name)
	return nil
}

// ReactivateCatalog re-activates a previously suspended catalog. The rows were
// never dropped, so this is the Add path: an unchanged version just clears the
// suspended flag, a changed one recompiles.
func (s *Store) ReactivateCatalog(ctx context.Context, name string, cat catsrc.Catalog) error {
	return s.AddCatalog(ctx, name, cat)
}

// IsSuspended reports whether the source exists and is suspended.
func (s *Store) IsSuspended(name string) bool {
	meta, ok, err := s.sourceMeta(context.Background(), name)
	if err != nil {
		logReadErr("IsSuspended", err)
		return false
	}
	return ok && meta.Suspended
}

// compileAndWrite runs the write-side pipeline: VALIDATE + PREPARE with the
// live Store as the compile target (cross-source references resolve through
// ForName), collect the physical entities, persist them transactionally. The
// compiled @dependency set lands in state; when a declared dependency is not
// active the source is written SUSPENDED (§5.1 suspend semantics — the rows
// are order-independent, visibility is not) and reactivateSuspended restores
// it once the dependencies arrive.
func (s *Store) compileAndWrite(ctx context.Context, name string, cat catsrc.Catalog, state *SourceState) error {
	compiled, err := ingest.New(ingest.Default()...).Compile(ctx, s, cat, cat.CompileOptions())
	if err != nil {
		// Tagged so the boot path can tell a bad declaration from an
		// unreachable source and suspend instead of dropping the rows.
		return fmt.Errorf("compile catalog %q: %w: %w", name, catalog.ErrSchemaInvalid, err)
	}
	if dc, ok := compiled.(base.DependentCompiledCatalog); ok {
		state.Dependencies = dc.Dependencies()
	}
	if len(state.Dependencies) > 0 && !state.Suspended {
		satisfied, err := s.depsActive(ctx, state.Dependencies)
		if err != nil {
			return err
		}
		if !satisfied {
			state.Suspended = true
			slog.Warn("catalog stored suspended: dependencies not satisfied",
				"catalog", name, "dependencies", state.Dependencies)
		}
	}
	d := collect(ctx, asExtensionsSource(cat), name)
	if _, err := s.writeSource(ctx, d, *state); err != nil {
		return err
	}
	return nil
}

// refreshDependents force-rewrites the ACTIVE dependents of a source whose
// content changed: their own versions are unchanged, but their extension
// references and virtual-field attribution resolve against the new base. A
// dependent that fails to refresh is suspended (the old manager's semantics —
// reactivation retries it later). Best-effort: failures are logged, the
// cascade continues.
func (s *Store) refreshDependents(ctx context.Context, base string, visited map[string]bool) {
	dependents, err := s.dependentsOf(ctx, base)
	if err != nil {
		logReadErr("refreshDependents", err)
		return
	}
	for _, name := range dependents {
		if visited[name] {
			continue
		}
		visited[name] = true
		s.catMu.RLock()
		cat, ok := s.catalogs[name]
		s.catMu.RUnlock()
		if !ok {
			continue // no live handle in this process — rows stay as written
		}
		meta, mok, err := s.sourceMeta(ctx, name)
		if err != nil || !mok || meta.Suspended {
			continue // suspended dependents are reactivateSuspended's job
		}
		state, err := s.sourceState(ctx, name, cat)
		if err != nil {
			slog.Error("refresh dependent: state", "catalog", name, "error", err)
			continue
		}
		state.Disabled = meta.Disabled
		state.Force = true
		if err := s.compileAndWrite(ctx, name, cat, &state); err != nil {
			slog.Error("refresh dependent failed; suspending", "catalog", name, "base", base, "error", err)
			if err := s.setFlags(ctx, name, meta.Loaded, meta.Disabled, true); err != nil {
				slog.Error("suspend failed dependent", "catalog", name, "error", err)
			}
			continue
		}
		slog.Info("dependent catalog refreshed", "catalog", name, "base", base)
		s.refreshDependents(ctx, name, visited)
	}
}

// reactivateSuspended retries suspended sources that have a live handle and
// whose stored dependencies are all active — loop until a full pass makes no
// progress (a reactivation may satisfy the next source's dependencies).
// Reactivation always recompiles (Force): the suspension reason was a missing
// or changed base, so the stored rows' resolution is stale. Best-effort.
func (s *Store) reactivateSuspended(ctx context.Context) {
	for {
		suspended, err := s.suspendedSources(ctx)
		if err != nil {
			logReadErr("reactivateSuspended", err)
			return
		}
		progress := false
		for _, name := range suspended {
			s.catMu.RLock()
			cat, ok := s.catalogs[name]
			s.catMu.RUnlock()
			if !ok {
				continue
			}
			satisfied, err := s.storedDepsSatisfied(ctx, name)
			if err != nil || !satisfied {
				continue
			}
			meta, mok, err := s.sourceMeta(ctx, name)
			if err != nil || !mok {
				continue
			}
			state, err := s.sourceState(ctx, name, cat)
			if err != nil {
				slog.Error("reactivate: state", "catalog", name, "error", err)
				continue
			}
			state.Disabled = meta.Disabled
			state.Force = true
			if err := s.compileAndWrite(ctx, name, cat, &state); err != nil {
				slog.Error("reactivate failed", "catalog", name, "error", err)
				continue
			}
			s.bumpSchemaVersion(ctx, name)
			slog.Info("catalog reactivated", "catalog", name)
			progress = true
		}
		if !progress {
			return
		}
	}
}

// suspendDependents recursively suspends every source depending on base —
// flags only, the rows stay for reactivation. Best-effort (mirrors the old
// manager): a failure is logged and the walk continues.
func (s *Store) suspendDependents(ctx context.Context, base string, visited map[string]bool) {
	dependents, err := s.dependentsOf(ctx, base)
	if err != nil {
		logReadErr("suspendDependents", err)
		return
	}
	for _, name := range dependents {
		if visited[name] {
			continue
		}
		visited[name] = true
		s.suspendDependents(ctx, name, visited)
		meta, ok, err := s.sourceMeta(ctx, name)
		if err != nil || !ok || meta.Suspended {
			continue
		}
		if err := s.setFlags(ctx, name, meta.Loaded, meta.Disabled, true); err != nil {
			slog.Error("suspend dependent", "catalog", name, "dependency", base, "error", err)
			continue
		}
		slog.Info("dependent catalog suspended", "catalog", name, "dependency", base)
	}
}

// dependentsOf lists sources declaring a dependency on base.
func (s *Store) dependentsOf(ctx context.Context, base string) ([]string, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("catalog dependents of %s: %w", base, err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT data_source FROM core.catalog.data_source_dependencies
		WHERE depends_on = `+lit(base)+` ORDER BY data_source`)
	if err != nil {
		return nil, fmt.Errorf("catalog dependents of %s: %w", base, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("catalog dependents of %s: %w", base, err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// suspendedSources lists the suspended sources from the meta.
func (s *Store) suspendedSources(ctx context.Context) ([]string, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("catalog suspended sources: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT data_source FROM core.catalog.data_source_meta
		WHERE suspended ORDER BY data_source`)
	if err != nil {
		return nil, fmt.Errorf("catalog suspended sources: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("catalog suspended sources: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// depsActive reports whether every named source is present and active.
func (s *Store) depsActive(ctx context.Context, deps []string) (bool, error) {
	if len(deps) == 0 {
		return true, nil
	}
	names := make([]string, 0, len(deps))
	seen := map[string]struct{}{}
	for _, d := range deps {
		if _, ok := seen[d]; ok || d == "" {
			continue
		}
		seen[d] = struct{}{}
		names = append(names, lit(d))
	}
	if len(names) == 0 {
		return true, nil // only empty names — nothing to check (and no IN ())
	}
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("catalog deps check: %w", err)
	}
	defer conn.Close()
	var active int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM core.catalog.data_source_meta
		WHERE data_source IN (`+strings.Join(names, ", ")+`)
		AND loaded AND NOT disabled AND NOT suspended`).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("catalog deps check: %w", err)
	}
	return active == len(names), nil
}

// storedDepsSatisfied reports whether every STORED dependency of the source is
// present and active (the anti-join over the meta).
func (s *Store) storedDepsSatisfied(ctx context.Context, name string) (bool, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("catalog deps of %s: %w", name, err)
	}
	defer conn.Close()
	var unsatisfied int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM core.catalog.data_source_dependencies d
		WHERE d.data_source = `+lit(name)+`
		AND NOT EXISTS (
			SELECT 1 FROM core.catalog.data_source_meta m
			WHERE m.data_source = d.depends_on AND m.loaded AND NOT m.disabled AND NOT m.suspended
		)`).Scan(&unsatisfied)
	if err != nil {
		return false, fmt.Errorf("catalog deps of %s: %w", name, err)
	}
	return unsatisfied == 0, nil
}

// ActiveCatalogs lists the catalogs that should be attached: stored, loaded,
// enabled and not suspended (the cluster.CatalogProvider surface — workers
// reconcile their attached sources against it).
func (s *Store) ActiveCatalogs(ctx context.Context) ([]string, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("catalog active sources: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT data_source FROM core.catalog.data_source_meta
		WHERE loaded AND NOT disabled AND NOT suspended ORDER BY data_source`)
	if err != nil {
		return nil, fmt.Errorf("catalog active sources: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("catalog active sources: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// CleanOrphanedCatalogs removes the stored schema of every source that is no
// longer registered: a metadata row with no core.data_sources row and no live
// handle in this process. Runtime sources have no data_sources row either, so
// the live handles are what keeps them — they are all added before startup
// reaches this point.
func (s *Store) CleanOrphanedCatalogs(ctx context.Context) error {
	if s.isReadonly {
		return ErrReadOnly
	}
	// The read is a separate call so its connection is released by the time the
	// deletions start: deleteSource takes its own connection for the
	// transaction, and holding this one across the loop would pin a second
	// pool slot for the whole cleanup.
	orphans, err := s.orphanedCatalogs(ctx)
	if err != nil {
		return err
	}
	for _, name := range orphans {
		slog.Info("removing orphaned catalog", "catalog", name)
		if err := s.deleteSource(ctx, name); err != nil { // invalidates its own cache
			slog.Error("failed to drop orphaned catalog", "catalog", name, "error", err)
			continue
		}
		s.bumpSchemaVersion(ctx, name)
	}
	return nil
}

// orphanedCatalogs lists the stored sources that are no longer registered: a
// metadata row with no core.data_sources row and no live handle in this
// process.
func (s *Store) orphanedCatalogs(ctx context.Context) ([]string, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("clean orphaned catalogs: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT m.data_source FROM core.catalog.data_source_meta m
		LEFT JOIN core.data_sources ds ON ds.name = m.data_source
		WHERE ds.name IS NULL ORDER BY m.data_source`)
	if err != nil {
		return nil, fmt.Errorf("clean orphaned catalogs: %w", err)
	}
	defer rows.Close()
	var orphans []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("clean orphaned catalogs: %w", err)
		}
		s.catMu.RLock()
		_, live := s.catalogs[name]
		s.catMu.RUnlock()
		if live {
			continue
		}
		orphans = append(orphans, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clean orphaned catalogs: %w", err)
	}
	return orphans, nil
}

// InvalidateAll drops every cached definition — the cluster worker's response
// to a schema-version change (the rows were rewritten by the writer node).
func (s *Store) InvalidateAll() { s.invalidateAll() }

// InvalidateCatalog drops the cached definitions a source contributed to. The
// affected-object closure is not available here (the write happened on another
// node), so the whole source bucket goes, roots included.
func (s *Store) InvalidateCatalog(name string) { s.invalidateSource(name, nil) }

// ClearSourceVersion invalidates a source's STORED version so the next load
// recompiles and rewrites its rows wholesale instead of taking the version gate
// (_schema_version_clean). The gate compares a content hash, so a row set that
// went stale for a reason the hash cannot see — a compiler change, a partial
// write recovered by hand — can only be forced out this way.
func (s *Store) ClearSourceVersion(ctx context.Context, name string) error {
	if s.isReadonly {
		return ErrReadOnly
	}
	if _, ok, err := s.sourceMeta(ctx, name); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("catalog %q not found", name)
	}
	if err := s.exec(ctx, `UPDATE core.catalog.data_source_meta SET version = `+lit("")+
		` WHERE data_source = `+lit(name)); err != nil {
		return fmt.Errorf("clear catalog version %q: %w", name, err)
	}
	slog.Info("catalog version cleared", "catalog", name)
	return nil
}

// sourceState assembles the writer's SourceState from the catalog handle: the
// content version, the compile options that shape stored names and the engine
// capabilities snapshot.
func (s *Store) sourceState(ctx context.Context, name string, cat catsrc.Catalog) (SourceState, error) {
	version, err := cat.Version(ctx)
	if err != nil {
		return SourceState{}, fmt.Errorf("catalog %q version: %w", name, err)
	}
	opts := cat.CompileOptions()
	var caps json.RawMessage
	if opts.Capabilities != nil {
		caps, err = json.Marshal(opts.Capabilities)
		if err != nil {
			return SourceState{}, fmt.Errorf("catalog %q capabilities: %w", name, err)
		}
	}
	return SourceState{
		Name:         name,
		Version:      version,
		Capabilities: caps,
		Engine:       opts.EngineType,
		ReadOnly:     opts.ReadOnly,
		Prefix:       opts.Prefix,
		AsModule:     opts.AsModule,
		IsExtension:  opts.IsExtension,
		Loaded:       true,
	}, nil
}

// sourceMetaRow is the stored load state of one source. Version is the STORED
// composite (SourceState.storedVersion form).
type sourceMetaRow struct {
	Version   string
	Loaded    bool
	Disabled  bool
	Suspended bool
}

// sourceMeta reads a source's stored version and flags (ok=false when the
// source was never written).
func (s *Store) sourceMeta(ctx context.Context, name string) (sourceMetaRow, bool, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return sourceMetaRow{}, false, fmt.Errorf("catalog read meta %s: %w", name, err)
	}
	defer conn.Close()
	var m sourceMetaRow
	err = conn.QueryRow(ctx, `SELECT version, loaded, disabled, suspended
		FROM core.catalog.data_source_meta WHERE data_source = `+lit(name)).
		Scan(&m.Version, &m.Loaded, &m.Disabled, &m.Suspended)
	if err == sql.ErrNoRows {
		return sourceMetaRow{}, false, nil
	}
	if err != nil {
		return sourceMetaRow{}, false, fmt.Errorf("catalog read meta %s: %w", name, err)
	}
	return m, true, nil
}

func (s *Store) putCatalog(name string, cat catsrc.Catalog) {
	s.catMu.Lock()
	s.catalogs[name] = cat
	s.catMu.Unlock()
}

func (s *Store) dropCatalog(name string) {
	s.catMu.Lock()
	delete(s.catalogs, name)
	s.catMu.Unlock()
}

// --- schema version counter (cluster change detection) ---

// settingsTable is the legacy CoreDB key-value table (created by the CoreDB
// bootstrap schema.sql, shared with the old provider) that carries the
// monotonically increasing schema_version counter cluster workers poll.
const settingsTable = "core._schema_settings"

// GetSchemaVersion returns the schema version counter (0 when unset).
func (s *Store) GetSchemaVersion(ctx context.Context) (int64, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("get schema version: %w", err)
	}
	defer conn.Close()
	var version int64
	err = conn.QueryRow(ctx,
		`SELECT CAST(TRIM(CAST(value AS VARCHAR), '"') AS BIGINT) FROM `+settingsTable+` WHERE key = 'schema_version'`).
		Scan(&version)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get schema version: %w", err)
	}
	return version, nil
}

// IncrementSchemaVersion bumps the schema version counter and returns the new
// value. Two-step read + literal write inside one transaction: no in-SQL
// arithmetic over the JSON value column, so the statement shapes hold on both
// a native DuckDB CoreDB and an attached PostgreSQL one. The management node
// is the single writer (same assumption the old provider made).
func (s *Store) IncrementSchemaVersion(ctx context.Context) (int64, error) {
	txCtx, err := s.pool.WithTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("increment schema version: %w", err)
	}
	defer s.pool.Rollback(txCtx)

	conn, err := s.pool.Conn(txCtx)
	if err != nil {
		return 0, fmt.Errorf("increment schema version: %w", err)
	}
	defer conn.Close()

	var cur int64
	err = conn.QueryRow(txCtx,
		`SELECT CAST(TRIM(CAST(value AS VARCHAR), '"') AS BIGINT) FROM `+settingsTable+` WHERE key = 'schema_version'`).
		Scan(&cur)
	switch {
	case err == sql.ErrNoRows:
		// The bootstrap seeds the row; tolerate a missing one anyway.
		if err := s.exec(txCtx, `INSERT INTO `+settingsTable+` (key, value) VALUES ('schema_version', `+lit(`"1"`)+`)`); err != nil {
			return 0, fmt.Errorf("increment schema version: %w", err)
		}
		cur = 0
	case err != nil:
		return 0, fmt.Errorf("increment schema version: %w", err)
	default:
		next := lit(`"` + strconv.FormatInt(cur+1, 10) + `"`)
		if err := s.exec(txCtx, `UPDATE `+settingsTable+` SET value = `+next+` WHERE key = 'schema_version'`); err != nil {
			return 0, fmt.Errorf("increment schema version: %w", err)
		}
	}
	if err := s.pool.Commit(txCtx); err != nil {
		return 0, fmt.Errorf("increment schema version: %w", err)
	}
	return cur + 1, nil
}

// bumpSchemaVersion increments the cluster change counter after a catalog
// mutation; a failure is logged, not propagated — the schema change itself
// already committed.
func (s *Store) bumpSchemaVersion(ctx context.Context, name string) {
	if _, err := s.IncrementSchemaVersion(ctx); err != nil {
		slog.Error("catalog store: increment schema version", "catalog", name, "error", err)
	}
}
