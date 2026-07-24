package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
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
// wholesale in one transaction.
func (s *Store) AddCatalog(ctx context.Context, name string, cat catsrc.Catalog) error {
	if s.isReadonly {
		return ErrReadOnly
	}
	state, err := s.sourceState(ctx, name, cat)
	if err != nil {
		return err
	}
	meta, ok, err := s.sourceMeta(ctx, name)
	if err != nil {
		return err
	}
	if ok && meta.Version == state.storedVersion() {
		// Content and options unchanged — the stored rows are current. Repair
		// visibility only: resume a suspended source, mark it loaded. The
		// disabled flag is an operator decision and survives redeploys.
		if meta.Suspended || !meta.Loaded {
			if err := s.setFlags(ctx, name, true, meta.Disabled, false); err != nil {
				return err
			}
			s.bumpSchemaVersion(ctx, name)
			slog.Info("catalog resumed (version unchanged)", "catalog", name, "version", state.Version)
		}
		s.putCatalog(name, cat)
		return nil
	}
	if ok {
		state.Disabled = meta.Disabled
	}
	if err := s.compileAndWrite(ctx, name, cat, state); err != nil {
		return err
	}
	s.putCatalog(name, cat)
	s.bumpSchemaVersion(ctx, name)
	slog.Info("catalog stored", "catalog", name, "version", state.Version)
	return nil
}

// RemoveCatalog deletes a source's entity rows, metadata, dependencies and
// seed annotations (the unregister primitive) and drops the source handle.
func (s *Store) RemoveCatalog(ctx context.Context, name string) error {
	if s.isReadonly {
		return ErrReadOnly
	}
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
// ForName), collect the physical entities, persist them transactionally.
func (s *Store) compileAndWrite(ctx context.Context, name string, cat catsrc.Catalog, state SourceState) error {
	if _, err := compiler.New(partialRules()...).Compile(ctx, s, cat, cat.CompileOptions()); err != nil {
		return fmt.Errorf("compile catalog %q: %w", name, err)
	}
	d := collect(ctx, asExtensionsSource(cat), name)
	if _, err := s.writeSource(ctx, d, state); err != nil {
		return err
	}
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
