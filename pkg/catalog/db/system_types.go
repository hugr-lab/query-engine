package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"sort"

	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/vektah/gqlparser/v2/ast"
)

// SystemCatalogName is the catalog name used for system types (scalars, directives).
// Using a named catalog instead of empty string prevents accidental collisions
// with types that don't specify a catalog.
const SystemCatalogName = "_system"

// computeSystemVersion computes a deterministic hash from compiled system
// type definitions. The hash changes when system types change between releases.
func computeSystemVersion(ctx context.Context, prov *static.Provider) string {
	h := sha256.New()

	// Collect sorted definition names + kinds for determinism.
	var names []string
	for def := range prov.Definitions(ctx) {
		names = append(names, def.Name+":"+string(def.Kind))
	}
	sort.Strings(names)
	for _, n := range names {
		h.Write([]byte(n))
	}

	// Include directive names and their argument signatures.
	type dirEntry struct {
		name string
		args string
	}
	var dirEntries []dirEntry
	for name, dir := range prov.DirectiveDefinitions(ctx) {
		argSig := ""
		for _, arg := range dir.Arguments {
			argSig += arg.Name + ":" + arg.Type.String() + ";"
		}
		dirEntries = append(dirEntries, dirEntry{name: name, args: argSig})
	}
	sort.Slice(dirEntries, func(i, j int) bool {
		return dirEntries[i].name < dirEntries[j].name
	})
	for _, e := range dirEntries {
		h.Write([]byte(e.name))
		h.Write([]byte(e.args))
	}

	return fmt.Sprintf("sys-%x", h.Sum(nil)[:8])
}

// InitSystemTypes persists system scalars and directives (with catalog="")
// to the _schema_* tables. The version is checked against _schema_settings
// to skip re-persisting if nothing changed.
//
// Called during engine bootstrap to ensure the DB provider can resolve
// basic types like Int, String, Boolean, etc.
func (p *Provider) InitSystemTypes(ctx context.Context) error {
	// Create static provider to get all system definitions.
	staticProv, err := static.New()
	if err != nil {
		return fmt.Errorf("init system types: %w", err)
	}

	version := computeSystemVersion(ctx, staticProv)

	// Check stored version.
	storedVersion, err := p.getSettingString(ctx, "system_types_version")
	if err != nil {
		return fmt.Errorf("init system types: %w", err)
	}

	if storedVersion == version {
		slog.Debug("system types up to date", "version", version)
		return nil
	}

	// Wrap as DefinitionsSource and persist with the system catalog name.
	sysSource := &systemTypesSource{prov: staticProv, ctx: ctx}
	if err := p.UpdateWithCatalog(ctx, sysSource, SystemCatalogName); err != nil {
		return fmt.Errorf("persist system types: %w", err)
	}

	// Store version.
	if err := p.setSettingString(ctx, "system_types_version", version); err != nil {
		return fmt.Errorf("store system types version: %w", err)
	}

	// Mark as initialized.
	if err := p.setSettingString(ctx, "initialized", "true"); err != nil {
		return fmt.Errorf("mark initialized: %w", err)
	}

	slog.Info("system types initialized", "version", version)
	return nil
}

// IsInitialized checks whether the DB has been initialized with system types.
func (p *Provider) IsInitialized(ctx context.Context) (bool, error) {
	val, err := p.getSettingString(ctx, "initialized")
	if err != nil {
		return false, err
	}
	return val == "true", nil
}

// systemTypesSource wraps a static.Provider to provide system type definitions
// as a DefinitionsSource for persisting to the DB.
type systemTypesSource struct {
	prov *static.Provider
	ctx  context.Context
}

func (s *systemTypesSource) ForName(ctx context.Context, name string) *ast.Definition {
	return s.prov.ForName(ctx, name)
}

func (s *systemTypesSource) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return s.prov.DirectiveForName(ctx, name)
}

func (s *systemTypesSource) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return s.prov.Definitions(s.ctx)
}

func (s *systemTypesSource) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return s.prov.DirectiveDefinitions(s.ctx)
}

// --- Settings helpers for string values ---

// getSettingString reads a string value from _schema_settings by key.
// Returns empty string if key does not exist.
func (p *Provider) getSettingString(ctx context.Context, key string) (string, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	var raw string
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT CAST(value AS VARCHAR) FROM %s WHERE key = $1`,
		p.table("_schema_settings"),
	), key).Scan(&raw)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get setting %q: %w", key, err)
	}

	// Value is stored as JSON string, so unquote it.
	var val string
	if err := json.Unmarshal([]byte(raw), &val); err != nil {
		// Fallback: return raw if not valid JSON string.
		return raw, nil
	}
	return val, nil
}

// setSettingString writes a string value to _schema_settings.
func (p *Provider) setSettingString(ctx context.Context, key, value string) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal setting: %w", err)
	}

	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2`,
		p.table("_schema_settings"),
	), key, string(data))
	return err
}
