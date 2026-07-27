package store

import (
	"context"
	"fmt"
	"strings"

	qetypes "github.com/hugr-lab/query-engine/types"
)

// Annotation SEEDING (D24) makes every logical entity of a source vector-
// searchable the moment its schema loads: on writeSource the store embeds each
// entity's description (or a synthetic fallback) and writes a vec-only
// annotation row — text columns stay NULL, marking it a SEED. It NEVER touches a
// curated row (description / long_description set), so curation always wins and
// survives reloads. Two write shapes:
//   - MODULES are INSERT-ONLY (anti-join): a module's description lives ONLY in
//     the curation (the source SDL carries none), so an existing module row —
//     seed or curated — is never rewritten, not even to refresh its vector.
//   - everything else refreshes its seed vector on reload (conditional upsert),
//     since its description comes from the SDL and can change.
//
// The mirror is a SWEEP on deleteSource (the unregister primitive — NOT unload,
// which only flips flags and keeps the rows): the source's seed rows are
// removed, curated rows are kept as tolerated orphans. Both statement shapes
// are the ones the CoreDB inventory pins as cross-engine
// (core-db/hugr_catalog_test.go probe E2 / F2).

// seedEntity is one entity to seed: its annotation identity plus the text to
// embed into the seed vector.
type seedEntity struct {
	kind   string
	key    string
	parent string
	text   string
}

// collectSeedEntities enumerates the logical entities of a written source — data
// source, modules, data objects, fields (columns + relation navigation fields),
// functions and residual types — each with the text to embed. Keyed by
// (kind, key) so a duplicate never reaches the multi-row upsert (which would
// fail on a repeated conflict target).
func collectSeedEntities(d *desired, dataSource string) []seedEntity {
	seen := map[string]struct{}{}
	var out []seedEntity
	add := func(kind, key, parent, text string) {
		id := kind + "\x1f" + key
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, seedEntity{kind: kind, key: key, parent: parent, text: text})
	}

	add(kindDataSource, dataSource, "", syntheticDescription("catalog", dataSource, "", "", ""))
	for _, m := range d.modules {
		add(kindModule, m.Name, m.Parent,
			embeddingText("", m.Description, syntheticDescription("module", m.Name, "", "", "")))
	}
	for _, o := range d.dataObjects {
		add(kindDataObject, o.Name, "",
			embeddingText("", o.Description, syntheticDescription("", o.Name, "", o.Module, o.DataSource)))
	}
	for _, f := range d.fields {
		add(kindField, fieldKey(f.TypeName, f.Name), f.TypeName,
			embeddingText("", f.Description, syntheticDescription("", f.Name, f.TypeName, "", f.DataSource)))
	}
	for _, r := range d.relations {
		if r.SourceField != "" {
			add(kindField, fieldKey(r.Source, r.SourceField), r.Source,
				embeddingText("", r.SourceFieldDescription,
					syntheticDescription("", r.SourceField, r.Source, "", r.DataSource)))
		}
		if r.DestinationField != "" {
			add(kindField, fieldKey(r.Destination, r.DestinationField), r.Destination,
				embeddingText("", r.DestinationFieldDescription,
					syntheticDescription("", r.DestinationField, r.Destination, "", r.DataSource)))
		}
	}
	for _, fn := range d.functions {
		add(kindFunction, functionKey(fn.Module, fn.Name), fn.Module,
			embeddingText("", fn.Description, syntheticDescription("", fn.Name, "", fn.Module, fn.DataSource)))
	}
	for _, t := range d.types {
		add(kindType, t.Name, "",
			embeddingText("", t.Description, syntheticDescription("", t.Name, "", t.Module, t.DataSource)))
	}
	return out
}

// seedRow is one embedded entity ready to persist: its annotation identity plus
// the computed vector.
type seedRow struct {
	kind   string
	key    string
	parent string
	vec    qetypes.Vector
}

// computeSeeds embeds every logical entity of a written source, OUTSIDE any
// transaction — the embedder is a network call and must not be made while a DB
// transaction holds locks (on an attached PostgreSQL CoreDB that would pin the
// remote transaction for the whole round-trip). Returns nil when embeddings are
// not configured. The caller treats an error as best-effort: it logs and writes
// the source WITHOUT seeds, so an embedder outage never blocks schema loading
// (the entity rows still carry the descriptions the overlay COALESCEs).
func (s *Store) computeSeeds(ctx context.Context, d *desired, state SourceState) ([]seedRow, error) {
	if s.embedder == nil || s.vecSize == 0 {
		return nil, nil
	}
	ents := collectSeedEntities(d, state.Name)
	if len(ents) == 0 {
		return nil, nil
	}
	texts := make([]string, len(ents))
	for i, e := range ents {
		texts[i] = e.text
	}
	vecs, err := s.embedBatch(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(vecs) != len(ents) {
		return nil, fmt.Errorf("catalog seed %s: embedder returned %d vectors for %d entities",
			state.Name, len(vecs), len(ents))
	}
	rows := make([]seedRow, len(ents))
	for i, e := range ents {
		rows[i] = seedRow{kind: e.kind, key: e.key, parent: e.parent, vec: vecs[i]}
	}
	return rows, nil
}

// writeSeeds persists the pre-computed seed rows INSIDE the write transaction
// (ctx carries it — every helper takes the transaction's connection from the
// pool), so the seeds commit atomically with the entity rows. Modules are
// INSERT-ONLY: a module's description lives ONLY in the annotation curation (the
// source SDL carries none), so the seed must never touch an existing module row
// — not even to refresh a vector. The rest refresh their seed vector, since
// their description comes from the SDL and can change. Both statement shapes are
// the CoreDB-pinned cross-engine ones (probe E3 anti-join / E2 conditional
// upsert).
func (s *Store) writeSeeds(ctx context.Context, source string, rows []seedRow) error {
	refresh := make([]seedRow, 0, len(rows))
	for _, r := range rows {
		if r.kind == kindModule {
			if err := s.insertSeedIfAbsent(ctx, r); err != nil {
				return fmt.Errorf("catalog seed %s: %w", source, err)
			}
			continue
		}
		refresh = append(refresh, r)
	}
	return s.upsertVectors(ctx, "catalog seed "+source, refresh, true)
}

// insertSeedIfAbsent inserts a vec-only seed row ONLY when no row for the key
// exists — the module write path, which must never update an existing (seed or
// curated) row. The anti-join is the cross-engine seed shape the CoreDB
// inventory pins (probe E3).
func (s *Store) insertSeedIfAbsent(ctx context.Context, r seedRow) error {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec(ctx, `INSERT INTO core.catalog.annotations (entity_kind, entity_key, parent, vec, updated_at)
		SELECT `+lit(r.kind)+`, `+lit(r.key)+`, `+lit(nilIfEmpty(r.parent))+`, `+vecLiteral(r.vec)+`, CURRENT_TIMESTAMP
		WHERE NOT EXISTS (SELECT 1 FROM core.catalog.annotations a
			WHERE a.entity_kind = `+lit(r.kind)+` AND a.entity_key = `+lit(r.key)+`)`)
	return err
}

// upsertVectors writes vec-only annotation rows in chunks: create the row (text
// NULL) or refresh an existing row's vector. seedsOnly guards the update with
// "uncurated rows only" — the load-time seed path, whose text comes from the
// SDL and must never overwrite a curation's vector (probe E2). The reindex path
// clears the guard: its text is the FULL overlay (curation first), so refreshing
// a curated row's vector is the point (probe E5). Rows must be unique by
// (kind, key) — a repeated conflict target fails the statement on both backends.
func (s *Store) upsertVectors(ctx context.Context, what string, rows []seedRow, seedsOnly bool) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	defer conn.Close()
	tail := ` ON CONFLICT (entity_kind, entity_key) DO UPDATE SET vec = EXCLUDED.vec,
		updated_at = EXCLUDED.updated_at`
	if seedsOnly {
		tail += ` WHERE description IS NULL AND long_description IS NULL`
	}
	head := `INSERT INTO core.catalog.annotations (entity_kind, entity_key, parent, vec, updated_at) VALUES `
	for start := 0; start < len(rows); start += insertChunk {
		end := min(start+insertChunk, len(rows))
		var b strings.Builder
		b.WriteString(head)
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(", ")
			}
			r := rows[i]
			fmt.Fprintf(&b, "(%s, %s, %s, %s, CURRENT_TIMESTAMP)",
				lit(r.kind), lit(r.key), lit(nilIfEmpty(r.parent)), vecLiteral(r.vec))
		}
		b.WriteString(tail)
		if _, err := conn.Exec(ctx, b.String()); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
	}
	return nil
}

// sweepAnnotationSeeds removes the SEED annotation rows of a source's own
// logical entities on unregister — curated rows (description / long_description
// set) are kept. It must run BEFORE deleteSourceRows, while the entity rows are
// still present to resolve the keys (ctx carries the transaction). Modules are
// shared across sources and are not swept here (a truly orphaned module seed is
// harmless — the module row is gone, so it never surfaces through
// entity_modules).
func (s *Store) sweepAnnotationSeeds(ctx context.Context, dataSource string) error {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("catalog sweep %s: %w", dataSource, err)
	}
	defer conn.Close()
	ds := lit(dataSource)
	seedOnly := ` AND description IS NULL AND long_description IS NULL`
	stmts := []string{
		`DELETE FROM core.catalog.annotations WHERE entity_kind = ` + lit(kindDataSource) +
			` AND entity_key = ` + ds + seedOnly,
		`DELETE FROM core.catalog.annotations WHERE entity_kind = ` + lit(kindDataObject) +
			` AND entity_key IN (SELECT name FROM core.catalog.data_objects WHERE data_source = ` + ds + `)` + seedOnly,
		`DELETE FROM core.catalog.annotations WHERE entity_kind = ` + lit(kindType) +
			` AND entity_key IN (SELECT name FROM core.catalog.types WHERE data_source = ` + ds + `)` + seedOnly,
		// Fields the source DECLARED (mirrors deleteSourceRows: an extension's
		// virtual field is attributed to the source whose data it reads).
		`DELETE FROM core.catalog.annotations WHERE entity_kind = ` + lit(kindField) +
			` AND entity_key IN (SELECT type_name || '.' || name FROM core.catalog.fields
				WHERE COALESCE(dependency_data_source, data_source) = ` + ds + `)` + seedOnly,
		`DELETE FROM core.catalog.annotations WHERE entity_kind = ` + lit(kindField) +
			` AND entity_key IN (
				SELECT source || '.' || source_field FROM core.catalog.relations
					WHERE data_source = ` + ds + ` AND source_field IS NOT NULL AND source_field <> ''
				UNION
				SELECT destination || '.' || destination_field FROM core.catalog.relations
					WHERE data_source = ` + ds + ` AND destination_field IS NOT NULL AND destination_field <> '')` + seedOnly,
		`DELETE FROM core.catalog.annotations WHERE entity_kind = ` + lit(kindFunction) +
			` AND entity_key IN (SELECT CASE WHEN module = '' THEN name ELSE module || '.' || name END
				FROM core.catalog.functions WHERE data_source = ` + ds + `)` + seedOnly,
	}
	for _, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("catalog sweep %s: %w", dataSource, err)
		}
	}
	return nil
}
