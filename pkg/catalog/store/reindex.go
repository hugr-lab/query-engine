package store

import (
	"context"
	"errors"
	"fmt"
)

// REINDEX recomputes the annotation VECTORS of the stored entities from their
// CURRENT text. It is the maintenance counterpart of the load-time seeding
// (seed.go): seeding embeds an entity once, when its source is written, so a
// vector goes stale whenever the embedder changes (a new model, a re-tuned
// prompt) or a curation was written while the embedder was down. Reindexing is
// therefore an explicit operator action (_schema_reindex), never automatic.
//
// The embedded text is the FULL overlay, in the same order the reader resolves
// a description — curation (long → short) first, then the entity's own SDL
// description, then a synthetic fallback built from the entity's metadata. That
// is why the write clears the seed guard: a curated row's vector SHOULD be
// rewritten here, unlike at seed time where the text lacks the curation.
//
// Scope: an empty name reindexes everything, a source name only that source's
// entities. Purely-generated curations (the gql_* kinds) have no source
// attribution and are reindexed only in the global pass.

const (
	// defaultReindexBatch / maxReindexBatch bound one embed+write round trip;
	// the UDF's batch_size argument is clamped into this range.
	defaultReindexBatch = 50
	maxReindexBatch     = 200
)

// ErrNoEmbeddings is returned by ReindexEmbeddings when the engine runs without
// an embedder — there are no vectors to recompute.
var ErrNoEmbeddings = errors.New("embeddings not configured (EMBEDDER_URL not set)")

// reindexTarget is one annotation row to re-embed: its identity plus the text.
type reindexTarget struct {
	kind   string
	key    string
	parent string
	text   string
}

// ReindexEmbeddings recomputes the embedding vector of every entity in scope
// (empty dataSource = all) and returns how many rows were rewritten. Embedding
// happens in batches OUTSIDE any transaction — each batch's vectors are written
// as they are computed, so a failure part-way leaves the already-processed rows
// updated (the count reports how far it got) instead of losing the whole run.
func (s *Store) ReindexEmbeddings(ctx context.Context, dataSource string, batchSize int) (int, error) {
	if s.isReadonly {
		return 0, ErrReadOnly
	}
	if s.embedder == nil || s.vecSize == 0 {
		return 0, ErrNoEmbeddings
	}
	switch {
	case batchSize <= 0:
		batchSize = defaultReindexBatch
	case batchSize > maxReindexBatch:
		batchSize = maxReindexBatch
	}
	targets, err := s.reindexTargets(ctx, dataSource)
	if err != nil {
		return 0, err
	}
	total := 0
	for start := 0; start < len(targets); start += batchSize {
		end := min(start+batchSize, len(targets))
		chunk := targets[start:end]
		texts := make([]string, len(chunk))
		for i, t := range chunk {
			texts[i] = t.text
		}
		vecs, err := s.embedBatch(ctx, texts)
		if err != nil {
			return total, fmt.Errorf("catalog reindex: %w", err)
		}
		if len(vecs) != len(chunk) {
			return total, fmt.Errorf("catalog reindex: embedder returned %d vectors for %d entities",
				len(vecs), len(chunk))
		}
		rows := make([]seedRow, len(chunk))
		for i, t := range chunk {
			rows[i] = seedRow{kind: t.kind, key: t.key, parent: t.parent, vec: vecs[i]}
		}
		if err := s.upsertVectors(ctx, "catalog reindex", rows, false); err != nil {
			return total, err
		}
		total += len(chunk)
	}
	return total, nil
}

// reindexTargets enumerates the annotation rows to re-embed, deduplicated by
// (kind, key) — the same identity may be reached twice (a field row and a
// relation's navigation field), and a repeated conflict target would fail the
// upsert. Order follows the enumeration order, so the first source of a text
// wins exactly as it does at seed time (collectSeedEntities).
func (s *Store) reindexTargets(ctx context.Context, dataSource string) ([]reindexTarget, error) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("catalog reindex targets: %w", err)
	}
	defer conn.Close()

	seen := map[string]struct{}{}
	var out []reindexTarget
	for _, stmt := range reindexQueries(dataSource) {
		rows, err := conn.Query(ctx, stmt)
		if err != nil {
			return nil, fmt.Errorf("catalog reindex targets: %w", err)
		}
		for rows.Next() {
			var r reindexRow
			if err := rows.Scan(&r.kind, &r.key, &r.parent, &r.curDesc, &r.curLongDesc,
				&r.entityDesc, &r.synType, &r.synName, &r.synParent, &r.synModule, &r.synCatalog); err != nil {
				rows.Close()
				return nil, fmt.Errorf("catalog reindex targets: %w", err)
			}
			id := r.kind + "\x1f" + r.key
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, reindexTarget{kind: r.kind, key: r.key, parent: r.parent, text: r.text()})
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("catalog reindex targets: %w", err)
		}
	}
	return out, nil
}

// reindexRow is the uniform projection every enumeration query produces: the
// annotation identity, the curated texts, the entity's own SDL description and
// the five syntheticDescription parts. Nothing is nullable — the queries
// COALESCE, so one scanner serves every kind.
type reindexRow struct {
	kind, key, parent           string
	curDesc, curLongDesc        string
	entityDesc                  string
	synType, synName, synParent string
	synModule, synCatalog       string
}

// text layers the row exactly like the reader resolves a description: curation
// first (long, then short), then the SDL description, then the synthetic
// fallback — so a reindexed vector matches what the curation and seed paths
// would have written for the same entity.
func (r reindexRow) text() string {
	return embeddingText(r.curLongDesc, r.curDesc,
		embeddingText("", r.entityDesc,
			syntheticDescription(r.synType, r.synName, r.synParent, r.synModule, r.synCatalog)))
}

// reindexQueries builds the per-kind enumeration statements for a scope. Each
// LEFT JOINs the curation onto the entity rows (the overlay read shape the
// entity_* views use) and projects the reindexRow columns, with the constant
// parts as literals so a single scanner serves all of them.
func reindexQueries(dataSource string) []string {
	// join correlates the curation by the kind's entity_key expression. Both
	// sides are EXPRESSIONS: every kind but the functions passes a literal, the
	// functions correlate on their own kind column (the kind is part of a
	// function's annotation identity).
	join := func(kindExpr, keyExpr string) string {
		return ` LEFT JOIN core.catalog.annotations a
			ON a.entity_kind = ` + kindExpr + ` AND a.entity_key = ` + keyExpr
	}
	// curated projects the two overlay text columns (absent row → empty).
	const curated = `coalesce(a.description, ''), coalesce(a.long_description, '')`
	// scoped appends a source predicate; an empty scope selects everything.
	scoped := func(where, column string) string {
		if dataSource == "" {
			return where
		}
		if where == "" {
			return " WHERE " + column + " = " + lit(dataSource)
		}
		return where + " AND " + column + " = " + lit(dataSource)
	}

	const fieldKeyExpr = `f.type_name || '.' || f.name`
	const functionKeyExpr = `CASE WHEN fn.module = '' THEN fn.name ELSE fn.module || '.' || fn.name END`
	const srcFieldKeyExpr = `r.source || '.' || r.source_field`
	const dstFieldKeyExpr = `r.destination || '.' || r.destination_field`

	queries := []string{
		// Data sources.
		`SELECT ` + lit(kindDataSource) + `, m.data_source, '', ` + curated + `, '',
			'catalog', m.data_source, '', '', ''
		FROM core.catalog.data_source_meta m` +
			join(lit(kindDataSource), `m.data_source`) +
			scoped("", "m.data_source"),

		// Modules: shared across sources, so a scoped run takes the modules the
		// source contributes to (the closure the writer maintains).
		`SELECT ` + lit(kindModule) + `, m.name, coalesce(m.parent, ''), ` + curated + `, coalesce(m.description, ''),
			'module', m.name, '', '', ''
		FROM core.catalog.modules m` +
			join(lit(kindModule), `m.name`) +
			moduleScope(dataSource),

		// Data objects.
		`SELECT ` + lit(kindDataObject) + `, o.name, '', ` + curated + `, coalesce(o.description, ''),
			'', o.name, '', o.module, o.data_source
		FROM core.catalog.data_objects o` +
			join(lit(kindDataObject), `o.name`) +
			scoped("", "o.data_source"),

		// Object fields (columns and declared virtual fields).
		`SELECT ` + lit(kindField) + `, ` + fieldKeyExpr + `, f.type_name, ` + curated + `, coalesce(f.description, ''),
			'', f.name, f.type_name, '', f.data_source
		FROM core.catalog.fields f` +
			join(lit(kindField), fieldKeyExpr) +
			scoped("", "f.data_source"),

		// Relation navigation fields, both legs (generated fields with no row in
		// catalog.fields — their SDL default lives on the relation).
		`SELECT ` + lit(kindField) + `, ` + srcFieldKeyExpr + `, r.source, ` + curated + `,
			coalesce(r.source_field_description, ''),
			'', r.source_field, r.source, '', r.data_source
		FROM core.catalog.relations r` +
			join(lit(kindField), srcFieldKeyExpr) +
			scoped(` WHERE r.source_field IS NOT NULL AND r.source_field <> ''`, "r.data_source"),

		`SELECT ` + lit(kindField) + `, ` + dstFieldKeyExpr + `, r.destination, ` + curated + `,
			coalesce(r.destination_field_description, ''),
			'', r.destination_field, r.destination, '', r.data_source
		FROM core.catalog.relations r` +
			join(lit(kindField), dstFieldKeyExpr) +
			scoped(` WHERE r.destination_field IS NOT NULL AND r.destination_field <> ''`, "r.data_source"),

		// Functions: keyed module.name UNDER THEIR KIND (parent = module), so the
		// annotation kind is projected and joined from the column — the same name
		// in two root namespaces re-embeds as two rows.
		`SELECT fn.kind, ` + functionKeyExpr + `, fn.module, ` + curated + `, coalesce(fn.description, ''),
			'', fn.name, '', fn.module, fn.data_source
		FROM core.catalog.functions fn` +
			join(`fn.kind`, functionKeyExpr) +
			scoped("", "fn.data_source"),

		// Residual source types.
		`SELECT ` + lit(kindType) + `, t.name, '', ` + curated + `, coalesce(t.description, ''),
			'', t.name, '', t.module, t.data_source
		FROM core.catalog.types t` +
			join(lit(kindType), `t.name`) +
			scoped("", "t.data_source"),
	}

	if dataSource != "" {
		return queries
	}
	// Curations of the GENERATED surface have no entity row and no source
	// attribution — they carry their own text and are reindexed globally only.
	return append(queries, `SELECT a.entity_kind, a.entity_key, coalesce(a.parent, ''),
			coalesce(a.description, ''), coalesce(a.long_description, ''), '', '', '', '', '', ''
		FROM core.catalog.annotations a
		WHERE a.entity_kind IN (`+lit(kindGQLType)+`, `+lit(kindGQLField)+`, `+lit(kindGQLArgument)+`)
		AND (a.description IS NOT NULL OR a.long_description IS NOT NULL)`)
}

// moduleScope restricts the module enumeration to the modules a source
// contributes to (module_data_sources is the writer-maintained closure).
func moduleScope(dataSource string) string {
	if dataSource == "" {
		return ""
	}
	return ` WHERE EXISTS (SELECT 1 FROM core.catalog.module_data_sources ms
		WHERE ms.module = m.name AND ms.data_source = ` + lit(dataSource) + `)`
}
