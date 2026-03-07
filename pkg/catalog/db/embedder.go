package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/types"
)

// Embedder is an optional dependency for computing embedding vectors.
// Defined in this package to avoid import cycles.
// The engine wires it to an EmbeddingSource at init time.
type Embedder interface {
	CreateEmbedding(ctx context.Context, input string) (types.Vector, error)
	CreateEmbeddings(ctx context.Context, inputs []string) ([]types.Vector, error)
}

// computeEmbeddings computes embedding vectors for a batch of texts.
// Returns zero-length vectors for each input if the embedder is nil or vecSize is 0.
func (p *Provider) computeEmbeddings(ctx context.Context, texts []string) ([]types.Vector, error) {
	if p.embedder == nil || p.vecSize == 0 || len(texts) == 0 {
		return make([]types.Vector, len(texts)), nil
	}
	return p.embedder.CreateEmbeddings(ctx, texts)
}

// EmbeddingText returns the best available text for embedding generation.
// Falls back through: longDesc → desc → syntheticDesc.
func EmbeddingText(longDesc, desc, syntheticDesc string) string {
	if longDesc != "" {
		return longDesc
	}
	if desc != "" {
		return desc
	}
	return syntheticDesc
}

// ReindexEmbeddings recomputes embedding vectors for schema entities.
// If name is empty, all entities are reindexed. Otherwise, only entities
// from the specified catalog are reindexed.
// Returns the number of entities reindexed.
func (p *Provider) ReindexEmbeddings(ctx context.Context, name string, batchSize int) (int, error) {
	if p.isReadonly {
		return 0, ErrReadOnly
	}
	if p.embedder == nil || p.vecSize == 0 {
		return 0, fmt.Errorf("embeddings not configured")
	}

	total := 0

	// Reindex types
	n, err := p.reindexTable(ctx, "_schema_types", "name", name, batchSize,
		func(name, desc, longDesc string) string {
			return EmbeddingText(longDesc, desc, SyntheticDescription("", name, "", "", ""))
		})
	if err != nil {
		return total, fmt.Errorf("reindex types: %w", err)
	}
	total += n

	// Reindex fields
	n, err = p.reindexFields(ctx, name, batchSize)
	if err != nil {
		return total, fmt.Errorf("reindex fields: %w", err)
	}
	total += n

	// Reindex modules
	n, err = p.reindexTable(ctx, "_schema_modules", "name", "", batchSize,
		func(name, desc, longDesc string) string {
			return EmbeddingText(longDesc, desc, SyntheticDescription("module", name, "", "", ""))
		})
	if err != nil {
		return total, fmt.Errorf("reindex modules: %w", err)
	}
	total += n

	// Reindex catalogs
	n, err = p.reindexTable(ctx, "_schema_catalogs", "name", name, batchSize,
		func(name, desc, longDesc string) string {
			return EmbeddingText(longDesc, desc, SyntheticDescription("catalog", name, "", "", ""))
		})
	if err != nil {
		return total, fmt.Errorf("reindex catalogs: %w", err)
	}
	total += n

	return total, nil
}

// reindexTable recomputes embeddings for a simple table with name/description/long_description columns.
func (p *Provider) reindexTable(ctx context.Context, table, pkCol, filterCatalog string, batchSize int, textFn func(name, desc, longDesc string) string) (int, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	query := fmt.Sprintf("SELECT %s, description, long_description FROM %s", pkCol, p.table(table))
	args := []any{}
	if filterCatalog != "" {
		query += " WHERE name = $1"
		args = append(args, filterCatalog)
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	var batch []entity
	total := 0

	for rows.Next() {
		var name, desc, longDesc string
		if err := rows.Scan(&name, &desc, &longDesc); err != nil {
			continue
		}
		batch = append(batch, entity{pk: name, text: textFn(name, desc, longDesc)})

		if len(batch) >= batchSize {
			n, err := p.flushEmbeddings(ctx, table, pkCol, batch)
			if err != nil {
				_ = rows.Close()
				return total, err
			}
			total += n
			batch = batch[:0]
		}
	}
	_ = rows.Close()

	if len(batch) > 0 {
		n, err := p.flushEmbeddings(ctx, table, pkCol, batch)
		if err != nil {
			return total, err
		}
		total += n
	}

	return total, nil
}

// reindexFields recomputes embeddings for _schema_fields (composite PK: type_name + name).
func (p *Provider) reindexFields(ctx context.Context, filterCatalog string, batchSize int) (int, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	query := fmt.Sprintf("SELECT type_name, name, description, long_description FROM %s", p.table("_schema_fields"))
	args := []any{}
	if filterCatalog != "" {
		query += " WHERE catalog = $1"
		args = append(args, filterCatalog)
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	var batch []fieldEntity
	total := 0

	for rows.Next() {
		var typeName, name, desc, longDesc string
		if err := rows.Scan(&typeName, &name, &desc, &longDesc); err != nil {
			continue
		}
		text := EmbeddingText(longDesc, desc, SyntheticDescription("", name, typeName, "", ""))
		batch = append(batch, fieldEntity{typeName: typeName, name: name, text: text})

		if len(batch) >= batchSize {
			n, err := p.flushFieldEmbeddings(ctx, batch)
			if err != nil {
				_ = rows.Close()
				return total, err
			}
			total += n
			batch = batch[:0]
		}
	}
	_ = rows.Close()

	if len(batch) > 0 {
		n, err := p.flushFieldEmbeddings(ctx, batch)
		if err != nil {
			return total, err
		}
		total += n
	}

	return total, nil
}

type entity struct {
	pk   string
	text string
}

func (p *Provider) flushEmbeddings(ctx context.Context, table, pkCol string, batch []entity) (int, error) {
	texts := make([]string, len(batch))
	for i, e := range batch {
		texts[i] = e.text
	}
	vecs, err := p.computeEmbeddings(ctx, texts)
	if err != nil {
		return 0, err
	}

	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	for i, e := range batch {
		_, err := p.execWrite(ctx, conn, fmt.Sprintf(
			"UPDATE %s SET vec = $2 WHERE %s = $1", p.table(table), pkCol,
		), e.pk, vecs[i])
		if err != nil {
			return i, err
		}
	}
	return len(batch), nil
}

type fieldEntity struct {
	typeName string
	name     string
	text     string
}

func (p *Provider) flushFieldEmbeddings(ctx context.Context, batch []fieldEntity) (int, error) {
	texts := make([]string, len(batch))
	for i, e := range batch {
		texts[i] = e.text
	}
	vecs, err := p.computeEmbeddings(ctx, texts)
	if err != nil {
		return 0, err
	}

	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	for i, e := range batch {
		_, err := p.execWrite(ctx, conn, fmt.Sprintf(
			"UPDATE %s SET vec = $3 WHERE type_name = $1 AND name = $2", p.table("_schema_fields"),
		), e.typeName, e.name, vecs[i])
		if err != nil {
			return i, err
		}
	}
	return len(batch), nil
}

// SyntheticDescription builds a fallback description from entity metadata.
// Used when no human-written description is available.
func SyntheticDescription(hugrType, entityName, parentName, moduleName, catalog string) string {
	var parts []string
	if hugrType != "" {
		parts = append(parts, hugrType)
	}
	parts = append(parts, entityName)
	if parentName != "" {
		parts = append(parts, fmt.Sprintf("on %s", parentName))
	}
	if moduleName != "" {
		parts = append(parts, fmt.Sprintf("in module %s", moduleName))
	}
	if catalog != "" {
		parts = append(parts, fmt.Sprintf("from catalog %s", catalog))
	}
	return strings.Join(parts, " ")
}
