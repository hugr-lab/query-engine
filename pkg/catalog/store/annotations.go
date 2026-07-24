package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

// The annotation overlay is the curation layer applied at the end of ForName,
// AFTER the default descriptions: a row in catalog.annotations overrides the
// description of a type, field or argument by name. Identity is
// (entity_kind, entity_key) and the entity_key is a dot-path locating the
// target (hugr_catalog.sql D-notes): a type is keyed by its name, a field by
// "<type>.<field>", an argument by "<type>.<field>.<arg>". The overlay is
// source-agnostic (never activity-gated) and orphan rows are legal; a row with
// a NULL description is a vector seed (D24) and leaves the generated text
// intact. This is the single key convention shared with the SchemaAnnotator
// writer.

const (
	annotationType     = "type"
	annotationField    = "field"
	annotationArgument = "argument"
)

// applyAnnotations overrides def's descriptions — the type itself, its fields
// and their arguments — from catalog.annotations. One query loads the type row
// and every field/argument row beneath it (entity_key prefix); rows are routed
// by entity_kind and located by their dot-path key.
func (s *Store) applyAnnotations(ctx context.Context, g *genContext, def *ast.Definition) error {
	rows, err := g.readAnnotations(ctx, def.Name)
	if err != nil {
		return err
	}
	prefix := def.Name + "."
	for _, a := range rows {
		switch a.kind {
		case annotationType:
			def.Description = a.description
		case annotationField:
			if f := def.Fields.ForName(strings.TrimPrefix(a.key, prefix)); f != nil {
				f.Description = a.description
			}
		case annotationArgument:
			fieldName, argName, ok := strings.Cut(strings.TrimPrefix(a.key, prefix), ".")
			if !ok {
				continue
			}
			if f := def.Fields.ForName(fieldName); f != nil {
				if arg := f.Arguments.ForName(argName); arg != nil {
					arg.Description = a.description
				}
			}
		}
	}
	return nil
}

// annotationRow is one curation row that carries text (seeds are skipped).
type annotationRow struct {
	kind        string
	key         string
	description string
}

// readAnnotations loads the type/field/argument curation rows for one type:
// the type row (entity_key = typeName) and everything beneath it (entity_key
// starts with "typeName."). Seed rows (NULL description) are dropped.
func (g *genContext) readAnnotations(ctx context.Context, typeName string) ([]annotationRow, error) {
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT entity_kind, entity_key, description
		FROM core.catalog.annotations
		WHERE entity_kind IN (`+lit(annotationType)+`, `+lit(annotationField)+`, `+lit(annotationArgument)+`)
		  AND (entity_key = `+lit(typeName)+` OR starts_with(entity_key, `+lit(typeName+".")+`))`)
	if err != nil {
		return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
	}
	defer rows.Close()
	var out []annotationRow
	for rows.Next() {
		var kind, key string
		var desc sql.NullString
		if err := rows.Scan(&kind, &key, &desc); err != nil {
			return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
		}
		if !desc.Valid {
			continue // seed row — no curated text
		}
		out = append(out, annotationRow{kind: kind, key: key, description: desc.String})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read annotations %s: %w", typeName, err)
	}
	return out, nil
}
