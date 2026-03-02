package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/db/schema"
	"github.com/hugr-lab/query-engine/pkg/types"
)

// Update persists compiled schema changes to the database.
//
// Processes definitions (@drop, @replace, @if_not_exists, regular add)
// and extensions (field add/drop/replace, directive changes) within
// a single transaction. Computes hugr_type and embeddings. Reconciles
// module and data object metadata. Invalidates cache after commit.
func (p *Provider) Update(ctx context.Context, changes base.DefinitionsSource) error {
	// Extract catalog name from the first definition with a @catalog directive
	catalogName := ""
	for def := range changes.Definitions(ctx) {
		catalogName = base.DefinitionCatalog(def)
		if catalogName != "" {
			break
		}
	}

	// Begin transaction
	txCtx, err := p.pool.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("update: begin tx: %w", err)
	}
	defer p.pool.Rollback(txCtx)

	// Upsert catalog record
	if catalogName != "" {
		if err := p.upsertCatalog(txCtx, catalogName); err != nil {
			return fmt.Errorf("update: %w", err)
		}
	}

	// Process definitions
	for def := range changes.Definitions(txCtx) {
		switch {
		case base.IsDropDefinition(def):
			if err := p.processDropDefinition(txCtx, def); err != nil {
				return fmt.Errorf("update drop: %w", err)
			}
		case base.IsReplaceDefinition(def):
			if err := p.processReplaceDefinition(txCtx, def, catalogName); err != nil {
				return fmt.Errorf("update replace: %w", err)
			}
		case base.IsIfNotExistsDefinition(def):
			if err := p.processIfNotExistsDefinition(txCtx, def, catalogName); err != nil {
				return fmt.Errorf("update if_not_exists: %w", err)
			}
		default:
			if err := p.persistDefinition(txCtx, def, catalogName); err != nil {
				return fmt.Errorf("update add: %w", err)
			}
		}
	}

	// Process directive definitions
	for name, dir := range changes.DirectiveDefinitions(txCtx) {
		if err := p.persistDirectiveDefinition(txCtx, name, dir); err != nil {
			return fmt.Errorf("update directive %s: %w", name, err)
		}
	}

	// Process extensions if available
	ext, hasExtensions := changes.(base.ExtensionsSource)
	if hasExtensions {
		for extDef := range ext.Extensions(txCtx) {
			if err := p.processExtension(txCtx, extDef, catalogName); err != nil {
				return fmt.Errorf("update extension: %w", err)
			}
		}
	}

	// Reconcile metadata
	if catalogName != "" {
		if err := p.reconcileMetadata(txCtx, catalogName); err != nil {
			return fmt.Errorf("update reconcile: %w", err)
		}
	}

	// Commit transaction
	if err := p.pool.Commit(txCtx); err != nil {
		return fmt.Errorf("update: commit: %w", err)
	}

	// Invalidate cache for the affected catalog
	if catalogName != "" {
		p.InvalidateCatalog(catalogName)
	} else {
		p.InvalidateAll()
	}

	return nil
}

// upsertCatalog inserts or updates a catalog record.
func (p *Provider) upsertCatalog(ctx context.Context, name string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("upsert catalog: %w", err)
	}
	defer conn.Close()

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`,
		p.table("_schema_catalogs"),
	), name)
	return err
}

// persistDefinition stores a type definition and all its children (fields, arguments, enum values).
func (p *Provider) persistDefinition(ctx context.Context, def *ast.Definition, catalogName string) error {
	// Strip control directives before persisting
	cleanDef := base.CloneDefinition(def, nil)
	cleanDef.Directives = base.StripControlDirectives(cleanDef.Directives)

	if err := p.upsertType(ctx, cleanDef, catalogName); err != nil {
		return err
	}

	for _, field := range cleanDef.Fields {
		if err := p.upsertField(ctx, cleanDef.Name, field, catalogName, ""); err != nil {
			return err
		}
		for _, arg := range field.Arguments {
			if err := p.upsertArgument(ctx, cleanDef.Name, field.Name, arg); err != nil {
				return err
			}
		}
	}

	for _, ev := range cleanDef.EnumValues {
		if err := p.upsertEnumValue(ctx, cleanDef.Name, ev); err != nil {
			return err
		}
	}

	return nil
}

// upsertType inserts or updates a type in _schema_types.
func (p *Provider) upsertType(ctx context.Context, def *ast.Definition, catalogName string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("upsert type: %w", err)
	}
	defer conn.Close()

	dirJSON, err := schema.MarshalDirectives(def.Directives)
	if err != nil {
		return fmt.Errorf("marshal directives for %s: %w", def.Name, err)
	}

	hugrType := string(schema.ClassifyType(def))
	module := base.DefinitionDirectiveArgString(def, base.ModuleDirectiveName, "name")
	ifaces := strings.Join(def.Interfaces, "|")
	unionTypes := strings.Join(def.Types, "|")

	// Check if type already exists with is_summarized=true
	var isSummarized bool
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT is_summarized FROM %s WHERE name = $1`, p.table("_schema_types"),
	), def.Name).Scan(&isSummarized)

	if err == nil && isSummarized {
		// Preserve description, long_description, and vec for summarized types
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET kind=$2, hugr_type=$3, module=$4, catalog=$5, directives=$6, interfaces=$7, union_types=$8
			 WHERE name=$1`,
			p.table("_schema_types"),
		), def.Name, string(def.Kind), hugrType, module, catalogName, string(dirJSON), ifaces, unionTypes)
		return err
	}

	if p.vecSize > 0 {
		// Compute embedding if available
		var vec types.Vector
		if p.embedder != nil {
			synth := SyntheticDescription(hugrType, def.Name, "", module, catalogName)
			text := EmbeddingText("", def.Description, synth)
			vec, _ = p.embedder.CreateEmbedding(ctx, text)
		}

		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name, kind, description, long_description, hugr_type, module, catalog, directives, interfaces, union_types, is_summarized, vec)
			 VALUES ($1, $2, $3, '', $4, $5, $6, $7, $8, $9, false, $10)
			 ON CONFLICT (name) DO UPDATE SET
			   kind=$2, description=$3, hugr_type=$4, module=$5, catalog=$6, directives=$7, interfaces=$8, union_types=$9, vec=$10`,
			p.table("_schema_types"),
		), def.Name, string(def.Kind), def.Description, hugrType, module, catalogName, string(dirJSON), ifaces, unionTypes, vec)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name, kind, description, long_description, hugr_type, module, catalog, directives, interfaces, union_types, is_summarized)
			 VALUES ($1, $2, $3, '', $4, $5, $6, $7, $8, $9, false)
			 ON CONFLICT (name) DO UPDATE SET
			   kind=$2, description=$3, hugr_type=$4, module=$5, catalog=$6, directives=$7, interfaces=$8, union_types=$9`,
			p.table("_schema_types"),
		), def.Name, string(def.Kind), def.Description, hugrType, module, catalogName, string(dirJSON), ifaces, unionTypes)
	}
	return err
}

// upsertField inserts or updates a field in _schema_fields.
func (p *Provider) upsertField(ctx context.Context, typeName string, field *ast.FieldDefinition, catalogName, depCatalog string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("upsert field: %w", err)
	}
	defer conn.Close()

	dirJSON, err := schema.MarshalDirectives(field.Directives)
	if err != nil {
		return fmt.Errorf("marshal field directives for %s.%s: %w", typeName, field.Name, err)
	}

	fieldType := schema.MarshalType(field.Type)
	hugrType := string(schema.ClassifyField(field, nil, nil))

	// Determine dependency catalog from field directives if not explicitly provided
	if depCatalog == "" {
		depCatalog = base.FieldDefDependency(field)
	}

	// Check if field already exists with is_summarized=true
	var isSummarized bool
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT is_summarized FROM %s WHERE type_name=$1 AND name=$2`, p.table("_schema_fields"),
	), typeName, field.Name).Scan(&isSummarized)

	if err == nil && isSummarized {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET field_type=$3, hugr_type=$4, catalog=$5, dependency_catalog=$6, directives=$7
			 WHERE type_name=$1 AND name=$2`,
			p.table("_schema_fields"),
		), typeName, field.Name, fieldType, hugrType, catalogName, nullStr(depCatalog), string(dirJSON))
		return err
	}

	if p.vecSize > 0 {
		var vec types.Vector
		if p.embedder != nil {
			synth := SyntheticDescription(hugrType, field.Name, typeName, "", catalogName)
			text := EmbeddingText("", field.Description, synth)
			vec, _ = p.embedder.CreateEmbedding(ctx, text)
		}

		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (type_name, name, field_type, description, long_description, hugr_type, catalog, dependency_catalog, directives, is_summarized, vec)
			 VALUES ($1, $2, $3, $4, '', $5, $6, $7, $8, false, $9)
			 ON CONFLICT (type_name, name) DO UPDATE SET
			   field_type=$3, description=$4, hugr_type=$5, catalog=$6, dependency_catalog=$7, directives=$8, vec=$9`,
			p.table("_schema_fields"),
		), typeName, field.Name, fieldType, field.Description, hugrType, catalogName, nullStr(depCatalog), string(dirJSON), vec)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (type_name, name, field_type, description, long_description, hugr_type, catalog, dependency_catalog, directives, is_summarized)
			 VALUES ($1, $2, $3, $4, '', $5, $6, $7, $8, false)
			 ON CONFLICT (type_name, name) DO UPDATE SET
			   field_type=$3, description=$4, hugr_type=$5, catalog=$6, dependency_catalog=$7, directives=$8`,
			p.table("_schema_fields"),
		), typeName, field.Name, fieldType, field.Description, hugrType, catalogName, nullStr(depCatalog), string(dirJSON))
	}
	return err
}

// upsertArgument inserts or updates an argument in _schema_arguments.
func (p *Provider) upsertArgument(ctx context.Context, typeName, fieldName string, arg *ast.ArgumentDefinition) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("upsert argument: %w", err)
	}
	defer conn.Close()

	dirJSON, err := schema.MarshalDirectives(arg.Directives)
	if err != nil {
		return fmt.Errorf("marshal arg directives for %s.%s.%s: %w", typeName, fieldName, arg.Name, err)
	}

	argType := schema.MarshalType(arg.Type)
	var defaultValue *string
	if arg.DefaultValue != nil {
		raw := arg.DefaultValue.Raw
		defaultValue = &raw
	}

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (type_name, field_name, name, arg_type, default_value, description, directives)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (type_name, field_name, name) DO UPDATE SET
		   arg_type=$4, default_value=$5, description=$6, directives=$7`,
		p.table("_schema_arguments"),
	), typeName, fieldName, arg.Name, argType, defaultValue, arg.Description, string(dirJSON))
	return err
}

// upsertEnumValue inserts or updates an enum value in _schema_enum_values.
func (p *Provider) upsertEnumValue(ctx context.Context, typeName string, ev *ast.EnumValueDefinition) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("upsert enum value: %w", err)
	}
	defer conn.Close()

	dirJSON, err := schema.MarshalDirectives(ev.Directives)
	if err != nil {
		return fmt.Errorf("marshal enum directives for %s.%s: %w", typeName, ev.Name, err)
	}

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (type_name, name, description, directives)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (type_name, name) DO UPDATE SET description=$3, directives=$4`,
		p.table("_schema_enum_values"),
	), typeName, ev.Name, ev.Description, string(dirJSON))
	return err
}

// persistDirectiveDefinition stores a directive definition in _schema_directives.
func (p *Provider) persistDirectiveDefinition(ctx context.Context, name string, dir *ast.DirectiveDefinition) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("persist directive: %w", err)
	}
	defer conn.Close()

	locations := make([]string, len(dir.Locations))
	for i, loc := range dir.Locations {
		locations[i] = string(loc)
	}
	locStr := strings.Join(locations, "|")

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (name, description, locations, is_repeatable)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (name) DO UPDATE SET description=$2, locations=$3, is_repeatable=$4`,
		p.table("_schema_directives"),
	), name, dir.Description, locStr, dir.IsRepeatable)
	return err
}

// processDropDefinition handles @drop directive on a definition.
func (p *Provider) processDropDefinition(ctx context.Context, def *ast.Definition) error {
	return p.deleteType(ctx, def.Name)
}

// processReplaceDefinition handles @replace directive: delete old, insert new.
func (p *Provider) processReplaceDefinition(ctx context.Context, def *ast.Definition, catalogName string) error {
	if err := p.deleteType(ctx, def.Name); err != nil {
		return err
	}
	return p.persistDefinition(ctx, def, catalogName)
}

// processIfNotExistsDefinition handles @if_not_exists directive: skip if already exists.
func (p *Provider) processIfNotExistsDefinition(ctx context.Context, def *ast.Definition, catalogName string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var count int
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE name = $1`, p.table("_schema_types"),
	), def.Name).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil // already exists, skip
	}
	return p.persistDefinition(ctx, def, catalogName)
}

// processExtension handles a type extension (field add/drop/replace, directive changes).
func (p *Provider) processExtension(ctx context.Context, extDef *ast.Definition, catalogName string) error {
	typeName := extDef.Name

	// Process field-level changes
	for _, field := range extDef.Fields {
		switch {
		case base.IsDropField(field):
			if err := p.deleteField(ctx, typeName, field.Name); err != nil {
				if base.DropFieldIfExists(field) {
					continue
				}
				return fmt.Errorf("drop field %s.%s: %w", typeName, field.Name, err)
			}
		case base.IsReplaceField(field):
			_ = p.deleteField(ctx, typeName, field.Name) // ignore error if doesn't exist
			cleanField := base.CloneFieldDefinition(field)
			cleanField.Directives = base.StripControlDirectives(cleanField.Directives)
			if err := p.upsertField(ctx, typeName, cleanField, catalogName, catalogName); err != nil {
				return fmt.Errorf("replace field %s.%s: %w", typeName, field.Name, err)
			}
			for _, arg := range cleanField.Arguments {
				if err := p.upsertArgument(ctx, typeName, cleanField.Name, arg); err != nil {
					return err
				}
			}
		default:
			// Add new field
			cleanField := base.CloneFieldDefinition(field)
			cleanField.Directives = base.StripControlDirectives(cleanField.Directives)
			depCat := base.FieldDefDependency(field)
			if depCat == "" {
				depCat = catalogName
			}
			if err := p.upsertField(ctx, typeName, cleanField, catalogName, depCat); err != nil {
				return fmt.Errorf("add field %s.%s: %w", typeName, field.Name, err)
			}
			for _, arg := range cleanField.Arguments {
				if err := p.upsertArgument(ctx, typeName, cleanField.Name, arg); err != nil {
					return err
				}
			}
		}
	}

	// Process enum value changes
	for _, ev := range extDef.EnumValues {
		if err := p.upsertEnumValue(ctx, typeName, ev); err != nil {
			return err
		}
	}

	return nil
}

// deleteType removes a type and all its children from the database.
func (p *Provider) deleteType(ctx context.Context, name string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("delete type: %w", err)
	}
	defer conn.Close()

	// Cascading deletes: enum_values → arguments → fields → types
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(`DELETE FROM %s WHERE type_name = $1`, p.table("_schema_enum_values")), name); err != nil {
		return err
	}
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(`DELETE FROM %s WHERE type_name = $1`, p.table("_schema_arguments")), name); err != nil {
		return err
	}
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(`DELETE FROM %s WHERE type_name = $1`, p.table("_schema_fields")), name); err != nil {
		return err
	}
	_, err = p.execWrite(ctx, conn, fmt.Sprintf(`DELETE FROM %s WHERE name = $1`, p.table("_schema_types")), name)
	return err
}

// deleteField removes a field and its arguments from the database.
func (p *Provider) deleteField(ctx context.Context, typeName, fieldName string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("delete field: %w", err)
	}
	defer conn.Close()

	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE type_name = $1 AND field_name = $2`,
		p.table("_schema_arguments"),
	), typeName, fieldName); err != nil {
		return err
	}
	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE type_name = $1 AND name = $2`,
		p.table("_schema_fields"),
	), typeName, fieldName)
	return err
}

// nullStr returns nil for empty string, otherwise the string value.
// Used to translate Go empty strings to SQL NULL for nullable VARCHAR columns.
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
