package db

import (
	"context"
	"encoding/json"
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
//
// The catalog name is extracted from the first definition with a @catalog directive.
func (p *Provider) Update(ctx context.Context, changes base.DefinitionsSource) error {
	return p.updateImpl(ctx, changes, "")
}

// UpdateWithCatalog persists compiled schema changes with an explicit catalog name override.
// When catalogOverride is non-empty, it is used instead of extracting from @catalog directives.
// Used for system types which don't carry @catalog directives.
func (p *Provider) UpdateWithCatalog(ctx context.Context, changes base.DefinitionsSource, catalogOverride string) error {
	return p.updateImpl(ctx, changes, catalogOverride)
}

// updateImpl is the shared implementation for Update and UpdateWithCatalog.
func (p *Provider) updateImpl(ctx context.Context, changes base.DefinitionsSource, catalogOverride string) error {
	// Collect all definitions first — iterators may be one-shot (iter.Seq),
	// so we must not iterate twice.
	var defs []*ast.Definition
	for def := range changes.Definitions(ctx) {
		defs = append(defs, def)
	}

	// Determine catalog name: use override if provided, otherwise extract from @catalog directive.
	catalogName := catalogOverride
	if catalogName == "" {
		for _, def := range defs {
			catalogName = base.DefinitionCatalog(def)
			if catalogName != "" {
				break
			}
		}
	}

	// Validate references before persisting: field types, argument types,
	// interface references, and union member types must all exist in
	// _schema_types or in the current batch. Fail fast on first missing reference.
	if err := p.validateReferences(ctx, defs); err != nil {
		return err
	}

	// Begin transaction
	txCtx, err := p.pool.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("update: begin tx: %w", err)
	}
	defer p.pool.Rollback(txCtx)

	// Acquire a single connection for all write operations within this transaction.
	conn, err := p.pool.Conn(txCtx)
	if err != nil {
		return fmt.Errorf("update: conn: %w", err)
	}
	defer conn.Close()

	// Upsert catalog record
	if catalogName != "" {
		if err := p.upsertCatalog(txCtx, conn, catalogName); err != nil {
			return fmt.Errorf("update: %w", err)
		}
	}

	// Process definitions from collected slice
	for _, def := range defs {
		switch {
		case base.IsDropDefinition(def):
			if err := p.processDropDefinition(txCtx, conn, def); err != nil {
				return fmt.Errorf("update drop: %w", err)
			}
		case base.IsReplaceDefinition(def):
			if err := p.processReplaceDefinition(txCtx, conn, def, catalogName); err != nil {
				return fmt.Errorf("update replace: %w", err)
			}
		case base.IsIfNotExistsDefinition(def):
			if err := p.processIfNotExistsDefinition(txCtx, conn, def, catalogName); err != nil {
				return fmt.Errorf("update if_not_exists: %w", err)
			}
		default:
			if err := p.persistDefinition(txCtx, conn, def, catalogName); err != nil {
				return fmt.Errorf("update add: %w", err)
			}
		}
	}

	// Process directive definitions
	for name, dir := range changes.DirectiveDefinitions(txCtx) {
		if err := p.persistDirectiveDefinition(txCtx, conn, name, dir); err != nil {
			return fmt.Errorf("update directive %s: %w", name, err)
		}
	}

	// Process extensions if available
	ext, hasExtensions := changes.(base.ExtensionsSource)
	if hasExtensions {
		for extDef := range ext.Extensions(txCtx) {
			if err := p.processExtension(txCtx, conn, extDef, catalogName); err != nil {
				return fmt.Errorf("update extension: %w", err)
			}
		}
	}

	// Release connection before reconcile (which acquires its own).
	conn.Close()

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
func (p *Provider) upsertCatalog(ctx context.Context, conn *Connection, name string) error {
	_, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`,
		p.table("_schema_catalogs"),
	), name)
	return err
}

// persistDefinition stores a type definition and all its children (fields, arguments, enum values).
// Computes embeddings in a single batch call instead of per-item.
func (p *Provider) persistDefinition(ctx context.Context, conn *Connection, def *ast.Definition, catalogName string) error {
	// Strip control directives before persisting
	cleanDef := base.CloneDefinition(def, nil)
	cleanDef.Directives = base.StripControlDirectives(cleanDef.Directives)

	// Collect embedding texts: type + all fields
	var texts []string
	typeVecIdx := -1
	fieldVecIdx := make(map[int]int) // field index → index in texts[]

	if p.vecSize > 0 && p.embedder != nil {
		hugrType := string(schema.ClassifyType(cleanDef))
		module := base.DefinitionDirectiveArgString(cleanDef, base.ModuleDirectiveName, "name")
		synth := SyntheticDescription(hugrType, cleanDef.Name, "", module, catalogName)
		typeVecIdx = len(texts)
		texts = append(texts, EmbeddingText("", cleanDef.Description, synth))

		for i, field := range cleanDef.Fields {
			ht := string(schema.ClassifyField(field, nil, nil))
			if ht == "" && field.Directives.ForName(base.ModuleCatalogDirectiveName) != nil {
				ht = string(base.HugrTypeFieldSubmodule)
			}
			synth := SyntheticDescription(ht, field.Name, cleanDef.Name, "", catalogName)
			fieldVecIdx[i] = len(texts)
			texts = append(texts, EmbeddingText("", field.Description, synth))
		}
	}

	// Batch compute embeddings (1 API call instead of N)
	vecs, err := p.computeEmbeddings(ctx, texts)
	if err != nil {
		return fmt.Errorf("compute embeddings: %w", err)
	}

	// Distribute vectors to upsert calls
	var typeVec types.Vector
	if typeVecIdx >= 0 {
		typeVec = vecs[typeVecIdx]
	}

	if err := p.upsertType(ctx, conn, cleanDef, catalogName, typeVec); err != nil {
		return err
	}

	for i, field := range cleanDef.Fields {
		var fieldVec types.Vector
		if idx, ok := fieldVecIdx[i]; ok {
			fieldVec = vecs[idx]
		}
		if err := p.upsertField(ctx, conn, cleanDef.Name, field, catalogName, "", i, fieldVec); err != nil {
			return err
		}
		for j, arg := range field.Arguments {
			if err := p.upsertArgument(ctx, conn, cleanDef.Name, field.Name, arg, j); err != nil {
				return err
			}
		}
	}

	for i, ev := range cleanDef.EnumValues {
		if err := p.upsertEnumValue(ctx, conn, cleanDef.Name, ev, i); err != nil {
			return err
		}
	}

	return nil
}

// upsertType inserts or updates a type in _schema_types.
// vec is pre-computed by the caller (batch path); nil when embedder is not configured.
// Always resets is_summarized=false — the summarization service will re-flag later.
func (p *Provider) upsertType(ctx context.Context, conn *Connection, def *ast.Definition, catalogName string, vec types.Vector) error {
	dirJSON, err := schema.MarshalDirectives(def.Directives)
	if err != nil {
		return fmt.Errorf("marshal directives for %s: %w", def.Name, err)
	}

	hugrType := string(schema.ClassifyType(def))
	module := base.DefinitionDirectiveArgString(def, base.ModuleDirectiveName, "name")
	ifaces := strings.Join(def.Interfaces, "|")
	unionTypes := strings.Join(def.Types, "|")

	if p.vecSize > 0 {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name, kind, description, long_description, hugr_type, module, catalog, directives, interfaces, union_types, is_summarized, vec)
			 VALUES ($1, $2, $3, '', $4, $5, $6, $7, $8, $9, false, $10)
			 ON CONFLICT (name) DO UPDATE SET
			   kind=$2, description=$3, long_description='', hugr_type=$4, module=$5, catalog=$6, directives=$7, interfaces=$8, union_types=$9, is_summarized=false, vec=$10`,
			p.table("_schema_types"),
		), def.Name, string(def.Kind), def.Description, hugrType, module, catalogName, string(dirJSON), ifaces, unionTypes, vec)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name, kind, description, long_description, hugr_type, module, catalog, directives, interfaces, union_types, is_summarized)
			 VALUES ($1, $2, $3, '', $4, $5, $6, $7, $8, $9, false)
			 ON CONFLICT (name) DO UPDATE SET
			   kind=$2, description=$3, long_description='', hugr_type=$4, module=$5, catalog=$6, directives=$7, interfaces=$8, union_types=$9, is_summarized=false`,
			p.table("_schema_types"),
		), def.Name, string(def.Kind), def.Description, hugrType, module, catalogName, string(dirJSON), ifaces, unionTypes)
	}
	return err
}

// upsertField inserts or updates a field in _schema_fields.
// ordinal preserves the original definition order of the field within its type.
// vec is pre-computed by the caller (batch path); nil when embedder is not configured.
// Always resets is_summarized=false — the summarization service will re-flag later.
func (p *Provider) upsertField(ctx context.Context, conn *Connection, typeName string, field *ast.FieldDefinition, catalogName, depCatalog string, ordinal int, vec types.Vector) error {
	dirJSON, err := schema.MarshalDirectives(field.Directives)
	if err != nil {
		return fmt.Errorf("marshal field directives for %s.%s: %w", typeName, field.Name, err)
	}

	hugrType := string(schema.ClassifyField(field, nil, nil))

	// Module entry fields have @module_catalog but ClassifyField can't detect
	// them without typeLookup; mark them explicitly.
	if hugrType == "" && field.Directives.ForName(base.ModuleCatalogDirectiveName) != nil {
		hugrType = string(base.HugrTypeFieldSubmodule)
	}

	// For submodule fields, store just the base type name (no nullable/list wrappers).
	// This enables direct JOIN with _schema_module_type_catalogs.type_name for filtering.
	var fieldType string
	if hugrType == string(base.HugrTypeFieldSubmodule) {
		fieldType = baseTypeName(field.Type)
	} else {
		fieldType = schema.MarshalType(field.Type)
	}

	// Determine dependency catalog from field directives if not explicitly provided
	if depCatalog == "" {
		depCatalog = base.FieldDefDependency(field)
	}

	if p.vecSize > 0 {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (type_name, name, field_type, description, long_description, hugr_type, catalog, dependency_catalog, directives, is_summarized, vec, ordinal)
			 VALUES ($1, $2, $3, $4, '', $5, $6, $7, $8, false, $9, $10)
			 ON CONFLICT (type_name, name) DO UPDATE SET
			   field_type=$3, description=$4, hugr_type=$5, catalog=$6, dependency_catalog=$7, directives=$8, is_summarized=false, vec=$9, ordinal=$10`,
			p.table("_schema_fields"),
		), typeName, field.Name, fieldType, field.Description, hugrType, catalogName, nullStr(depCatalog), string(dirJSON), vec, ordinal)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (type_name, name, field_type, description, long_description, hugr_type, catalog, dependency_catalog, directives, is_summarized, ordinal)
			 VALUES ($1, $2, $3, $4, '', $5, $6, $7, $8, false, $9)
			 ON CONFLICT (type_name, name) DO UPDATE SET
			   field_type=$3, description=$4, hugr_type=$5, catalog=$6, dependency_catalog=$7, directives=$8, is_summarized=false, ordinal=$9`,
			p.table("_schema_fields"),
		), typeName, field.Name, fieldType, field.Description, hugrType, catalogName, nullStr(depCatalog), string(dirJSON), ordinal)
	}
	return err
}

// upsertArgument inserts or updates an argument in _schema_arguments.
// ordinal preserves the original definition order of the argument within its field.
func (p *Provider) upsertArgument(ctx context.Context, conn *Connection, typeName, fieldName string, arg *ast.ArgumentDefinition, ordinal int) error {
	dirJSON, err := schema.MarshalDirectives(arg.Directives)
	if err != nil {
		return fmt.Errorf("marshal arg directives for %s.%s.%s: %w", typeName, fieldName, arg.Name, err)
	}

	argType := schema.MarshalType(arg.Type)
	var defaultValue *string
	if arg.DefaultValue != nil {
		// Store as JSON to preserve the value Kind (IntValue, BooleanValue, etc.)
		encoded, err := json.Marshal(schema.MarshalValue(arg.DefaultValue))
		if err != nil {
			return fmt.Errorf("marshal default value for %s.%s.%s: %w", typeName, fieldName, arg.Name, err)
		}
		s := string(encoded)
		defaultValue = &s
	}

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (type_name, field_name, name, arg_type, default_value, description, directives, ordinal)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (type_name, field_name, name) DO UPDATE SET
		   arg_type=$4, default_value=$5, description=$6, directives=$7, ordinal=$8`,
		p.table("_schema_arguments"),
	), typeName, fieldName, arg.Name, argType, defaultValue, arg.Description, string(dirJSON), ordinal)
	return err
}

// upsertEnumValue inserts or updates an enum value in _schema_enum_values.
// ordinal preserves the original definition order of the enum value within its type.
func (p *Provider) upsertEnumValue(ctx context.Context, conn *Connection, typeName string, ev *ast.EnumValueDefinition, ordinal int) error {
	dirJSON, err := schema.MarshalDirectives(ev.Directives)
	if err != nil {
		return fmt.Errorf("marshal enum directives for %s.%s: %w", typeName, ev.Name, err)
	}

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (type_name, name, description, directives, ordinal)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (type_name, name) DO UPDATE SET description=$3, directives=$4, ordinal=$5`,
		p.table("_schema_enum_values"),
	), typeName, ev.Name, ev.Description, string(dirJSON), ordinal)
	return err
}

// persistDirectiveDefinition stores a directive definition in _schema_directives.
func (p *Provider) persistDirectiveDefinition(ctx context.Context, conn *Connection, name string, dir *ast.DirectiveDefinition) error {
	locations := make([]string, len(dir.Locations))
	for i, loc := range dir.Locations {
		locations[i] = string(loc)
	}
	locStr := strings.Join(locations, "|")

	argsJSON, err := schema.MarshalArgumentDefinitions(dir.Arguments)
	if err != nil {
		return fmt.Errorf("marshal directive arguments: %w", err)
	}

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (name, description, locations, is_repeatable, arguments)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (name) DO UPDATE SET description=$2, locations=$3, is_repeatable=$4, arguments=$5`,
		p.table("_schema_directives"),
	), name, dir.Description, locStr, dir.IsRepeatable, string(argsJSON))
	return err
}

// processDropDefinition handles @drop directive on a definition.
func (p *Provider) processDropDefinition(ctx context.Context, conn *Connection, def *ast.Definition) error {
	return p.deleteType(ctx, conn, def.Name)
}

// processReplaceDefinition handles @replace directive: delete old, insert new.
func (p *Provider) processReplaceDefinition(ctx context.Context, conn *Connection, def *ast.Definition, catalogName string) error {
	if err := p.deleteType(ctx, conn, def.Name); err != nil {
		return err
	}
	return p.persistDefinition(ctx, conn, def, catalogName)
}

// processIfNotExistsDefinition handles @if_not_exists directive: skip if already exists.
func (p *Provider) processIfNotExistsDefinition(ctx context.Context, conn *Connection, def *ast.Definition, catalogName string) error {
	var count int
	err := conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE name = $1`, p.table("_schema_types"),
	), def.Name).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil // already exists, skip
	}
	return p.persistDefinition(ctx, conn, def, catalogName)
}

// processExtension handles a type extension (field add/drop/replace, directive changes).
func (p *Provider) processExtension(ctx context.Context, conn *Connection, extDef *ast.Definition, catalogName string) error {
	typeName := extDef.Name

	// Determine the dependency catalog: use @dependency if present, otherwise
	// look up the type's owning catalog from DB. If the type is owned by a
	// different catalog, that catalog is the dependency.
	resolveDepCat := func(field *ast.FieldDefinition) string {
		depCat := base.FieldDefDependency(field)
		if depCat != "" {
			return depCat
		}
		// Auto-detect from the type's owning catalog in _schema_types.
		ownerCat := p.typeOwnerCatalogConn(ctx, conn, typeName)
		if ownerCat != "" && ownerCat != catalogName {
			return ownerCat
		}
		return catalogName
	}

	// Batch compute embeddings for add/replace fields.
	var texts []string
	fieldVecIdx := make(map[int]int) // field index in extDef.Fields → index in texts[]
	if p.vecSize > 0 && p.embedder != nil {
		for fieldIdx, field := range extDef.Fields {
			if base.IsDropField(field) {
				continue
			}
			cleanField := base.CloneFieldDefinition(field)
			cleanField.Directives = base.StripControlDirectives(cleanField.Directives)
			ht := string(schema.ClassifyField(cleanField, nil, nil))
			if ht == "" && cleanField.Directives.ForName(base.ModuleCatalogDirectiveName) != nil {
				ht = string(base.HugrTypeFieldSubmodule)
			}
			synth := SyntheticDescription(ht, field.Name, typeName, "", catalogName)
			fieldVecIdx[fieldIdx] = len(texts)
			texts = append(texts, EmbeddingText("", field.Description, synth))
		}
	}

	// Single batch call instead of N individual CreateEmbedding calls
	vecs, err := p.computeEmbeddings(ctx, texts)
	if err != nil {
		return fmt.Errorf("compute embeddings: %w", err)
	}

	// Process field-level changes with pre-computed vectors
	for fieldIdx, field := range extDef.Fields {
		switch {
		case base.IsDropField(field):
			if err := p.deleteField(ctx, conn, typeName, field.Name); err != nil {
				if base.DropFieldIfExists(field) {
					continue
				}
				return fmt.Errorf("drop field %s.%s: %w", typeName, field.Name, err)
			}
		case base.IsReplaceField(field):
			// Delete old field if it exists; ignore "not found" errors since
			// replace should work even if the field doesn't exist yet.
			if err := p.deleteField(ctx, conn, typeName, field.Name); err != nil {
				// Only log, don't fail — the field might not exist yet
			}
			cleanField := base.CloneFieldDefinition(field)
			cleanField.Directives = base.StripControlDirectives(cleanField.Directives)
			depCat := resolveDepCat(field)
			var fieldVec types.Vector
			if idx, ok := fieldVecIdx[fieldIdx]; ok {
				fieldVec = vecs[idx]
			}
			if err := p.upsertField(ctx, conn, typeName, cleanField, catalogName, depCat, fieldIdx, fieldVec); err != nil {
				return fmt.Errorf("replace field %s.%s: %w", typeName, field.Name, err)
			}
			for i, arg := range cleanField.Arguments {
				if err := p.upsertArgument(ctx, conn, typeName, cleanField.Name, arg, i); err != nil {
					return err
				}
			}
		default:
			// Add new field
			cleanField := base.CloneFieldDefinition(field)
			cleanField.Directives = base.StripControlDirectives(cleanField.Directives)
			depCat := resolveDepCat(field)
			var fieldVec types.Vector
			if idx, ok := fieldVecIdx[fieldIdx]; ok {
				fieldVec = vecs[idx]
			}
			if err := p.upsertField(ctx, conn, typeName, cleanField, catalogName, depCat, fieldIdx, fieldVec); err != nil {
				return fmt.Errorf("add field %s.%s: %w", typeName, field.Name, err)
			}
			for i, arg := range cleanField.Arguments {
				if err := p.upsertArgument(ctx, conn, typeName, cleanField.Name, arg, i); err != nil {
					return err
				}
			}
		}
	}

	// Process enum value changes
	for i, ev := range extDef.EnumValues {
		if err := p.upsertEnumValue(ctx, conn, typeName, ev, i); err != nil {
			return err
		}
	}

	// Merge type-level directives from the extension into the existing type.
	// Extensions can carry directives like @references that must be persisted
	// on the base type definition for the planner to resolve reference joins.
	extDirs := base.StripControlDirectives(extDef.Directives)
	if len(extDirs) > 0 {
		if err := p.mergeTypeDirectives(ctx, conn, typeName, extDirs); err != nil {
			return fmt.Errorf("merge directives for %s: %w", typeName, err)
		}
	}

	// Evict the extended type from cache since its fields/enum values changed.
	// This is needed because InvalidateCatalog only clears types owned by the
	// current catalog, not types from other catalogs that were extended.
	p.cache.evictType(typeName)

	return nil
}

// mergeTypeDirectives appends extension directives to an existing type's directives in the DB.
// Deduplicates repeatable directives (like @references) by their "name" argument.
func (p *Provider) mergeTypeDirectives(ctx context.Context, conn *Connection, typeName string, newDirs ast.DirectiveList) error {
	// Load current directives from DB
	var dirJSON string
	err := conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT CAST(directives AS VARCHAR) FROM %s WHERE name = $1`,
		p.table("_schema_types"),
	), typeName).Scan(&dirJSON)
	if err != nil {
		return fmt.Errorf("load directives: %w", err)
	}

	existing, err := schema.UnmarshalDirectives([]byte(dirJSON))
	if err != nil {
		return fmt.Errorf("unmarshal directives: %w", err)
	}

	// Build a set of existing directive identifiers for deduplication.
	// For repeatable directives (like @references), use "name" + the "name" arg value as key.
	existingKeys := make(map[string]struct{}, len(existing))
	for _, d := range existing {
		existingKeys[directiveKey(d)] = struct{}{}
	}

	// Append only new directives that don't already exist
	for _, d := range newDirs {
		key := directiveKey(d)
		if _, exists := existingKeys[key]; exists {
			continue
		}
		existing = append(existing, d)
		existingKeys[key] = struct{}{}
	}

	// Write updated directives back
	merged, err := schema.MarshalDirectives(existing)
	if err != nil {
		return fmt.Errorf("marshal merged directives: %w", err)
	}

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`UPDATE %s SET directives = $2 WHERE name = $1`,
		p.table("_schema_types"),
	), typeName, string(merged))
	return err
}

// directiveKey returns a string key for deduplicating directives.
// For directives with a "name" argument (like @references), uses "directiveName:nameArg".
// For others, uses just the directive name.
func directiveKey(d *ast.Directive) string {
	nameArg := d.Arguments.ForName("name")
	if nameArg != nil && nameArg.Value != nil {
		return d.Name + ":" + nameArg.Value.Raw
	}
	return d.Name
}

// typeOwnerCatalog returns the catalog that owns the given type, or "" if not found.
// Acquires its own connection — used by external callers.
func (p *Provider) typeOwnerCatalog(ctx context.Context, typeName string) string {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return ""
	}
	defer conn.Close()
	return p.typeOwnerCatalogConn(ctx, conn, typeName)
}

// typeOwnerCatalogConn returns the catalog that owns the given type, using the provided connection.
func (p *Provider) typeOwnerCatalogConn(ctx context.Context, conn *Connection, typeName string) string {
	var catalog string
	err := conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT COALESCE(catalog, '') FROM %s WHERE name = $1`,
		p.table("_schema_types"),
	), typeName).Scan(&catalog)
	if err != nil {
		return ""
	}
	return catalog
}

// deleteType removes a type and all its children from the database.
func (p *Provider) deleteType(ctx context.Context, conn *Connection, name string) error {
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
	_, err := p.execWrite(ctx, conn, fmt.Sprintf(`DELETE FROM %s WHERE name = $1`, p.table("_schema_types")), name)
	return err
}

// deleteField removes a field and its arguments from the database.
func (p *Provider) deleteField(ctx context.Context, conn *Connection, typeName, fieldName string) error {
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE type_name = $1 AND field_name = $2`,
		p.table("_schema_arguments"),
	), typeName, fieldName); err != nil {
		return err
	}
	_, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE type_name = $1 AND name = $2`,
		p.table("_schema_fields"),
	), typeName, fieldName)
	return err
}

// validateReferences checks that all type references in the batch are resolvable:
// field types, argument types, interface references, and union member types must
// exist either in _schema_types or in the current batch being added.
// Returns an error on the first missing reference with the type name in the message.
func (p *Provider) validateReferences(ctx context.Context, defs []*ast.Definition) error {
	// Build set of type names being added in this batch (skip drops).
	batchTypes := make(map[string]struct{})
	for _, def := range defs {
		if !base.IsDropDefinition(def) {
			batchTypes[def.Name] = struct{}{}
		}
	}

	// typeExists checks if a type name exists in DB or batch.
	typeExists := func(name string) (bool, error) {
		if _, ok := batchTypes[name]; ok {
			return true, nil
		}
		conn, err := p.pool.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer conn.Close()
		var count int
		err = conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT count(*) FROM %s WHERE name = $1`, p.table("_schema_types"),
		), name).Scan(&count)
		if err != nil {
			return false, err
		}
		return count > 0, nil
	}

	// Check each non-drop definition.
	for _, def := range defs {
		if base.IsDropDefinition(def) {
			continue
		}

		// Validate interface references.
		for _, iface := range def.Interfaces {
			exists, err := typeExists(iface)
			if err != nil {
				return fmt.Errorf("validate references: %w", err)
			}
			if !exists {
				return fmt.Errorf("validate references: type %q referenced as interface by %q does not exist", iface, def.Name)
			}
		}

		// Validate union member types.
		for _, member := range def.Types {
			exists, err := typeExists(member)
			if err != nil {
				return fmt.Errorf("validate references: %w", err)
			}
			if !exists {
				return fmt.Errorf("validate references: type %q referenced as union member by %q does not exist", member, def.Name)
			}
		}

		// Validate field type references.
		for _, field := range def.Fields {
			if typeName := baseTypeName(field.Type); typeName != "" {
				exists, err := typeExists(typeName)
				if err != nil {
					return fmt.Errorf("validate references: %w", err)
				}
				if !exists {
					return fmt.Errorf("validate references: type %q referenced by field %s.%s does not exist", typeName, def.Name, field.Name)
				}
			}

			// Validate argument type references.
			for _, arg := range field.Arguments {
				if typeName := baseTypeName(arg.Type); typeName != "" {
					exists, err := typeExists(typeName)
					if err != nil {
						return fmt.Errorf("validate references: %w", err)
					}
					if !exists {
						return fmt.Errorf("validate references: type %q referenced by argument %s.%s.%s does not exist", typeName, def.Name, field.Name, arg.Name)
					}
				}
			}
		}
	}

	return nil
}

// baseTypeName extracts the underlying named type from an *ast.Type,
// unwrapping list wrappers and non-null markers.
func baseTypeName(t *ast.Type) string {
	if t == nil {
		return ""
	}
	if t.NamedType != "" {
		return t.NamedType
	}
	return baseTypeName(t.Elem)
}

// nullStr returns nil for empty string, otherwise the string value.
// Used to translate Go empty strings to SQL NULL for nullable VARCHAR columns.
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

