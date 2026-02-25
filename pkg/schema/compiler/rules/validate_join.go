package rules

import (
	"regexp"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// reSQLField matches bracket-notation field references in SQL strings:
// [field], [$catalog], [source.field], [$table.field.nested]
var reSQLField = regexp.MustCompile(`\[\$?[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\]`)

var _ base.BatchRule = (*JoinValidator)(nil)

// JoinValidator validates @join directives on fields in data objects.
// It checks that:
// - source_fields and references_fields have equal length
// - Referenced object exists
// - Source fields exist and are scalars
// - References fields exist and types match source fields
// - SQL-referenced fields are valid
//
// Runs in FINALIZE phase so all types are resolved.
type JoinValidator struct{}

func (r *JoinValidator) Name() string     { return "JoinValidator" }
func (r *JoinValidator) Phase() base.Phase { return base.PhaseFinalize }

func (r *JoinValidator) ProcessAll(ctx base.CompilationContext) error {
	for name := range ctx.Objects() {
		def := ctx.LookupType(name)
		if def == nil {
			continue
		}
		for _, f := range def.Fields {
			joinDir := f.Directives.ForName("join")
			if joinDir == nil {
				continue
			}
			if err := validateJoinField(ctx, def, f, joinDir); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateJoinField(ctx base.CompilationContext, def *ast.Definition, field *ast.FieldDefinition, dir *ast.Directive) error {
	refName := base.DirectiveArgString(dir, "references_name")
	sourceFields := base.DirectiveArgStrings(dir, "source_fields")
	refsFields := base.DirectiveArgStrings(dir, "references_fields")
	sql := base.DirectiveArgString(dir, "sql")

	// 1. Validate source_fields and references_fields have equal length
	if len(sourceFields) != len(refsFields) {
		return gqlerror.ErrorPosf(field.Position,
			"@join on %s.%s: source_fields and references_fields must have the same number of fields",
			def.Name, field.Name)
	}

	// 2. Validate referenced object exists
	refDef := ctx.LookupType(refName)
	if refDef == nil {
		// Also check source
		refDef = ctx.Source().ForName(ctx.Context(), refName)
	}
	if refDef == nil {
		return gqlerror.ErrorPosf(field.Position,
			"@join on %s.%s: references object %q not found",
			def.Name, field.Name, refName)
	}

	// 3. Propagate @catalog from referenced object to field if missing
	if field.Directives.ForName("catalog") == nil {
		if refCatalog := refDef.Directives.ForName("catalog"); refCatalog != nil {
			field.Directives = append(field.Directives, refCatalog)
		}
	}

	// 4. Validate source fields exist and are scalar; validate reference fields match
	for i, sfn := range sourceFields {
		sf := findFieldByPath(ctx, def, sfn)
		if sf == nil {
			return gqlerror.ErrorPosf(field.Position,
				"@join on %s.%s: source field %q not found",
				def.Name, field.Name, sfn)
		}
		// Source field must be scalar (not list)
		if sf.Type.NamedType == "" {
			return gqlerror.ErrorPosf(field.Position,
				"@join on %s.%s: source field %q must be a scalar type",
				def.Name, field.Name, sfn)
		}
		if !ctx.IsScalar(sf.Type.Name()) {
			return gqlerror.ErrorPosf(field.Position,
				"@join on %s.%s: source field %q must be a scalar type",
				def.Name, field.Name, sfn)
		}

		// Validate reference field exists and type matches
		rfn := refsFields[i]
		rf := findFieldByPath(ctx, refDef, rfn)
		if rf == nil {
			return gqlerror.ErrorPosf(field.Position,
				"@join on %s.%s: references field %q not found in %s",
				def.Name, field.Name, rfn, refName)
		}
		if !equalTypesIgnoreNull(sf.Type, rf.Type) {
			return gqlerror.ErrorPosf(field.Position,
				"@join on %s.%s: field %q and references field %q must have the same type",
				def.Name, field.Name, sfn, rfn)
		}
	}

	// 5. Validate SQL-referenced fields
	if sql != "" {
		sqlFields := extractFieldsFromSQL(sql)
		for _, sf := range sqlFields {
			// Skip $-prefixed system vars (e.g. [$catalog])
			if strings.HasPrefix(sf, "$") {
				continue
			}
			parts := strings.SplitN(sf, ".", 2)
			if len(parts) != 2 {
				return gqlerror.ErrorPosf(field.Position,
					"@join on %s.%s: invalid field %q in SQL",
					def.Name, field.Name, sf)
			}
			switch parts[0] {
			case "source":
				if err := validateSubQueryField(ctx, def, parts[1], field.Position, def.Name, field.Name); err != nil {
					return err
				}
			case "dest":
				if err := validateSubQueryField(ctx, refDef, parts[1], field.Position, def.Name, field.Name); err != nil {
					return err
				}
			default:
				return gqlerror.ErrorPosf(field.Position,
					"@join on %s.%s: invalid field prefix %q in SQL, expected 'source' or 'dest'",
					def.Name, field.Name, parts[0])
			}
		}
	}

	return nil
}

// findFieldByPath resolves a dotted field path like "field" or "nested.field"
// on a definition, traversing object types.
func findFieldByPath(ctx base.CompilationContext, def *ast.Definition, path string) *ast.FieldDefinition {
	parts := strings.SplitN(path, ".", 2)
	f := def.Fields.ForName(parts[0])
	if f == nil {
		return nil
	}
	if len(parts) == 1 {
		return f
	}
	// Traverse into nested object
	nestedDef := ctx.LookupType(f.Type.Name())
	if nestedDef == nil {
		return nil
	}
	return findFieldByPath(ctx, nestedDef, parts[1])
}

// validateSubQueryField validates that a field path exists and ends in a scalar.
func validateSubQueryField(ctx base.CompilationContext, def *ast.Definition, path string, pos *ast.Position, objName, fieldName string) error {
	parts := strings.SplitN(path, ".", 2)
	f := def.Fields.ForName(parts[0])
	if f == nil {
		return gqlerror.ErrorPosf(pos,
			"@join on %s.%s: field %q not found in %s",
			objName, fieldName, parts[0], def.Name)
	}
	if len(parts) == 1 {
		// Terminal field — must be scalar
		if f.Type.NamedType == "" || !ctx.IsScalar(f.Type.Name()) {
			return gqlerror.ErrorPosf(pos,
				"@join on %s.%s: field %q in %s must be a scalar type",
				objName, fieldName, parts[0], def.Name)
		}
		return nil
	}
	// Intermediate field — must be an object (not scalar, not list)
	if f.Type.NamedType == "" || ctx.IsScalar(f.Type.Name()) {
		return gqlerror.ErrorPosf(pos,
			"@join on %s.%s: field %q in %s must be an object type for nested path",
			objName, fieldName, parts[0], def.Name)
	}
	nestedDef := ctx.LookupType(f.Type.Name())
	if nestedDef == nil {
		return gqlerror.ErrorPosf(pos,
			"@join on %s.%s: object %q not found",
			objName, fieldName, f.Type.Name())
	}
	return validateSubQueryField(ctx, nestedDef, parts[1], pos, objName, fieldName)
}

// extractFieldsFromSQL extracts [field.path] references from SQL strings.
// Matches patterns like [source.field], [dest.field.nested], [$catalog].
// Returns the field names with brackets stripped (e.g. "source.field", "$catalog").
func extractFieldsFromSQL(sql string) []string {
	if sql == "" {
		return nil
	}
	matches := reSQLField.FindAllString(sql, -1)
	if matches == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(matches))
	var fields []string
	for _, m := range matches {
		f := m[1 : len(m)-1] // strip surrounding []
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		fields = append(fields, f)
	}
	return fields
}

// equalTypes checks if two types are structurally equal.
func equalTypes(a, b *ast.Type) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.NamedType != b.NamedType || a.NonNull != b.NonNull {
		return false
	}
	return equalTypes(a.Elem, b.Elem)
}

// equalTypesIgnoreNull checks if two types have the same named type, ignoring nullability.
// This is appropriate for @join validation where a non-null PK joining a nullable FK is valid.
func equalTypesIgnoreNull(a, b *ast.Type) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.NamedType != b.NamedType {
		return false
	}
	return equalTypesIgnoreNull(a.Elem, b.Elem)
}
