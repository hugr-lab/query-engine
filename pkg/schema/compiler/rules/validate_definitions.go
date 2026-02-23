package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ base.BatchRule = (*DefinitionValidator)(nil)

// DefinitionValidator validates source schema structure during the VALIDATE phase.
// It checks that @table/@view types have @pk fields and that field types are valid.
type DefinitionValidator struct{}

func (r *DefinitionValidator) Name() string      { return "DefinitionValidator" }
func (r *DefinitionValidator) Phase() base.Phase { return base.PhaseValidate }

func (r *DefinitionValidator) ProcessAll(ctx base.CompilationContext) error {
	for def := range ctx.Source().Definitions(ctx.Context()) {
		if err := validateDefinition(ctx, def); err != nil {
			return err
		}
	}
	return nil
}

func validateDefinition(ctx base.CompilationContext, def *ast.Definition) error {
	isTable := def.Directives.ForName("table") != nil
	isView := def.Directives.ForName("view") != nil

	if isTable || isView {
		if err := validatePrimaryKey(def, isTable); err != nil {
			return err
		}
	}

	if err := validateFieldTypes(ctx, def); err != nil {
		return err
	}

	if err := validateReferences(ctx, def); err != nil {
		return err
	}

	return nil
}

// validatePrimaryKey ensures that @table/@view definitions have at least one @pk field.
func validatePrimaryKey(def *ast.Definition, isTable bool) error {
	for _, f := range def.Fields {
		if f.Directives.ForName("pk") != nil {
			return nil
		}
	}
	return gqlerror.ErrorPosf(def.Position, "%s %q must have at least one @pk field",
		directiveKind(isTable), def.Name)
}

// validateFieldTypes checks that every field's named type is either a known scalar
// or a definition reachable via source or target lookup.
func validateFieldTypes(ctx base.CompilationContext, def *ast.Definition) error {
	for _, f := range def.Fields {
		typeName := f.Type.Name()
		if typeName == "" {
			continue
		}
		if ctx.IsScalar(typeName) {
			continue
		}
		if ctx.Source().ForName(ctx.Context(), typeName) != nil {
			continue
		}
		if ctx.LookupType(typeName) != nil {
			continue
		}
		return gqlerror.ErrorPosf(f.Position, "field %q of %q has unknown type %q",
			f.Name, def.Name, typeName)
	}
	return nil
}

// validateReferences checks @references directives on definitions:
// - Source fields exist
// - Referenced object exists
// - References fields exist in target object
// - Field count matches
// - Field types match between source and target
// Also validates @field_references directives on individual fields.
func validateReferences(ctx base.CompilationContext, def *ast.Definition) error {
	// Validate definition-level @references
	for _, dir := range def.Directives {
		if dir.Name != "references" {
			continue
		}
		if err := validateReferencesDirective(ctx, def, dir); err != nil {
			return err
		}
	}

	// Validate field-level @field_references
	for _, f := range def.Fields {
		frDir := f.Directives.ForName("field_references")
		if frDir == nil {
			continue
		}
		if err := validateFieldReferences(ctx, def, f, frDir); err != nil {
			return err
		}
	}

	return nil
}

func validateReferencesDirective(ctx base.CompilationContext, def *ast.Definition, dir *ast.Directive) error {
	refName := base.DirectiveArgString(dir, "references_name")
	sourceFields := base.DirectiveArgStrings(dir, "source_fields")
	refsFields := base.DirectiveArgStrings(dir, "references_fields")

	// 1. Source fields exist
	for _, fieldName := range sourceFields {
		if !hasField(def, fieldName) {
			return gqlerror.ErrorPosf(dir.Position,
				"@references on %q refers to unknown field %q", def.Name, fieldName)
		}
	}

	// 2. Referenced object exists
	refDef := ctx.Source().ForName(ctx.Context(), refName)
	if refDef == nil {
		refDef = ctx.LookupType(refName)
	}
	if refDef == nil {
		return gqlerror.ErrorPosf(dir.Position,
			"@references on %q: referenced object %q not found", def.Name, refName)
	}

	// 3. If references_fields not specified, infer from PK of target
	if len(refsFields) == 0 {
		for _, f := range refDef.Fields {
			if f.Directives.ForName("pk") != nil {
				refsFields = append(refsFields, f.Name)
			}
		}
		// If we still can't infer, skip type checking — ReferencesRule will handle defaults
		if len(refsFields) == 0 {
			return nil
		}
	}

	// 4. Field count must match
	if len(sourceFields) != len(refsFields) {
		return gqlerror.ErrorPosf(dir.Position,
			"@references on %q: fields (%d) and references_fields (%d) must have the same number of entries",
			def.Name, len(sourceFields), len(refsFields))
	}

	// 5. References fields exist in target and types match
	for i, sfn := range sourceFields {
		sf := def.Fields.ForName(sfn)
		if sf == nil {
			continue // already checked above
		}
		rfn := refsFields[i]
		rf := refDef.Fields.ForName(rfn)
		if rf == nil {
			return gqlerror.ErrorPosf(dir.Position,
				"@references on %q: references field %q not found in %q",
				def.Name, rfn, refName)
		}
		if !equalTypes(sf.Type, rf.Type) {
			return gqlerror.ErrorPosf(dir.Position,
				"@references on %q: field %q (%s) and references field %q (%s) in %q must have the same type",
				def.Name, sfn, typeString(sf.Type), rfn, typeString(rf.Type), refName)
		}
	}

	return nil
}

func validateFieldReferences(ctx base.CompilationContext, def *ast.Definition, field *ast.FieldDefinition, dir *ast.Directive) error {
	refName := base.DirectiveArgString(dir, "references_name")
	refField := base.DirectiveArgString(dir, "field")

	// Referenced object exists
	refDef := ctx.Source().ForName(ctx.Context(), refName)
	if refDef == nil {
		refDef = ctx.LookupType(refName)
	}
	if refDef == nil {
		return gqlerror.ErrorPosf(dir.Position,
			"@field_references on %s.%s: referenced object %q not found",
			def.Name, field.Name, refName)
	}

	// Reference field exists
	if refField == "" {
		// Default to field name
		refField = field.Name
	}
	rf := refDef.Fields.ForName(refField)
	if rf == nil {
		return gqlerror.ErrorPosf(dir.Position,
			"@field_references on %s.%s: field %q not found in %q",
			def.Name, field.Name, refField, refName)
	}

	// Types match
	if !equalTypes(field.Type, rf.Type) {
		return gqlerror.ErrorPosf(dir.Position,
			"@field_references on %s.%s: type %s doesn't match referenced field %s.%s type %s",
			def.Name, field.Name, typeString(field.Type), refName, refField, typeString(rf.Type))
	}

	return nil
}

// typeString returns a human-readable type representation.
func typeString(t *ast.Type) string {
	if t == nil {
		return "<nil>"
	}
	if t.Elem != nil {
		s := "[" + typeString(t.Elem) + "]"
		if t.NonNull {
			s += "!"
		}
		return s
	}
	s := t.NamedType
	if t.NonNull {
		s += "!"
	}
	return s
}

func hasField(def *ast.Definition, name string) bool {
	for _, f := range def.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

func directiveKind(isTable bool) string {
	if isTable {
		return "@table"
	}
	return "@view"
}
