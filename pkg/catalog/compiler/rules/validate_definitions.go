package rules

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
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
		// Validate @function(sql:) parameter references on Function/MutationFunction types
		if def.Name == "Function" || def.Name == "MutationFunction" {
			if err := validateFunctionSQL(def); err != nil {
				return err
			}
		}
		// Validate @view + @args consistency
		if def.Directives.ForName(base.ObjectViewDirectiveName) != nil && def.Directives.ForName(base.ViewArgsDirectiveName) != nil {
			if err := validateViewArgs(ctx, def); err != nil {
				return err
			}
		}
		// Validate @arg_default on field arguments and on input type fields.
		// Catches all definitions — fields with @function/@function_call/
		// @table_function_call_join, plus input types used as @args(name:).
		if err := validateArgDefaults(def); err != nil {
			return err
		}
	}
	// Also validate extensions for SQL refs and @arg_default usage.
	if extSrc, ok := ctx.Source().(base.ExtensionsSource); ok {
		for ext := range extSrc.Extensions(ctx.Context()) {
			if ext.Name == "Function" || ext.Name == "MutationFunction" {
				if err := validateFunctionSQL(ext); err != nil {
					return err
				}
			}
			if err := validateArgDefaults(ext); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateArgDefaults walks a type definition and validates every @arg_default
// directive it contains:
//
//   - On field arguments: only allowed when the field has @function,
//     @function_call, or @table_function_call_join.
//   - On input type fields: always allowed (input types are validated by
//     consumers like @args(name:)).
//
// Each directive's value must be a known placeholder, and the underlying
// argument/field must not have a GraphQL default value.
func validateArgDefaults(def *ast.Definition) error {
	if def.Kind == ast.InputObject {
		for _, field := range def.Fields {
			if err := validateArgDefaultDirective(
				field.Directives.ForName(base.ArgDefaultDirectiveName),
				field.Name, field.DefaultValue, field.Position,
			); err != nil {
				return err
			}
		}
		return nil
	}
	for _, field := range def.Fields {
		isFunctionLike := field.Directives.ForName(base.FunctionDirectiveName) != nil ||
			field.Directives.ForName(base.FunctionCallDirectiveName) != nil ||
			field.Directives.ForName(base.FunctionCallTableJoinDirectiveName) != nil
		for _, arg := range field.Arguments {
			d := arg.Directives.ForName(base.ArgDefaultDirectiveName)
			if d == nil {
				continue
			}
			if !isFunctionLike {
				return gqlerror.ErrorPosf(d.Position,
					"@arg_default on %s.%s argument %q: only valid on arguments of fields with @function, @function_call, or @table_function_call_join",
					def.Name, field.Name, arg.Name)
			}
			if err := validateArgDefaultDirective(d, arg.Name, arg.DefaultValue, arg.Position); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateArgDefaultDirective(d *ast.Directive, name string, defaultValue *ast.Value, pos *ast.Position) error {
	if d == nil {
		return nil
	}
	value := base.DirectiveArgString(d, base.ArgValue)
	if value == "" {
		return gqlerror.ErrorPosf(d.Position,
			"@arg_default on %q: value is required", name)
	}
	if !sdl.IsKnownPlaceholder(value) {
		return gqlerror.ErrorPosf(d.Position,
			"@arg_default on %q: placeholder %q is not a known context variable", name, value)
	}
	if defaultValue != nil {
		return gqlerror.ErrorPosf(pos,
			"@arg_default on %q: argument cannot have both default value and @arg_default directive", name)
	}
	return nil
}

func validateDefinition(ctx base.CompilationContext, def *ast.Definition) error {
	isTable := def.Directives.ForName(base.ObjectTableDirectiveName) != nil
	isView := def.Directives.ForName(base.ObjectViewDirectiveName) != nil

	// Validate @pk fields are scalar types (if present)
	if isTable || isView {
		if err := validatePrimaryKeyTypes(ctx, def); err != nil {
			return err
		}
	}

	// Validate @cube and @hypertable require @table or @view
	if def.Directives.ForName(base.ObjectCubeDirectiveName) != nil && !isTable && !isView {
		return gqlerror.ErrorPosf(def.Position,
			"object %q: @cube requires @table or @view directive", def.Name)
	}
	if def.Directives.ForName(base.ObjectHyperTableDirectiveName) != nil && !isTable && !isView {
		return gqlerror.ErrorPosf(def.Position,
			"object %q: @hypertable requires @table or @view directive", def.Name)
	}

	if err := validateFieldTypes(ctx, def); err != nil {
		return err
	}

	// Validate @measurement and @timescale_key field directives
	if err := validateCubeHypertableFields(ctx, def); err != nil {
		return err
	}

	if err := validateReferences(ctx, def); err != nil {
		return err
	}

	return nil
}

// validateCubeHypertableFields validates @measurement and @timescale_key field directives.
func validateCubeHypertableFields(ctx base.CompilationContext, def *ast.Definition) error {
	isCube := def.Directives.ForName(base.ObjectCubeDirectiveName) != nil
	isHypertable := def.Directives.ForName(base.ObjectHyperTableDirectiveName) != nil

	for _, f := range def.Fields {
		// @measurement requires @cube on parent object
		if mDir := f.Directives.ForName(base.FieldMeasurementDirectiveName); mDir != nil {
			if !isCube {
				return gqlerror.ErrorPosf(mDir.Position,
					"field %q of %q: @measurement requires @cube directive on the object", f.Name, def.Name)
			}
			// Must be a named scalar type
			if f.Type.NamedType == "" || !ctx.IsScalar(f.Type.Name()) {
				return gqlerror.ErrorPosf(mDir.Position,
					"field %q of %q: @measurement field must be a scalar type", f.Name, def.Name)
			}
			// Must support measurement aggregation
			s := ctx.ScalarLookup(f.Type.Name())
			if s != nil {
				if _, ok := s.(types.MeasurementAggregatable); !ok {
					return gqlerror.ErrorPosf(mDir.Position,
						"field %q of %q: scalar type %q does not support measurement aggregation", f.Name, def.Name, f.Type.Name())
				}
			}
		}

		// @timescale_key requires @hypertable on parent object
		if tsDir := f.Directives.ForName(base.FieldTimescaleKeyDirectiveName); tsDir != nil {
			if !isHypertable {
				return gqlerror.ErrorPosf(tsDir.Position,
					"field %q of %q: @timescale_key requires @hypertable directive on the object", f.Name, def.Name)
			}
		}
	}

	return nil
}

// validatePrimaryKeyTypes checks that all @pk fields (if any) are scalar types.
func validatePrimaryKeyTypes(ctx base.CompilationContext, def *ast.Definition) error {
	for _, f := range def.Fields {
		if f.Directives.ForName(base.FieldPrimaryKeyDirectiveName) != nil {
			typeName := f.Type.Name()
			if typeName == "" || !ctx.IsScalar(typeName) {
				return gqlerror.ErrorPosf(f.Position,
					"field %q of %q: @pk field must be a scalar type, got %s", f.Name, def.Name, typeString(f.Type))
			}
		}
	}
	return nil
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
		if dir.Name != base.ReferencesDirectiveName {
			continue
		}
		if err := validateReferencesDirective(ctx, def, dir); err != nil {
			return err
		}
	}

	// Validate field-level @field_references
	for _, f := range def.Fields {
		frDir := f.Directives.ForName(base.FieldReferencesDirectiveName)
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
	refName := base.DirectiveArgString(dir, base.ArgReferencesName)
	sourceFields := base.DirectiveArgStrings(dir, base.ArgSourceFields)
	refsFields := base.DirectiveArgStrings(dir, base.ArgReferencesFields)

	// 1. Source fields exist
	// For extension types (no @table/@view), check the provider type instead
	fieldDef := def
	isTable := def.Directives.ForName(base.ObjectTableDirectiveName) != nil
	isView := def.Directives.ForName(base.ObjectViewDirectiveName) != nil
	if !isTable && !isView {
		if provDef := ctx.LookupType(def.Name); provDef != nil {
			fieldDef = provDef
		}
	}
	for _, fieldName := range sourceFields {
		if !hasField(fieldDef, fieldName) {
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
			if f.Directives.ForName(base.FieldPrimaryKeyDirectiveName) != nil {
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
		if !equalTypesIgnoreNull(sf.Type, rf.Type) {
			return gqlerror.ErrorPosf(dir.Position,
				"@references on %q: field %q (%s) and references field %q (%s) in %q must have the same type",
				def.Name, sfn, typeString(sf.Type), rfn, typeString(rf.Type), refName)
		}
	}

	return nil
}

func validateFieldReferences(ctx base.CompilationContext, def *ast.Definition, field *ast.FieldDefinition, dir *ast.Directive) error {
	refName := base.DirectiveArgString(dir, base.ArgReferencesName)
	refField := base.DirectiveArgString(dir, base.ArgField)

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

	// Types match (ignoring nullability — nullable FK to non-null PK is valid)
	if !equalTypesIgnoreNullability(field.Type, rf.Type) {
		return gqlerror.ErrorPosf(dir.Position,
			"@field_references on %s.%s: type %s doesn't match referenced field %s.%s type %s",
			def.Name, field.Name, typeString(field.Type), refName, refField, typeString(rf.Type))
	}

	return nil
}

// equalTypesIgnoreNullability compares two types by name and list structure,
// ignoring NonNull. A nullable FK referencing a non-null PK is valid.
func equalTypesIgnoreNullability(a, b *ast.Type) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.NamedType != b.NamedType {
		return false
	}
	return equalTypesIgnoreNullability(a.Elem, b.Elem)
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

// validateFunctionSQL validates [paramName] references in @function(sql: "...") strings.
// Each non-$ reference must match a declared argument on the function field.
func validateFunctionSQL(def *ast.Definition) error {
	for _, field := range def.Fields {
		funcDir := field.Directives.ForName(base.FunctionDirectiveName)
		if funcDir == nil {
			continue
		}
		sql := base.DirectiveArgString(funcDir, base.ArgSQL)
		if sql == "" {
			continue
		}
		refs := extractFieldsFromSQL(sql)
		for _, ref := range refs {
			// Skip $-prefixed system vars (e.g. [$catalog])
			if strings.HasPrefix(ref, "$") {
				continue
			}
			if field.Arguments.ForName(ref) == nil {
				return gqlerror.ErrorPosf(field.Position,
					"@function %q: sql references unknown argument %q",
					field.Name, ref)
			}
		}
	}
	return nil
}

// validateViewArgs validates @view + @args consistency:
// - The @args input type must exist and be INPUT_OBJECT
// - If @view has sql: argument, [paramName] references must be either
//   view fields, $-prefixed system vars, or input args fields
func validateViewArgs(ctx base.CompilationContext, def *ast.Definition) error {
	argsDir := def.Directives.ForName(base.ViewArgsDirectiveName)
	argInputName := base.DirectiveArgString(argsDir, base.ArgName)
	if argInputName == "" {
		return nil
	}

	// Validate input type exists
	inputDef := ctx.Source().ForName(ctx.Context(), argInputName)
	if inputDef == nil {
		inputDef = ctx.LookupType(argInputName)
	}
	if inputDef == nil {
		return gqlerror.ErrorPosf(argsDir.Position,
			"@args on %q: input type %q not found", def.Name, argInputName)
	}
	if inputDef.Kind != ast.InputObject {
		return gqlerror.ErrorPosf(argsDir.Position,
			"@args on %q: type %q must be an input type", def.Name, argInputName)
	}

	// Validate SQL parameter references if @view has sql: argument
	viewDir := def.Directives.ForName(base.ObjectViewDirectiveName)
	sql := base.DirectiveArgString(viewDir, base.ArgSQL)
	if sql == "" {
		return nil
	}

	refs := extractFieldsFromSQL(sql)
	for _, ref := range refs {
		if strings.HasPrefix(ref, "$") {
			continue
		}
		// Check if ref is a field of the view itself
		if hasField(def, ref) {
			continue
		}
		// Check if ref is a field of the args input type
		if inputDef.Fields.ForName(ref) != nil {
			continue
		}
		return gqlerror.ErrorPosf(viewDir.Position,
			"@view on %q: sql references unknown field or argument %q", def.Name, ref)
	}
	return nil
}
