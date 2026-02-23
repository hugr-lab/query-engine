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

	if err := validateReferences(def); err != nil {
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

// validateReferences checks that fields listed in @references directives
// actually exist in the definition.
func validateReferences(def *ast.Definition) error {
	for _, dir := range def.Directives {
		if dir.Name != "references" {
			continue
		}
		fieldsArg := dir.Arguments.ForName("fields")
		if fieldsArg == nil {
			continue
		}
		for _, child := range fieldsArg.Value.Children {
			fieldName := child.Value.Raw
			if !hasField(def, fieldName) {
				return gqlerror.ErrorPosf(dir.Position,
					"@references on %q refers to unknown field %q", def.Name, fieldName)
			}
		}
	}
	return nil
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
