package rules

import (
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*ArgumentTypeValidator)(nil)

// ArgumentTypeValidator verifies that all field arguments reference valid input
// types (Scalar, Enum, InputObject) after compilation. It also validates that
// InputObject fields reference valid input types. This catches cases where
// an argument or InputObject field type is accidentally an Object (which is
// illegal in GraphQL and causes "Introspection must provide input type for
// arguments" errors).
type ArgumentTypeValidator struct{}

func (r *ArgumentTypeValidator) Name() string     { return "ArgumentTypeValidator" }
func (r *ArgumentTypeValidator) Phase() base.Phase { return base.PhaseFinalize }

func (r *ArgumentTypeValidator) ProcessAll(ctx base.CompilationContext) error {
	var errs []string

	isInputType := func(typeName string) bool {
		if ctx.IsScalar(typeName) {
			return true
		}
		switch typeName {
		case "String", "Int", "Float", "Boolean", "ID":
			return true
		}
		def := ctx.LookupType(typeName)
		if def == nil {
			// Type not found in output or provider — may be a system type
			// that hasn't been loaded yet (e.g., during bootstrap).
			return true
		}
		switch def.Kind {
		case ast.Scalar, ast.Enum, ast.InputObject:
			return true
		default:
			return false
		}
	}

	checkArgs := func(defName, fieldName string, args ast.ArgumentDefinitionList) {
		for _, arg := range args {
			typeName := arg.Type.Name()
			if !isInputType(typeName) {
				def := ctx.LookupType(typeName)
				errs = append(errs, fmt.Sprintf("%s.%s arg %q: type %q is %s, must be Scalar/Enum/InputObject", defName, fieldName, arg.Name, typeName, def.Kind))
			}
		}
	}

	checkInputObjectFields := func(def *ast.Definition) {
		if def.Kind != ast.InputObject {
			return
		}
		for _, f := range def.Fields {
			typeName := f.Type.Name()
			if !isInputType(typeName) {
				td := ctx.LookupType(typeName)
				errs = append(errs, fmt.Sprintf("InputObject %s.%s: type %q is %s, must be Scalar/Enum/InputObject", def.Name, f.Name, typeName, td.Kind))
			}
		}
	}

	// Check all output definitions
	for def := range ctx.OutputDefinitions() {
		for _, f := range def.Fields {
			checkArgs(def.Name, f.Name, f.Arguments)
		}
		checkInputObjectFields(def)
	}

	// Check extensions (Query/Mutation fields, InputObject extensions)
	for ext := range ctx.OutputExtensions() {
		for _, f := range ext.Fields {
			checkArgs(ext.Name, f.Name, f.Arguments)
		}
		checkInputObjectFields(ext)
	}

	if len(errs) > 0 {
		return fmt.Errorf("argument type validation failed:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}
