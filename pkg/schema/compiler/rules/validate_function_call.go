package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ base.BatchRule = (*FunctionCallValidator)(nil)

// FunctionCallValidator validates @function_call and @table_function_call_join
// directives on fields in data objects. It checks:
// - Referenced function exists in Function/MutationFunction types
// - Return type matches the function definition
// - Field arguments exist in function definition and types match
// - Args map entries reference valid function arguments
// - Args map source fields exist in the parent object and types match
// - All required function arguments are provided
//
// Runs in FINALIZE phase because Function types are created during GENERATE.
type FunctionCallValidator struct{}

func (r *FunctionCallValidator) Name() string     { return "FunctionCallValidator" }
func (r *FunctionCallValidator) Phase() base.Phase { return base.PhaseFinalize }

func (r *FunctionCallValidator) ProcessAll(ctx base.CompilationContext) error {
	for name := range ctx.Objects() {
		def := ctx.LookupType(name)
		if def == nil {
			continue
		}
		for _, f := range def.Fields {
			if err := validateFuncCallField(ctx, def, f); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateFuncCallField(ctx base.CompilationContext, def *ast.Definition, field *ast.FieldDefinition) error {
	fcDir := field.Directives.ForName("function_call")
	tfjDir := field.Directives.ForName("table_function_call_join")
	if fcDir == nil && tfjDir == nil {
		return nil
	}

	dir := fcDir
	isTableFuncJoin := false
	if dir == nil {
		dir = tfjDir
		isTableFuncJoin = true
	}

	refName := base.DirectiveArgString(dir, "references_name")
	moduleName := base.DirectiveArgString(dir, "module")
	argsMap := extractArgsMap(dir)

	// 1. Resolve the function definition
	funcField := resolveFunctionField(ctx, refName, moduleName)
	if funcField == nil {
		return gqlerror.ErrorPosf(field.Position,
			"%s.%s: unknown function %q", def.Name, field.Name, refName)
	}

	// 2. Validate return type
	if !isTableFuncJoin {
		if !equalTypes(funcField.Type, field.Type) {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q return type should be %s, same as in the function definition",
				def.Name, field.Name, refName, field.Type.Name())
		}
	} else {
		// For table_function_call_join: name must match, function returns list
		if funcField.Type.Name() != field.Type.Name() || funcField.Type.NamedType != "" {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q return type should be %s, same as in the function definition",
				def.Name, field.Name, refName, field.Type.Name())
		}
	}

	// 3. Propagate @catalog from function to field if missing
	if field.Directives.ForName("catalog") == nil {
		if funcCatalog := funcField.Directives.ForName("catalog"); funcCatalog != nil {
			field.Directives = append(field.Directives, funcCatalog)
		}
	}

	// 4. Validate field arguments exist in function and types match
	usedArgs := make(map[string]struct{})
	for _, arg := range field.Arguments {
		funcArg := funcField.Arguments.ForName(arg.Name)
		if funcArg == nil {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q doesn't have argument %q",
				def.Name, field.Name, refName, arg.Name)
		}
		if _, ok := argsMap[arg.Name]; ok {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function argument %q is redefined in args",
				def.Name, field.Name, arg.Name)
		}
		if !equalTypes(funcArg.Type, arg.Type) {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q argument %q type should be %s, same as in the function definition",
				def.Name, field.Name, refName, arg.Name, arg.Type.Name())
		}
		usedArgs[arg.Name] = struct{}{}
	}

	// 5. Validate args map entries
	for argName, sourcePath := range argsMap {
		funcArg := funcField.Arguments.ForName(argName)
		if funcArg == nil {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q doesn't have argument %q",
				def.Name, field.Name, refName, argName)
		}
		// Validate source field exists and type matches
		sourceField := findFieldByPath(ctx, def, sourcePath)
		if sourceField == nil {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q argument %q source field %q not found in %s",
				def.Name, field.Name, refName, argName, sourcePath, def.Name)
		}
		if !equalTypes(sourceField.Type, funcArg.Type) {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q argument %q type should be %s, same as in the function definition",
				def.Name, field.Name, refName, argName, sourceField.Type.Name())
		}
		usedArgs[argName] = struct{}{}
	}

	// 6. Check all required function arguments are provided
	for _, funcArg := range funcField.Arguments {
		if _, ok := usedArgs[funcArg.Name]; !ok && funcArg.DefaultValue == nil && funcArg.Type.NonNull {
			return gqlerror.ErrorPosf(field.Position,
				"%s.%s: function %q required argument %q is not provided",
				def.Name, field.Name, refName, funcArg.Name)
		}
	}

	return nil
}

// resolveFunctionField looks up a function field in Function or module function types.
func resolveFunctionField(ctx base.CompilationContext, refName, moduleName string) *ast.FieldDefinition {
	// If module specified, look in module function type
	if moduleName != "" {
		modFuncTypeName := "_module_" + moduleName + "_function"
		modFuncDef := ctx.LookupType(modFuncTypeName)
		if modFuncDef != nil {
			if f := modFuncDef.Fields.ForName(refName); f != nil {
				return f
			}
		}
	}

	// Look in root Function type
	funcDef := ctx.LookupType("Function")
	if funcDef != nil {
		if f := funcDef.Fields.ForName(refName); f != nil {
			return f
		}
	}

	// Look in MutationFunction type
	mutFuncDef := ctx.LookupType("MutationFunction")
	if mutFuncDef != nil {
		if f := mutFuncDef.Fields.ForName(refName); f != nil {
			return f
		}
	}

	return nil
}

// extractArgsMap extracts the args map from a function_call/table_function_call_join directive.
// The args argument is an object value like: args: {code: "iata_code", radius: "distance"}
func extractArgsMap(dir *ast.Directive) map[string]string {
	result := make(map[string]string)
	argsArg := dir.Arguments.ForName("args")
	if argsArg == nil || argsArg.Value == nil {
		return result
	}
	for _, child := range argsArg.Value.Children {
		result[child.Name] = child.Value.Raw
	}
	return result
}
