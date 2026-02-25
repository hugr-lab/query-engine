package rules

import (
	"strings"

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
	// Build function registry: map function name → field definition.
	// Includes functions from root Function/MutationFunction types AND module function types.
	funcRegistry := buildFuncRegistry(ctx)

	for name := range ctx.Objects() {
		def := ctx.LookupType(name)
		if def == nil {
			continue
		}
		for _, f := range def.Fields {
			if err := validateFuncCallField(ctx, def, f, funcRegistry); err != nil {
				return err
			}
		}
	}
	return nil
}

// buildFuncRegistry collects all function fields from Function, MutationFunction,
// and module function types into a single lookup map by field name.
func buildFuncRegistry(ctx base.CompilationContext) map[string]*ast.FieldDefinition {
	registry := make(map[string]*ast.FieldDefinition)

	// Collect from root Function type
	collectFuncsFromType(ctx, "Function", registry)
	// Collect from root MutationFunction type
	collectFuncsFromType(ctx, "MutationFunction", registry)

	// Collect from module function types: they follow the pattern _module_<name>_function.
	// Also check parent module paths because functions may be registered at a higher
	// module level (e.g., a function with module "transport_db" is visible to objects
	// with module "transport_db.transport.air").
	checked := make(map[string]bool)
	for name := range ctx.Objects() {
		info := ctx.GetObject(name)
		if info == nil || info.Module == "" {
			continue
		}
		// Check all module paths from the full module down to the top-level
		parts := strings.Split(info.Module, ".")
		for i := len(parts); i > 0; i-- {
			mod := strings.Join(parts[:i], ".")
			modFuncTypeName := "_module_" + strings.ReplaceAll(mod, ".", "_") + "_function"
			if !checked[modFuncTypeName] {
				checked[modFuncTypeName] = true
				collectFuncsFromType(ctx, modFuncTypeName, registry)
			}
			modMutFuncTypeName := "_module_" + strings.ReplaceAll(mod, ".", "_") + "_mutation_function"
			if !checked[modMutFuncTypeName] {
				checked[modMutFuncTypeName] = true
				collectFuncsFromType(ctx, modMutFuncTypeName, registry)
			}
		}
	}

	return registry
}

func collectFuncsFromType(ctx base.CompilationContext, typeName string, registry map[string]*ast.FieldDefinition) {
	if def := ctx.LookupType(typeName); def != nil {
		for _, f := range def.Fields {
			registry[f.Name] = f
		}
	}
	if ext := ctx.LookupExtension(typeName); ext != nil {
		for _, f := range ext.Fields {
			registry[f.Name] = f
		}
	}
}

func validateFuncCallField(ctx base.CompilationContext, def *ast.Definition, field *ast.FieldDefinition, funcRegistry map[string]*ast.FieldDefinition) error {
	fcDir := field.Directives.ForName(base.FunctionCallDirectiveName)
	tfjDir := field.Directives.ForName(base.FunctionCallTableJoinDirectiveName)
	if fcDir == nil && tfjDir == nil {
		return nil
	}

	dir := fcDir
	isTableFuncJoin := false
	if dir == nil {
		dir = tfjDir
		isTableFuncJoin = true
	}

	refName := base.DirectiveArgString(dir, base.ArgReferencesName)
	argsMap := extractArgsMap(dir)

	// 1. Resolve the function definition using the pre-built registry
	funcField := funcRegistry[refName]
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
	if field.Directives.ForName(base.CatalogDirectiveName) == nil {
		if funcCatalog := funcField.Directives.ForName(base.CatalogDirectiveName); funcCatalog != nil {
			field.Directives = append(field.Directives, funcCatalog)
		}
	}

	// 3b. Propagate module from function's @module directive to the function_call/
	// table_function_call_join directive. Skip for extension fields (@dependency)
	// since they may reference functions from a different catalog.
	if funcModule := funcModuleName(funcField); funcModule != "" && field.Directives.ForName(base.DependencyDirectiveName) == nil {
		base.SetDirectiveArg(dir, base.ArgModule, funcModule)
	}

	// 4. Validate field arguments that correspond to function arguments.
	// Field arguments may also include compiler-added args (e.g., geometry transforms,
	// subquery filter/limit) which are not function arguments and should be skipped.
	usedArgs := make(map[string]struct{})
	for _, arg := range field.Arguments {
		funcArg := funcField.Arguments.ForName(arg.Name)
		if funcArg == nil {
			// Not a function argument — skip (could be compiler-added like transforms)
			continue
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
		if !equalTypesIgnoreNull(sourceField.Type, funcArg.Type) {
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


// extractArgsMap extracts the args map from a function_call/table_function_call_join directive.
// The args argument is an object value like: args: {code: "iata_code", radius: "distance"}
func extractArgsMap(dir *ast.Directive) map[string]string {
	result := make(map[string]string)
	argsArg := dir.Arguments.ForName(base.ArgArgs)
	if argsArg == nil || argsArg.Value == nil {
		return result
	}
	for _, child := range argsArg.Value.Children {
		result[child.Name] = child.Value.Raw
	}
	return result
}

// funcModuleName returns the module name from a function field's @module directive.
func funcModuleName(funcField *ast.FieldDefinition) string {
	return base.DirectiveArgString(funcField.Directives.ForName(base.ModuleDirectiveName), base.ArgName)
}
