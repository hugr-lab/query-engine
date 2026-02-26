package rules

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ base.BatchRule = (*ExtensionValidator)(nil)

// ExtensionValidator validates extension source constraints during the VALIDATE phase.
// Only active when opts.IsExtension is true.
// Extension definitions:
//   - Can only contain views (no @table-only objects)
//   - Cannot contain modules, functions, system types, or scalars
type ExtensionValidator struct{}

func (r *ExtensionValidator) Name() string     { return "ExtensionValidator" }
func (r *ExtensionValidator) Phase() base.Phase { return base.PhaseValidate }

func (r *ExtensionValidator) ProcessAll(ctx base.CompilationContext) error {
	if !ctx.CompileOptions().IsExtension {
		return nil
	}

	for def := range ctx.Source().Definitions(ctx.Context()) {
		if err := validateExtensionDef(def); err != nil {
			return err
		}
	}
	return nil
}

var _ base.DefinitionRule = (*DependencyCollector)(nil)

// DependencyCollector collects @dependency directives from source definitions
// during the VALIDATE phase and registers them on the compilation context.
// Only active when opts.IsExtension is true.
type DependencyCollector struct{}

func (r *DependencyCollector) Name() string     { return "DependencyCollector" }
func (r *DependencyCollector) Phase() base.Phase { return base.PhaseValidate }

func (r *DependencyCollector) Match(def *ast.Definition) bool {
	return def.Directives.ForName(base.DependencyDirectiveName) != nil
}

func (r *DependencyCollector) Process(ctx base.CompilationContext, def *ast.Definition) error {
	if !ctx.CompileOptions().IsExtension {
		return nil
	}
	for _, name := range base.DefinitionDependencies(def) {
		if name != "" {
			ctx.RegisterDependency(name)
		}
	}
	return nil
}

func validateExtensionDef(def *ast.Definition) error {
	if def.Kind == ast.Object {
		hasTable := def.Directives.ForName("table") != nil
		hasView := def.Directives.ForName("view") != nil
		// Data objects: only views allowed
		if hasTable && !hasView {
			return gqlerror.ErrorPosf(def.Position,
				"extension definition %s can't contain data objects (tables)", def.Name)
		}

		// No modules
		if def.Directives.ForName("module_root") != nil {
			return gqlerror.ErrorPosf(def.Position,
				"extension definition %s can't contain modules", def.Name)
		}

		// No functions (Function/MutationFunction types with @function fields)
		if def.Name == "Function" || def.Name == "MutationFunction" {
			for _, f := range def.Fields {
				if f.Name != "_stub" && f.Name != "_placeholder" && f.Directives.ForName("function") != nil {
					return gqlerror.ErrorPosf(def.Position,
						"extension definition %s can't contain functions", def.Name)
				}
			}
		}
	}

	// No scalar types
	if def.Kind == ast.Scalar {
		return gqlerror.ErrorPosf(def.Position,
			"extension definition %s can't contain system types", def.Name)
	}
	// No system types
	if def.Directives.ForName("system") != nil || strings.HasPrefix(def.Name, "__") {
		return gqlerror.ErrorPosf(def.Position,
			"extension definition %s can't contain system types", def.Name)
	}

	return nil
}
