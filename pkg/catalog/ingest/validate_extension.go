package ingest

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
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

func (r *ExtensionValidator) Name() string      { return "ExtensionValidator" }
func (r *ExtensionValidator) Phase() base.Phase { return base.PhaseValidate }

func (r *ExtensionValidator) ProcessAll(ctx base.CompilationContext) error {
	if !ctx.CompileOptions().IsExtension {
		return validateNoCrossSourceExtends(ctx)
	}

	for def := range ctx.Source().Definitions(ctx.Context()) {
		if err := validateExtensionDef(def); err != nil {
			return err
		}
	}
	// Also validate extensions (e.g. "extend type Function { ... }") which are
	// kept separate from definitions by ExtensionsSource implementations.
	if extSrc, ok := ctx.Source().(base.ExtensionsSource); ok {
		for ext := range extSrc.Extensions(ctx.Context()) {
			if err := validateExtensionDef(ext); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateNoCrossSourceExtends enforces, for a source that is NOT an extension,
// the contract the whole catalog rests on: a regular source describes ONLY its
// own data.
//
// `extend type X` where X belongs to another source is how a source reaches
// across the seam — it is the same act as an `@join` or `@function_call` to a
// foreign target, which JoinValidator and FunctionCallValidator already refuse
// outside an extension source (rules 2b / 1b). Reached through an `extend type`
// it was refused nowhere: InternalExtensionMerger asks only whether the target
// resolves in the provider, never who is asking. So a plain source could
// contribute fields to another source's objects, which then outlive their
// declarer in ways nothing accounts for — the storage attributes them to the
// source the DATA comes from, and the dependency gating that makes extensions
// safe is only wired for extension sources.
//
// The module ROOTS are not cross-source: every source declares its functions
// and subscriptions by extending them, and they are engine-owned, not another
// source's property.
func validateNoCrossSourceExtends(ctx base.CompilationContext) error {
	extSrc, ok := ctx.Source().(base.ExtensionsSource)
	if !ok {
		return nil
	}
	for ext := range extSrc.Extensions(ctx.Context()) {
		switch ext.Name {
		case base.QueryBaseName, base.MutationBaseName, base.SubscriptionBaseName,
			base.FunctionTypeName, base.FunctionMutationTypeName:
			continue
		}
		if base.ModuleRootInfo(ext) != nil {
			continue
		}
		// The source's own type: `extend type Foo` next to `type Foo` is a
		// same-source merge, which PREPARE does in place.
		if ctx.Source().ForName(ctx.Context(), ext.Name) != nil {
			continue
		}
		// Three ways to get here, and they want different advice.
		target := ctx.LookupType(ext.Name)
		switch {
		case target == nil:
			// A typo. It used to vanish without a trace: InternalExtensionMerger
			// skips an unresolvable target silently, so the fields were simply
			// never contributed and nothing said why.
			return gqlerror.ErrorPosf(ext.Position,
				"extend type %s: no such type", ext.Name)
		case base.DefinitionCatalog(target) == "":
			// A system type — the engine's, not a data source's.
			return gqlerror.ErrorPosf(ext.Position,
				"extend type %s: system types cannot be extended", ext.Name)
		default:
			return gqlerror.ErrorPosf(ext.Position,
				"extend type %s: it belongs to data source %q — a regular source describes "+
					"only its own data, and cross-source extensions belong to an extension source",
				ext.Name, base.DefinitionCatalog(target))
		}
	}
	return nil
}

var _ base.BatchRule = (*DependencyCollector)(nil)

// DependencyCollector collects @dependency directives from source definitions
// AND extensions during the VALIDATE phase and registers them on the compilation
// context. Only active when opts.IsExtension is true.
//
// This must be a BatchRule (not DefinitionRule) because extension sources
// primarily contain "extend type" blocks that are extensions, not definitions.
// DefinitionRules only iterate source.Definitions(), missing extensions entirely.
type DependencyCollector struct{}

func (r *DependencyCollector) Name() string      { return "DependencyCollector" }
func (r *DependencyCollector) Phase() base.Phase { return base.PhaseValidate }

func (r *DependencyCollector) ProcessAll(ctx base.CompilationContext) error {
	if !ctx.CompileOptions().IsExtension {
		return nil
	}

	// Collect from source definitions
	for def := range ctx.Source().Definitions(ctx.Context()) {
		collectDeps(ctx, def)
	}

	// Collect from source extensions (extend type blocks)
	if extSrc, ok := ctx.Source().(base.ExtensionsSource); ok {
		for ext := range extSrc.Extensions(ctx.Context()) {
			collectDeps(ctx, ext)
		}
	}
	return nil
}

func collectDeps(ctx base.CompilationContext, def *ast.Definition) {
	for _, name := range base.DefinitionDependencies(def) {
		if name != "" {
			ctx.RegisterDependency(name)
		}
	}
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

		// No @sql fields on extension types (only @join, @function_call, @table_function_call_join, @references allowed)
		for _, f := range def.Fields {
			if f.Directives.ForName("sql") != nil {
				return gqlerror.ErrorPosf(f.Position,
					"extension definition %s: @sql fields are not allowed on extension types", def.Name)
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
