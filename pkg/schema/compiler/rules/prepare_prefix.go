package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*PrefixPreparer)(nil)

type PrefixPreparer struct{}

func (r *PrefixPreparer) Name() string     { return "PrefixPreparer" }
func (r *PrefixPreparer) Phase() base.Phase { return base.PhasePrepare }

func (r *PrefixPreparer) ProcessAll(ctx base.CompilationContext) error {
	opts := ctx.CompileOptions()

	// Collect all source definition names for prefix mapping
	sourceNames := make(map[string]bool)
	for def := range ctx.Source().Definitions(ctx.Context()) {
		sourceNames[def.Name] = true
	}

	// Process each definition
	for def := range ctx.Source().Definitions(ctx.Context()) {
		isTable := def.Directives.ForName("table") != nil
		isView := def.Directives.ForName("view") != nil
		isFunc := def.Directives.ForName("function") != nil
		isM2M := false
		tableName := ""

		if isTable {
			tableDir := def.Directives.ForName("table")
			if arg := tableDir.Arguments.ForName("name"); arg != nil {
				tableName = arg.Value.Raw
			}
			if arg := tableDir.Arguments.ForName("is_m2m"); arg != nil {
				isM2M = arg.Value.Raw == "true"
			}
		}
		if isView {
			viewDir := def.Directives.ForName("view")
			if arg := viewDir.Arguments.ForName("name"); arg != nil {
				tableName = arg.Value.Raw
			}
		}

		originalName := def.Name

		// Collect PK fields
		var pks []string
		for _, f := range def.Fields {
			if f.Directives.ForName("pk") != nil {
				pks = append(pks, f.Name)
			}
		}

		// Register ObjectInfo for data objects
		if isTable || isView {
			module := ""
			if opts.AsModule {
				module = opts.Name
			} else if modDir := def.Directives.ForName("module"); modDir != nil {
				if arg := modDir.Arguments.ForName("name"); arg != nil {
					module = arg.Value.Raw
				}
			}

			prefixedName := ctx.ApplyPrefix(def.Name)

			info := &base.ObjectInfo{
				Name:         prefixedName,
				OriginalName: originalName,
				TableName:    tableName,
				Module:       module,
				IsReplace:    base.IsReplaceDefinition(def),
				IsView:       isView,
				IsM2M:        isM2M,
				PrimaryKey:   pks,
			}
			ctx.RegisterObject(prefixedName, info)

			// Apply prefix to definition name (NOT for functions)
			if opts.Prefix != "" {
				def.Name = prefixedName
				// Add @original_name directive
				pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}
				def.Directives = append(def.Directives, &ast.Directive{
					Name: "original_name",
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: originalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					},
					Position: pos,
				})
			}
		}

		// For functions, register info but skip renaming
		if isFunc {
			module := ""
			if opts.AsModule {
				module = opts.Name
			}
			info := &base.ObjectInfo{
				Name:         def.Name,
				OriginalName: originalName,
				Module:       module,
			}
			ctx.RegisterObject(def.Name, info)
		}

		// Rename type references in fields to use prefixed names
		if opts.Prefix != "" {
			for _, f := range def.Fields {
				renameTypeRefs(f.Type, opts.Prefix, sourceNames)
			}
		}
	}

	return nil
}

// renameTypeRefs prefixes type names that refer to source definitions.
func renameTypeRefs(t *ast.Type, prefix string, sourceNames map[string]bool) {
	if t == nil {
		return
	}
	if t.Elem != nil {
		renameTypeRefs(t.Elem, prefix, sourceNames)
		return
	}
	if sourceNames[t.NamedType] {
		t.NamedType = prefix + t.NamedType
	}
}
