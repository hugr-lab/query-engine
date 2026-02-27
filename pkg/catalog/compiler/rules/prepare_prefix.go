package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
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
			if arg := tableDir.Arguments.ForName(base.ArgName); arg != nil {
				tableName = arg.Value.Raw
			}
			if arg := tableDir.Arguments.ForName(base.ArgIsM2M); arg != nil {
				isM2M = arg.Value.Raw == "true"
			}
		}
		if isView {
			viewDir := def.Directives.ForName("view")
			if arg := viewDir.Arguments.ForName(base.ArgName); arg != nil {
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
				// Concatenate with inline @module if present (e.g., "transport" + "air" → "transport.air")
				// Also update the directive value to match the old compiler's behavior.
				if modDir := def.Directives.ForName(base.ModuleDirectiveName); modDir != nil {
					if arg := modDir.Arguments.ForName(base.ArgName); arg != nil {
						if arg.Value.Raw != "" {
							module = opts.Name + "." + arg.Value.Raw
						}
						arg.Value.Raw = module
					}
				}
			} else if modDir := def.Directives.ForName(base.ModuleDirectiveName); modDir != nil {
				if arg := modDir.Arguments.ForName(base.ArgName); arg != nil {
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
				IsCube:       def.Directives.ForName("cube") != nil,
				IsHypertable: def.Directives.ForName("hypertable") != nil,
				PrimaryKey:   pks,
			}
			ctx.RegisterObject(prefixedName, info)

			// Apply prefix to definition name (NOT for functions, NOT for extension views)
			if opts.Prefix != "" && !(opts.IsExtension && isView) {
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

		// Apply prefix to non-data definitions (plain Object, InputObject, Interface, Union, Enum)
		// Data objects (table/view) are handled above; functions skip renaming.
		// Also skip Function/MutationFunction types — they're system-level containers
		// processed by FunctionRule which matches on exact name.
		if opts.Prefix != "" && !isTable && !isView && !isFunc &&
			def.Name != "Function" && def.Name != "MutationFunction" {
			switch def.Kind {
			case ast.Object, ast.InputObject, ast.Interface, ast.Enum:
				originalName := def.Name
				def.Name = ctx.ApplyPrefix(def.Name)
				pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}
				def.Directives = append(def.Directives, &ast.Directive{
					Name: "original_name",
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: originalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					},
					Position: pos,
				})
				// Rename interface references
				for i, iface := range def.Interfaces {
					if sourceNames[iface] {
						def.Interfaces[i] = opts.Prefix + "_" + iface
					}
				}
			case ast.Union:
				originalName := def.Name
				def.Name = ctx.ApplyPrefix(def.Name)
				pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}
				def.Directives = append(def.Directives, &ast.Directive{
					Name: "original_name",
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: originalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					},
					Position: pos,
				})
				// Rename union member types
				for i, typeName := range def.Types {
					if sourceNames[typeName] {
						def.Types[i] = opts.Prefix + "_" + typeName
					}
				}
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

		// Rename type references in fields and directives to use prefixed names
		if opts.Prefix != "" {
			for _, f := range def.Fields {
				renameTypeRefs(f.Type, opts.Prefix, sourceNames)
				// Rename references in field directives
				for _, d := range f.Directives.ForNames("field_references") {
					renameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
				}
				// Rename references_name in @join directives
				for _, d := range f.Directives.ForNames("join") {
					renameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
				}
				// Rename references_name in function call directives
				if !opts.AsModule {
					for _, d := range f.Directives.ForNames("function_call") {
						renameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
					}
					for _, d := range f.Directives.ForNames("table_function_call_join") {
						renameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
					}
				}
			}
			// Rename references in definition directives
			for _, d := range def.Directives.ForNames("references") {
				renameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
				renameDirectiveArgIfSource(d, "m2m_name", opts.Prefix, sourceNames)
			}
			// Rename @args name argument
			if d := def.Directives.ForName("args"); d != nil {
				renameDirectiveArgIfSource(d, "name", opts.Prefix, sourceNames)
			}
		}
	}

	// Also rename type references in promoted definitions (e.g. Function from
	// InternalExtensionMerger). Their field return types may reference source
	// definitions that were just prefixed.
	if opts.Prefix != "" {
		for _, def := range ctx.PromotedDefinitions() {
			for _, f := range def.Fields {
				renameTypeRefs(f.Type, opts.Prefix, sourceNames)
				for _, d := range f.Directives.ForNames("field_references") {
					renameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
				}
				for _, d := range f.Directives.ForNames("table_function_call_join") {
					renameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
				}
			}
		}
	}

	return nil
}

// renameDirectiveArgIfSource prefixes a directive argument value if it matches a source definition name.
func renameDirectiveArgIfSource(d *ast.Directive, argName, prefix string, sourceNames map[string]bool) {
	a := d.Arguments.ForName(argName)
	if a == nil || a.Value == nil {
		return
	}
	if sourceNames[a.Value.Raw] {
		a.Value.Raw = prefix + "_" + a.Value.Raw
	}
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
		t.NamedType = prefix + "_" + t.NamedType
	}
}
