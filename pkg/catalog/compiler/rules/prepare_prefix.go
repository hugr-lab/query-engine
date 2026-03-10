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
	// Collect all source definition names for prefix mapping
	sourceNames := make(map[string]bool)
	for def := range ctx.Source().Definitions(ctx.Context()) {
		sourceNames[def.Name] = true
	}
	return PrefixAndRegister(ctx, sourceNames)
}

// PrefixAndRegister applies prefix renaming to source definitions and registers
// ObjectInfo for data objects. The sourceNames map controls which type references
// get prefixed — it must include ALL names from the catalog namespace (not just
// the definitions being processed). This is the core logic shared between
// PrefixPreparer (full compilation) and the incremental compiler (which passes
// sourceNames that include the full baseCatalog namespace).
func PrefixAndRegister(ctx base.CompilationContext, sourceNames map[string]bool) error {
	opts := ctx.CompileOptions()

	// Process each definition from the source
	for def := range ctx.Source().Definitions(ctx.Context()) {
		prefixDefinition(ctx, def, sourceNames, opts)
	}

	// Also rename type references in promoted definitions (e.g. Function from
	// InternalExtensionMerger). Their field return types may reference source
	// definitions that were just prefixed.
	if opts.Prefix != "" {
		for _, def := range ctx.PromotedDefinitions() {
			renameFieldRefs(def.Fields, opts.Prefix, sourceNames, opts.AsModule)
		}
	}

	return nil
}

// prefixDefinition applies prefix renaming to a single definition,
// registers ObjectInfo for data objects, and renames type references.
func prefixDefinition(ctx base.CompilationContext, def *ast.Definition, sourceNames map[string]bool, opts base.Options) {
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
		if opts.Prefix != "" && (!opts.IsExtension || !isView) {
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
		renameFieldRefs(def.Fields, opts.Prefix, sourceNames, opts.AsModule)
		// Rename references in definition directives
		for _, d := range def.Directives.ForNames("references") {
			RenameDirectiveArgIfSource(d, "references_name", opts.Prefix, sourceNames)
			RenameDirectiveArgIfSource(d, "m2m_name", opts.Prefix, sourceNames)
		}
		// Rename @args name argument
		if d := def.Directives.ForName("args"); d != nil {
			RenameDirectiveArgIfSource(d, "name", opts.Prefix, sourceNames)
		}
	}
}

// renameFieldRefs renames type references and directive arguments in field definitions.
func renameFieldRefs(fields ast.FieldList, prefix string, sourceNames map[string]bool, asModule bool) {
	for _, f := range fields {
		RenameTypeRefs(f.Type, prefix, sourceNames)
		// Rename type references in field arguments (e.g. input types used in function args)
		for _, arg := range f.Arguments {
			RenameTypeRefs(arg.Type, prefix, sourceNames)
		}
		for _, d := range f.Directives.ForNames("field_references") {
			RenameDirectiveArgIfSource(d, "references_name", prefix, sourceNames)
		}
		for _, d := range f.Directives.ForNames("join") {
			RenameDirectiveArgIfSource(d, "references_name", prefix, sourceNames)
		}
		if !asModule {
			for _, d := range f.Directives.ForNames("function_call") {
				RenameDirectiveArgIfSource(d, "references_name", prefix, sourceNames)
			}
			for _, d := range f.Directives.ForNames("table_function_call_join") {
				RenameDirectiveArgIfSource(d, "references_name", prefix, sourceNames)
			}
		}
	}
}

// RenameDirectiveArgIfSource prefixes a directive argument value if it matches a source definition name.
func RenameDirectiveArgIfSource(d *ast.Directive, argName, prefix string, sourceNames map[string]bool) {
	a := d.Arguments.ForName(argName)
	if a == nil || a.Value == nil {
		return
	}
	if sourceNames[a.Value.Raw] {
		a.Value.Raw = prefix + "_" + a.Value.Raw
	}
}

// RenameTypeRefs prefixes type names that refer to source definitions.
func RenameTypeRefs(t *ast.Type, prefix string, sourceNames map[string]bool) {
	if t == nil {
		return
	}
	if t.Elem != nil {
		RenameTypeRefs(t.Elem, prefix, sourceNames)
		return
	}
	if sourceNames[t.NamedType] {
		t.NamedType = prefix + "_" + t.NamedType
	}
}
