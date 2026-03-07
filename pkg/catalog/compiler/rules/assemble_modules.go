package rules

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*ModuleAssembler)(nil)

// ModuleAssembler creates module type hierarchy from ObjectInfo.Module values
// and moves per-object query/mutation fields into module-scoped types.
type ModuleAssembler struct{}

func (r *ModuleAssembler) Name() string     { return "ModuleAssembler" }
func (r *ModuleAssembler) Phase() base.Phase { return base.PhaseAssemble }

func (r *ModuleAssembler) ProcessAll(ctx base.CompilationContext) error {
	pos := compiledPos("module")

	// Phase 1: Collect all fields per module.
	type moduleFields struct {
		queryFields     ast.FieldList
		mutationFields  ast.FieldList
		queryFuncFields ast.FieldList
		mutFuncFields   ast.FieldList
	}
	modules := make(map[string]*moduleFields)
	getOrCreate := func(mod string) *moduleFields {
		if mf := modules[mod]; mf != nil {
			return mf
		}
		mf := &moduleFields{}
		modules[mod] = mf
		return mf
	}

	// Collect query/mutation fields from data objects.
	for name, info := range ctx.Objects() {
		if info.Module == "" {
			continue
		}
		mf := getOrCreate(info.Module)

		// Add @module directive on the object definition (or update existing)
		if def := ctx.LookupType(name); def != nil {
			if existing := def.Directives.ForName(base.ModuleDirectiveName); existing != nil {
				if a := existing.Arguments.ForName(base.ArgName); a != nil {
					a.Value.Raw = info.Module
				}
			} else {
				def.Directives = append(def.Directives, &ast.Directive{
					Name: "module",
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: info.Module, Kind: ast.StringValue, Position: pos}, Position: pos},
					},
					Position: pos,
				})
			}
		}

		if qFields := ctx.QueryFields()[name]; len(qFields) > 0 {
			mf.queryFields = append(mf.queryFields, qFields...)
			delete(ctx.QueryFields(), name)
		}
		if mFields := ctx.MutationFields()[name]; len(mFields) > 0 {
			mf.mutationFields = append(mf.mutationFields, mFields...)
			delete(ctx.MutationFields(), name)
		}
	}

	// Collect function fields from Function/MutationFunction extensions.
	if funcExt := ctx.LookupExtension("Function"); funcExt != nil {
		var remaining ast.FieldList
		for _, f := range funcExt.Fields {
			if f.Name == "_stub" || f.Name == "_placeholder" {
				continue
			}
			mod := base.DirectiveArgString(f.Directives.ForName(base.ModuleDirectiveName), base.ArgName)
			if mod == "" {
				remaining = append(remaining, f)
				continue
			}
			mf := getOrCreate(mod)
			mf.queryFuncFields = append(mf.queryFuncFields, f)
		}
		funcExt.Fields = remaining
	}

	if mutFuncExt := ctx.LookupExtension("MutationFunction"); mutFuncExt != nil {
		var remaining ast.FieldList
		for _, f := range mutFuncExt.Fields {
			if f.Name == "_stub" || f.Name == "_placeholder" {
				continue
			}
			mod := base.DirectiveArgString(f.Directives.ForName(base.ModuleDirectiveName), base.ArgName)
			if mod == "" {
				remaining = append(remaining, f)
				continue
			}
			mf := getOrCreate(mod)
			mf.mutFuncFields = append(mf.mutFuncFields, f)
		}
		mutFuncExt.Fields = remaining
	}

	if len(modules) == 0 {
		return nil
	}

	// Sort modules for deterministic output.
	sortedMods := make([]string, 0, len(modules))
	for mod := range modules {
		sortedMods = append(sortedMods, mod)
	}
	sort.Strings(sortedMods)

	// Phase 2: Distribute fields into module type hierarchy.
	//
	// For each module that has fields of a given kind (query/mutation/function/mut_function):
	//   1. Find or create the leaf module type
	//   2. Add fields to it via extension
	//   3. Walk up the module chain, ensuring parent types exist and wiring child→parent
	//   4. Wire top-level module to the root type (Query/Mutation/Function/MutationFunction)
	//
	// Type lookup order (per the user algorithm):
	//   1. Already created by us (in this compilation) → use it
	//   2. Exists in provider (from another catalog) → extend it
	//   3. Not found → create new definition

	modCatDir := optsModuleCatalogDirective(ctx.CompileOptions())

	ensured := make(map[string]bool)
	ensureType := func(typeName, mod, modRootType string) {
		if ensured[typeName] {
			return
		}
		ensured[typeName] = true
		if ctx.LookupType(typeName) != nil {
			// Exists in provider — add @module_catalog via extension to track this catalog
			ctx.AddExtension(&ast.Definition{
				Kind:       ast.Object,
				Name:       typeName,
				Position:   pos,
				Directives: ast.DirectiveList{modCatDir},
			})
			return
		}
		ctx.AddDefinition(&ast.Definition{
			Kind:     ast.Object,
			Name:     typeName,
			Position: pos,
			Directives: ast.DirectiveList{
				{Name: "module_root", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: modRootType, Kind: ast.EnumValue, Position: pos}, Position: pos},
				}, Position: pos},
				modCatDir,
			},
		})
	}

	wired := make(map[string]bool)
	wireChild := func(parentTypeName, childName, childTypeName, desc string) {
		key := parentTypeName + "." + childName
		if wired[key] {
			return
		}
		wired[key] = true
		// Check if field already exists on the provider type
		if def := ctx.LookupType(parentTypeName); def != nil {
			for _, f := range def.Fields {
				if f.Name == childName {
					// Field exists — merge @module_catalog via extension
					ctx.AddExtension(&ast.Definition{
						Kind:     ast.Object,
						Name:     parentTypeName,
						Position: pos,
						Fields: ast.FieldList{
							{Name: childName, Directives: ast.DirectiveList{modCatDir}},
						},
					})
					return
				}
			}
		}
		ctx.AddExtension(&ast.Definition{
			Kind:     ast.Object,
			Name:     parentTypeName,
			Position: pos,
			Fields: ast.FieldList{
				{Name: childName, Description: desc, Type: ast.NamedType(childTypeName, pos), Position: pos,
					Directives: ast.DirectiveList{modCatDir}},
			},
		})
	}

	rootWired := make(map[string]bool)

	// --- Query module types ---
	for _, mod := range sortedMods {
		mf := modules[mod]
		if len(mf.queryFields) == 0 {
			continue
		}
		typeName := "_module_" + strings.ReplaceAll(mod, ".", "_") + "_query"
		ensureType(typeName, mod, "QUERY")
		ctx.AddExtension(&ast.Definition{
			Kind: ast.Object, Name: typeName, Position: pos,
			Fields: mf.queryFields,
		})

		parts := strings.Split(mod, ".")
		childTypeName := typeName
		for i := len(parts) - 1; i >= 1; i-- {
			parentMod := strings.Join(parts[:i], ".")
			parentTypeName := "_module_" + strings.ReplaceAll(parentMod, ".", "_") + "_query"
			ensureType(parentTypeName, parentMod, "QUERY")
			wireChild(parentTypeName, parts[i], childTypeName,
				"The root query object of the module "+strings.Join(parts[:i+1], "."))
			childTypeName = parentTypeName
		}

		topMod := parts[0]
		if !rootWired["query."+topMod] {
			rootWired["query."+topMod] = true
			ctx.RegisterQueryFields("_module_"+topMod, []*ast.FieldDefinition{
				{Name: topMod, Type: ast.NamedType(childTypeName, pos), Position: pos,
					Directives: ast.DirectiveList{modCatDir}},
			})
		}
	}

	// --- Mutation module types ---
	for _, mod := range sortedMods {
		mf := modules[mod]
		if len(mf.mutationFields) == 0 {
			continue
		}
		typeName := "_module_" + strings.ReplaceAll(mod, ".", "_") + "_mutation"
		ensureType(typeName, mod, "MUTATION")
		ctx.AddExtension(&ast.Definition{
			Kind: ast.Object, Name: typeName, Position: pos,
			Fields: mf.mutationFields,
		})

		parts := strings.Split(mod, ".")
		childTypeName := typeName
		for i := len(parts) - 1; i >= 1; i-- {
			parentMod := strings.Join(parts[:i], ".")
			parentTypeName := "_module_" + strings.ReplaceAll(parentMod, ".", "_") + "_mutation"
			ensureType(parentTypeName, parentMod, "MUTATION")
			wireChild(parentTypeName, parts[i], childTypeName,
				"The root mutation object of the module "+strings.Join(parts[:i+1], "."))
			childTypeName = parentTypeName
		}

		topMod := parts[0]
		if !rootWired["mutation."+topMod] {
			rootWired["mutation."+topMod] = true
			ctx.RegisterMutationFields("_module_"+topMod, []*ast.FieldDefinition{
				{Name: topMod, Type: ast.NamedType(childTypeName, pos), Position: pos,
					Directives: ast.DirectiveList{modCatDir}},
			})
		}
	}

	// --- Function module types ---
	for _, mod := range sortedMods {
		mf := modules[mod]
		if len(mf.queryFuncFields) == 0 {
			continue
		}
		typeName := "_module_" + strings.ReplaceAll(mod, ".", "_") + "_function"
		ensureType(typeName, mod, "FUNCTION")
		// Build function fields + aggregation fields
		tmpDef := &ast.Definition{Name: typeName, Fields: mf.queryFuncFields}
		addModuleFuncAggregations(ctx, tmpDef, pos)
		ctx.AddExtension(&ast.Definition{
			Kind: ast.Object, Name: typeName, Position: pos,
			Fields: tmpDef.Fields,
		})

		parts := strings.Split(mod, ".")
		childTypeName := typeName
		for i := len(parts) - 1; i >= 1; i-- {
			parentMod := strings.Join(parts[:i], ".")
			parentTypeName := "_module_" + strings.ReplaceAll(parentMod, ".", "_") + "_function"
			ensureType(parentTypeName, parentMod, "FUNCTION")
			wireChild(parentTypeName, parts[i], childTypeName,
				"The root function object of the module "+strings.Join(parts[:i+1], "."))
			childTypeName = parentTypeName
		}

		topMod := parts[0]
		if !rootWired["function."+topMod] {
			rootWired["function."+topMod] = true
			ctx.AddExtension(&ast.Definition{
				Kind: ast.Object, Name: "Function", Position: pos,
				Fields: ast.FieldList{
					{Name: topMod, Description: "The root function object of the module " + topMod,
						Type: ast.NamedType(childTypeName, pos), Position: pos,
						Directives: ast.DirectiveList{modCatDir}},
				},
			})
		}
	}

	// --- Mutation function module types ---
	for _, mod := range sortedMods {
		mf := modules[mod]
		if len(mf.mutFuncFields) == 0 {
			continue
		}
		typeName := "_module_" + strings.ReplaceAll(mod, ".", "_") + "_mut_function"
		ensureType(typeName, mod, "MUT_FUNCTION")
		ctx.AddExtension(&ast.Definition{
			Kind: ast.Object, Name: typeName, Position: pos,
			Fields: mf.mutFuncFields,
		})

		parts := strings.Split(mod, ".")
		childTypeName := typeName
		for i := len(parts) - 1; i >= 1; i-- {
			parentMod := strings.Join(parts[:i], ".")
			parentTypeName := "_module_" + strings.ReplaceAll(parentMod, ".", "_") + "_mut_function"
			ensureType(parentTypeName, parentMod, "MUT_FUNCTION")
			wireChild(parentTypeName, parts[i], childTypeName,
				"The root mutation function object of the module "+strings.Join(parts[:i+1], "."))
			childTypeName = parentTypeName
		}

		topMod := parts[0]
		if !rootWired["mut_function."+topMod] {
			rootWired["mut_function."+topMod] = true
			ctx.AddExtension(&ast.Definition{
				Kind: ast.Object, Name: "MutationFunction", Position: pos,
				Fields: ast.FieldList{
					{Name: topMod, Description: "The root mutation function object of the module " + topMod,
						Type: ast.NamedType(childTypeName, pos), Position: pos,
						Directives: ast.DirectiveList{modCatDir}},
				},
			})
		}
	}

	return nil
}

// addModuleFuncAggregations adds aggregation query fields to a module function type
// for function fields that return a list of data objects.
// This matches the old compiler's addAggregationQueries behavior on module function types.
func addModuleFuncAggregations(ctx base.CompilationContext, funcType *ast.Definition, pos *ast.Position) {
	opts := ctx.CompileOptions()
	var aggFields ast.FieldList

	for _, f := range funcType.Fields {
		// Only list-returning fields
		if f.Type.NamedType != "" {
			continue
		}
		targetName := f.Type.Name()
		// Target must be a data object (has @table or @view)
		targetDef := ctx.LookupType(targetName)
		if targetDef == nil || targetDef.Kind != ast.Object {
			continue
		}
		if targetDef.Directives.ForName(base.ObjectTableDirectiveName) == nil && targetDef.Directives.ForName(base.ObjectViewDirectiveName) == nil {
			continue
		}

		aggTypeName := "_" + targetName + "_aggregation"
		if ctx.LookupType(aggTypeName) == nil {
			continue
		}
		bucketAggTypeName := "_" + targetName + "_aggregation_bucket"

		// Add {name}_aggregation field
		aggFields = append(aggFields, &ast.FieldDefinition{
			Name:      f.Name + "_aggregation",
			Type:      ast.NamedType(aggTypeName, pos),
			Arguments: f.Arguments,
			Directives: ast.DirectiveList{
				funcAggQueryDirective(f.Name, false, pos),
				optsCatalogDirective(opts),
			},
			Position: pos,
		})

		// Add {name}_bucket_aggregation field
		aggFields = append(aggFields, &ast.FieldDefinition{
			Name:      f.Name + "_bucket_aggregation",
			Type:      ast.ListType(ast.NamedType(bucketAggTypeName, pos), pos),
			Arguments: f.Arguments,
			Directives: ast.DirectiveList{
				funcAggQueryDirective(f.Name, true, pos),
				optsCatalogDirective(opts),
			},
			Position: pos,
		})

		// Replace existing AGGREGATE @query directives on the target data object.
		// In the old compiler, module function types are processed before module
		// query types, so the function aggregation name wins. We replicate this
		// by replacing any existing AGGREGATE/AGGREGATE_BUCKET @query directives.
		replaceOrAddQueryDirective(targetDef, "AGGREGATE", f.Name+"_aggregation", pos)
		replaceOrAddQueryDirective(targetDef, "AGGREGATE_BUCKET", f.Name+"_bucket_aggregation", pos)
	}

	funcType.Fields = append(funcType.Fields, aggFields...)
}

// replaceOrAddQueryDirective replaces an existing @query directive of the given type
// on a definition, or adds a new one if none exists.
func replaceOrAddQueryDirective(def *ast.Definition, queryType, name string, pos *ast.Position) {
	for _, d := range def.Directives {
		if d.Name == base.QueryDirectiveName && base.DirectiveArgString(d, base.ArgType) == queryType {
			// Replace the name
			if a := d.Arguments.ForName(base.ArgName); a != nil {
				a.Value.Raw = name
			}
			return
		}
	}
	def.Directives = append(def.Directives, &ast.Directive{Name: base.QueryDirectiveName, Arguments: ast.ArgumentList{
		{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
		{Name: "type", Value: &ast.Value{Raw: queryType, Kind: ast.EnumValue, Position: pos}, Position: pos},
	}, Position: pos})
}

// optsModuleCatalogDirective creates a @module_catalog directive from compile options.
func optsModuleCatalogDirective(opts base.Options) *ast.Directive {
	pos := compiledPos("module_catalog")
	return &ast.Directive{
		Name: base.ModuleCatalogDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: opts.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// funcAggQueryDirective creates an @aggregation_query directive for a function aggregation field.
func funcAggQueryDirective(fieldName string, isBucket bool, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: "aggregation_query",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: fieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "is_bucket", Value: &ast.Value{Raw: fmt.Sprint(isBucket), Kind: ast.BooleanValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}
