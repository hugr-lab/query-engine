package rules

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*ModuleAssembler)(nil)

// ModuleAssembler creates module type hierarchy from ObjectInfo.Module values
// and moves per-object query/mutation fields into module-scoped types.
type ModuleAssembler struct{}

func (r *ModuleAssembler) Name() string     { return "ModuleAssembler" }
func (r *ModuleAssembler) Phase() base.Phase { return base.PhaseAssemble }

func (r *ModuleAssembler) ProcessAll(ctx base.CompilationContext) error {
	// Collect fields per module first, then create types with real fields
	type moduleFields struct {
		queryFields    ast.FieldList
		mutationFields ast.FieldList
	}
	modules := make(map[string]*moduleFields)

	pos := compiledPos("module")

	for name, info := range ctx.Objects() {
		if info.Module == "" {
			continue
		}

		mf := modules[info.Module]
		if mf == nil {
			mf = &moduleFields{}
			modules[info.Module] = mf
		}

		// Add @module directive on the object definition (or update existing)
		if def := ctx.LookupType(name); def != nil {
			if existing := def.Directives.ForName("module"); existing != nil {
				// Update existing @module value (may have been set by inline SDL or PrefixPreparer)
				if a := existing.Arguments.ForName("name"); a != nil {
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

		// Collect query fields
		if qFields := ctx.QueryFields()[name]; len(qFields) > 0 {
			mf.queryFields = append(mf.queryFields, qFields...)
			delete(ctx.QueryFields(), name)
		}

		// Collect mutation fields
		if mFields := ctx.MutationFields()[name]; len(mFields) > 0 {
			mf.mutationFields = append(mf.mutationFields, mFields...)
			delete(ctx.MutationFields(), name)
		}
	}

	// Also collect function fields by module from Function/MutationFunction types
	type funcModuleFields struct {
		queryFuncFields ast.FieldList
		mutFuncFields   ast.FieldList
	}
	funcModules := make(map[string]*funcModuleFields)

	if funcDef := ctx.LookupType("Function"); funcDef != nil {
		var remaining ast.FieldList
		for _, f := range funcDef.Fields {
			if f.Name == "_stub" || f.Name == "_placeholder" {
				continue
			}
			mod := base.DirectiveArgString(f.Directives.ForName("module"), "name")
			if mod == "" {
				remaining = append(remaining, f)
				continue
			}
			fm := funcModules[mod]
			if fm == nil {
				fm = &funcModuleFields{}
				funcModules[mod] = fm
			}
			fm.queryFuncFields = append(fm.queryFuncFields, f)
		}
		funcDef.Fields = remaining
	}

	if mutFuncDef := ctx.LookupType("MutationFunction"); mutFuncDef != nil {
		var remaining ast.FieldList
		for _, f := range mutFuncDef.Fields {
			if f.Name == "_stub" || f.Name == "_placeholder" {
				continue
			}
			mod := base.DirectiveArgString(f.Directives.ForName("module"), "name")
			if mod == "" {
				remaining = append(remaining, f)
				continue
			}
			fm := funcModules[mod]
			if fm == nil {
				fm = &funcModuleFields{}
				funcModules[mod] = fm
			}
			fm.mutFuncFields = append(fm.mutFuncFields, f)
		}
		mutFuncDef.Fields = remaining
	}

	if len(modules) == 0 && len(funcModules) == 0 {
		return nil
	}

	// Collect all unique module names including implicit parents.
	// For dotted names like "transport.air", ensure "transport" also exists.
	allModules := make(map[string]bool)
	for mod := range modules {
		allModules[mod] = true
		parts := strings.Split(mod, ".")
		for i := 1; i < len(parts); i++ {
			allModules[strings.Join(parts[:i], ".")] = true
		}
	}
	for mod := range funcModules {
		allModules[mod] = true
		parts := strings.Split(mod, ".")
		for i := 1; i < len(parts); i++ {
			allModules[strings.Join(parts[:i], ".")] = true
		}
	}

	// Sort by depth (parents before children), then alphabetically.
	sortedModules := make([]string, 0, len(allModules))
	for mod := range allModules {
		sortedModules = append(sortedModules, mod)
	}
	sort.Slice(sortedModules, func(i, j int) bool {
		di := strings.Count(sortedModules[i], ".")
		dj := strings.Count(sortedModules[j], ".")
		if di != dj {
			return di < dj
		}
		return sortedModules[i] < sortedModules[j]
	})

	// Track created module types for adding child fields on parents.
	createdQueryTypes := make(map[string]*ast.Definition)
	createdMutTypes := make(map[string]*ast.Definition)
	createdFuncTypes := make(map[string]*ast.Definition)

	for _, mod := range sortedModules {
		modTypeName := "_module_" + strings.ReplaceAll(mod, ".", "_")
		queryTypeName := modTypeName + "_query"
		mutTypeName := modTypeName + "_mutation"

		// Check if module types already exist in provider (multi-catalog case)
		existsInProvider := ctx.LookupType(queryTypeName) != nil

		// --- Module query type ---
		queryDirs := ast.DirectiveList{
			{Name: "module_root", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "QUERY", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
		}
		if existsInProvider {
			queryDirs = append(queryDirs, &ast.Directive{Name: "if_not_exists", Position: pos})
		}
		queryModType := &ast.Definition{
			Kind:       ast.Object,
			Name:       queryTypeName,
			Position:   pos,
			Directives: queryDirs,
		}
		ctx.AddDefinition(queryModType)
		createdQueryTypes[mod] = queryModType

		// Add query fields as extension (only if this module has data objects)
		if mf := modules[mod]; mf != nil && len(mf.queryFields) > 0 {
			ctx.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     queryTypeName,
				Position: pos,
				Fields:   mf.queryFields,
			})
		}

		// --- Module mutation type ---
		mutDirs := ast.DirectiveList{
			{Name: "module_root", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "MUTATION", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
		}
		if existsInProvider {
			mutDirs = append(mutDirs, &ast.Directive{Name: "if_not_exists", Position: pos})
		}
		mutModType := &ast.Definition{
			Kind:       ast.Object,
			Name:       mutTypeName,
			Position:   pos,
			Directives: mutDirs,
		}
		ctx.AddDefinition(mutModType)
		createdMutTypes[mod] = mutModType

		// Add mutation fields as extension (only if this module has data objects)
		if mf := modules[mod]; mf != nil && len(mf.mutationFields) > 0 {
			ctx.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     mutTypeName,
				Position: pos,
				Fields:   mf.mutationFields,
			})
		}

		// --- Module function type (if this module has function fields) ---
		if fm := funcModules[mod]; fm != nil {
			if len(fm.queryFuncFields) > 0 {
				funcTypeName := modTypeName + "_function"
				funcModType := &ast.Definition{
					Kind:     ast.Object,
					Name:     funcTypeName,
					Position: pos,
					Directives: ast.DirectiveList{
						{Name: "module_root", Arguments: ast.ArgumentList{
							{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
							{Name: "type", Value: &ast.Value{Raw: "FUNCTION", Kind: ast.EnumValue, Position: pos}, Position: pos},
						}, Position: pos},
					},
					Fields: fm.queryFuncFields,
				}
				ctx.AddDefinition(funcModType)
				createdFuncTypes[mod] = funcModType
				addModuleFuncAggregations(ctx, funcModType, pos)
			}
			delete(funcModules, mod)
		}

		// Wire up hierarchy: child modules add field on parent, top-level registers on root
		parts := strings.Split(mod, ".")
		if len(parts) > 1 {
			// Has parent — add child field on parent module types
			parentMod := strings.Join(parts[:len(parts)-1], ".")
			childName := parts[len(parts)-1]

			if parentQuery := createdQueryTypes[parentMod]; parentQuery != nil {
				parentQuery.Fields = append(parentQuery.Fields, &ast.FieldDefinition{
					Name:        childName,
					Description: "The root query object of the module " + mod,
					Type:        ast.NamedType(queryTypeName, pos),
					Position:    pos,
				})
			}
			if parentMut := createdMutTypes[parentMod]; parentMut != nil {
				parentMut.Fields = append(parentMut.Fields, &ast.FieldDefinition{
					Name:        childName,
					Description: "The root mutation object of the module " + mod,
					Type:        ast.NamedType(mutTypeName, pos),
					Position:    pos,
				})
			}
			if funcType := createdFuncTypes[mod]; funcType != nil {
				funcTypeName := modTypeName + "_function"
				if parentFunc := createdFuncTypes[parentMod]; parentFunc != nil {
					parentFunc.Fields = append(parentFunc.Fields, &ast.FieldDefinition{
						Name:        childName,
						Description: "The root query object of the module " + mod,
						Type:        ast.NamedType(funcTypeName, pos),
						Position:    pos,
					})
				}
			}
		} else {
			// Top-level module — register on root Query/Mutation
			modField := &ast.FieldDefinition{
				Name:     mod,
				Type:     ast.NamedType(queryTypeName, pos),
				Position: pos,
			}
			ctx.RegisterQueryFields("_module_"+mod, []*ast.FieldDefinition{modField})

			mutField := &ast.FieldDefinition{
				Name:     mod,
				Type:     ast.NamedType(mutTypeName, pos),
				Position: pos,
			}
			ctx.RegisterMutationFields("_module_"+mod, []*ast.FieldDefinition{mutField})

			// Wire function module type on Function/MutationFunction
			if funcType := createdFuncTypes[mod]; funcType != nil {
				funcTypeName := modTypeName + "_function"
				if funcDef := ctx.LookupType("Function"); funcDef != nil {
					funcDef.Fields = append(funcDef.Fields, &ast.FieldDefinition{
						Name:        mod,
						Description: "The root query object of the module " + mod,
						Type:        ast.NamedType(funcTypeName, pos),
						Position:    pos,
					})
				}
			}
		}
	}

	// Handle function modules that don't have corresponding data object modules
	for mod, fm := range funcModules {
		modTypeName := "_module_" + strings.ReplaceAll(mod, ".", "_")
		if len(fm.queryFuncFields) > 0 {
			funcTypeName := modTypeName + "_function"
			funcModType := &ast.Definition{
				Kind:     ast.Object,
				Name:     funcTypeName,
				Position: pos,
				Directives: ast.DirectiveList{
					{Name: "module_root", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: mod, Kind: ast.StringValue, Position: pos}, Position: pos},
						{Name: "type", Value: &ast.Value{Raw: "FUNCTION", Kind: ast.EnumValue, Position: pos}, Position: pos},
					}, Position: pos},
				},
				Fields: fm.queryFuncFields,
			}
			ctx.AddDefinition(funcModType)
			addModuleFuncAggregations(ctx, funcModType, pos)
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
		if targetDef.Directives.ForName("table") == nil && targetDef.Directives.ForName("view") == nil {
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
		if d.Name == "query" && base.DirectiveArgString(d, "type") == queryType {
			// Replace the name
			if a := d.Arguments.ForName("name"); a != nil {
				a.Value.Raw = name
			}
			return
		}
	}
	def.Directives = append(def.Directives, &ast.Directive{Name: "query", Arguments: ast.ArgumentList{
		{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
		{Name: "type", Value: &ast.Value{Raw: queryType, Kind: ast.EnumValue, Position: pos}, Position: pos},
	}, Position: pos})
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
