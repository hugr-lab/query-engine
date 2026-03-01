package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*ExtensionFieldAggregationRule)(nil)

// ExtensionFieldAggregationRule generates aggregation, sub-aggregation, bucket aggregation,
// and filter types for fields added via `extend type` blocks targeting provider types.
// Also handles @references directives on extension types (new feature).
//
// Must run after AggregationRule so that base aggregation types already exist.
type ExtensionFieldAggregationRule struct{}

func (r *ExtensionFieldAggregationRule) Name() string     { return "ExtensionFieldAggregationRule" }
func (r *ExtensionFieldAggregationRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *ExtensionFieldAggregationRule) ProcessAll(ctx base.CompilationContext) error {
	source, ok := ctx.Source().(base.ExtensionsSource)
	if !ok {
		return nil
	}

	for ext := range source.Extensions(ctx.Context()) {
		// Skip Function/MutationFunction — they are promoted by InternalExtensionMerger
		if ext.Name == "Function" || ext.Name == "MutationFunction" {
			continue
		}

		// Only process extensions targeting provider types (not source types)
		if ctx.Source().ForName(ctx.Context(), ext.Name) != nil {
			continue
		}

		targetDef := ctx.LookupType(ext.Name)
		if targetDef == nil {
			continue
		}

		// Only process data objects (tables/views) — skip enums, inputs, etc.
		if targetDef.Directives.ForName("table") == nil && targetDef.Directives.ForName("view") == nil {
			continue
		}

		pos := compiledPos(ext.Name)
		aggTypeName := "_" + ext.Name + "_aggregation"

		// Process @references directives on the extension
		for _, dir := range ext.Directives.ForNames(base.ReferencesDirectiveName) {
			ProcessExtensionReferences(ctx, ext, dir, pos)
		}

		// Process individual fields
		for _, f := range ext.Fields {
			if f.Name == "_stub" || f.Name == "_placeholder" {
				continue
			}

			isJoin := f.Directives.ForName("join") != nil
			isTFCJ := f.Directives.ForName("table_function_call_join") != nil
			isFuncCall := f.Directives.ForName("function_call") != nil

			if isJoin {
				processExtensionJoinField(ctx, ext.Name, f, aggTypeName, pos)
			} else if isTFCJ {
				processExtensionTFCJField(ctx, ext.Name, f, aggTypeName, pos)
			} else if isFuncCall {
				processExtensionScalarAggField(ctx, f, aggTypeName, pos)
			}
		}
	}
	return nil
}

// processExtensionJoinField handles @join list fields added via extension.
// Adds subquery args to the field, aggregation fields on the base object,
// and direct agg + sub-agg on the aggregation type.
func processExtensionJoinField(ctx base.CompilationContext, parentName string, f *ast.FieldDefinition, aggTypeName string, pos *ast.Position) {
	// Only list fields
	if f.Type.NamedType != "" {
		return
	}
	targetName := f.Type.Name()
	// Use LookupType instead of GetObject — the target type is in the provider,
	// not in the current compilation's object registry.
	targetDef := ctx.LookupType(targetName)
	if targetDef == nil {
		return
	}

	targetFilterName := targetName + "_filter"
	targetAggName := "_" + targetName + "_aggregation"
	subAggName := AggTypeNameAtDepth(targetName, 1)
	targetOpts := optsFromTargetCatalog(targetDef)

	// Add @catalog from the target type so the planner knows which engine
	// this join's data comes from (critical for cross-source joins).
	if catDir := targetCatalogDirective(targetDef); catDir != nil {
		f.Directives = append(f.Directives, catDir)
	}

	// Add subquery args to the field itself, including view args for parameterized views
	if len(f.Arguments) == 0 {
		f.Arguments = extensionSubQueryArgs(targetDef, targetFilterName, pos)
	}

	// Add {name}_aggregation and {name}_bucket_aggregation on base object
	addReferenceAggregationFields(ctx, parentName, f.Name, targetName, targetFilterName, targetOpts, pos)

	// Add direct agg + sub-agg fields on aggregation type
	ctx.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     aggTypeName,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name:      f.Name,
				Type:      ast.NamedType(targetAggName, pos),
				Arguments: AggRefArgs(targetFilterName, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(f.Name, pos),
				},
				Position: pos,
			},
			{
				Name:      f.Name + "_aggregation",
				Type:      ast.NamedType(subAggName, pos),
				Arguments: AggSubRefArgs(targetFilterName, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(f.Name, pos),
				},
				Position: pos,
			},
		},
	})
}

// processExtensionTFCJField handles @table_function_call_join list fields added via extension.
// Preserves original args, adds aggregation and bucket aggregation on the base object,
// and direct agg + sub-agg on the aggregation type.
func processExtensionTFCJField(ctx base.CompilationContext, parentName string, f *ast.FieldDefinition, aggTypeName string, pos *ast.Position) {
	// Only list fields
	if f.Type.NamedType != "" {
		return
	}
	targetName := f.Type.Name()
	// Use LookupType instead of GetObject — the target type is in the provider,
	// not in the current compilation's object registry.
	targetDef := ctx.LookupType(targetName)
	if targetDef == nil {
		return
	}

	targetAggName := "_" + targetName + "_aggregation"
	bucketAggName := "_" + targetName + "_aggregation_bucket"
	subAggName := AggTypeNameAtDepth(targetName, 1)
	targetCatDir := targetCatalogDirective(targetDef)

	// Add @catalog from the target type so the planner knows which engine
	// this join's data comes from (critical for cross-source joins).
	if targetCatDir != nil {
		f.Directives = append(f.Directives, targetCatDir)
	}

	origArgs := CloneArgDefs(f.Arguments, pos)

	// Add {name}_aggregation and {name}_bucket_aggregation on base object (with original args)
	ctx.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     parentName,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name:        f.Name + "_aggregation",
				Description: "The aggregation for " + f.Name,
				Type:        ast.NamedType(targetAggName, pos),
				Arguments:   origArgs,
				Directives: ast.DirectiveList{
					{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
						{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
						{Name: base.ArgName, Value: &ast.Value{Raw: f.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
					targetCatDir,
				},
				Position: pos,
			},
			{
				Name:        f.Name + "_bucket_aggregation",
				Description: "The bucket aggregation for " + f.Name,
				Type:        ast.ListType(ast.NamedType(bucketAggName, pos), pos),
				Arguments:   CloneArgDefs(f.Arguments, pos),
				Directives: ast.DirectiveList{
					{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
						{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
						{Name: base.ArgName, Value: &ast.Value{Raw: f.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
					targetCatDir,
				},
				Position: pos,
			},
		},
	})

	// Add direct agg + sub-agg fields on aggregation type (with original args)
	ctx.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     aggTypeName,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name:      f.Name,
				Type:      ast.NamedType(targetAggName, pos),
				Arguments: CloneArgDefs(f.Arguments, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(f.Name, pos),
				},
				Position: pos,
			},
			{
				Name:      f.Name + "_aggregation",
				Type:      ast.NamedType(subAggName, pos),
				Arguments: CloneArgDefs(f.Arguments, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(f.Name, pos),
				},
				Position: pos,
			},
		},
	})
}

// processExtensionScalarAggField handles @function_call scalar fields added via extension.
// Adds aggregation field on the aggregation type if the scalar is aggregatable.
func processExtensionScalarAggField(ctx base.CompilationContext, f *ast.FieldDefinition, aggTypeName string, pos *ast.Position) {
	scalarName := f.Type.Name()
	s := ctx.ScalarLookup(scalarName)
	if s == nil {
		return
	}
	agg, ok := s.(types.Aggregatable)
	if !ok {
		return
	}

	aggFieldType := agg.AggregationTypeName()
	ctx.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     aggTypeName,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name: f.Name,
				Type: ast.NamedType(aggFieldType, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(f.Name, pos),
				},
				Position: pos,
			},
		},
	})
}

// ProcessExtensionReferences handles @references directives on extension `extend type` blocks.
// Generates forward/back query fields, filter extensions, and aggregation extensions.
// MUST NOT modify mutation input types (cross-catalog mutations not supported).
func ProcessExtensionReferences(ctx base.CompilationContext, ext *ast.Definition, dir *ast.Directive, pos *ast.Position) {
	refName := base.DirectiveArgString(dir, base.ArgReferencesName)
	if refName == "" {
		return
	}

	targetDef := ctx.LookupType(refName)
	if targetDef == nil {
		return
	}

	sourceFields := base.DirectiveArgStrings(dir, base.ArgSourceFields)
	refFields := base.DirectiveArgStrings(dir, base.ArgReferencesFields)
	if len(sourceFields) == 0 || len(refFields) == 0 {
		return
	}

	query := base.DirectiveArgString(dir, base.ArgQuery)
	if query == "" {
		query = refName
	}
	refQuery := base.DirectiveArgString(dir, base.ArgReferencesQuery)
	if refQuery == "" {
		refQuery = ext.Name
	}
	dirName := base.DirectiveArgString(dir, base.ArgName)
	if dirName == "" {
		dirName = refName
		if len(sourceFields) == 1 {
			dirName += "_" + sourceFields[0]
		}
	}
	isM2MRef := base.DirectiveArgString(dir, base.ArgIsM2M) == "true"
	m2mName := base.DirectiveArgString(dir, base.ArgM2MName)

	// Enrich @references directive with missing default args
	if dir.Arguments.ForName(base.ArgIsM2M) == nil {
		dir.Arguments = append(dir.Arguments, &ast.Argument{
			Name: base.ArgIsM2M, Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos,
		})
	}
	if dir.Arguments.ForName(base.ArgM2MName) == nil {
		dir.Arguments = append(dir.Arguments, &ast.Argument{
			Name: base.ArgM2MName, Value: &ast.Value{Raw: "", Kind: ast.StringValue, Position: pos}, Position: pos,
		})
	}
	if dir.Arguments.ForName(base.ArgDescription) == nil {
		dir.Arguments = append(dir.Arguments, &ast.Argument{
			Name: base.ArgDescription, Value: &ast.Value{Raw: targetDef.Description, Kind: ast.StringValue, Position: pos}, Position: pos,
		})
	}
	if dir.Arguments.ForName(base.ArgReferencesDescription) == nil {
		dir.Arguments = append(dir.Arguments, &ast.Argument{
			Name: base.ArgReferencesDescription, Value: &ast.Value{Raw: ext.Description, Kind: ast.StringValue, Position: pos}, Position: pos,
		})
	}

	refQueryDir := referencesQueryDirective(refName, dirName, isM2MRef, m2mName, pos)
	targetFilterName := refName + "_filter"

	// @catalog for forward fields: data comes from the referenced type (refName)
	forwardOpts := optsFromTargetCatalog(targetDef)

	// Forward reference field on source object
	var forwardType *ast.Type
	var forwardArgs ast.ArgumentDefinitionList
	if isM2MRef {
		forwardType = ast.ListType(ast.NamedType(refName, pos), pos)
		forwardArgs = SubQueryArgs(targetFilterName, pos)
	} else {
		forwardType = &ast.Type{NamedType: refName}
		forwardArgs = ast.ArgumentDefinitionList{
			{Name: "inner", Description: base.DescInnerJoinRef, Type: ast.NamedType("Boolean", pos), Position: pos},
		}
	}

	forwardExt := &ast.Definition{
		Kind:     ast.Object,
		Name:     ext.Name,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name:      query,
				Type:      forwardType,
				Arguments: forwardArgs,
				Directives: ast.DirectiveList{
					refQueryDir,
					targetCatalogDirective(targetDef),
				},
				Position: pos,
			},
		},
	}
	ctx.AddExtension(forwardExt)

	// Add forward reference to filter input (skip mutation inputs — cross-catalog constraint)
	addReferenceToFilterInput(ctx, ext.Name, query, targetFilterName, isM2MRef, refQueryDir, forwardOpts, pos)

	// Add forward reference field to source's aggregation type
	addReferenceToAggregationType(ctx, ext.Name, query, refName, targetFilterName, isM2MRef, forwardOpts, pos)

	// Back-reference field on target object
	if refQuery != "" && !isM2MRef {
		// @catalog for back-reference fields: data comes from ext.Name (the source type)
		sourceDef := ctx.LookupType(ext.Name)
		backOpts := optsFromTargetCatalog(sourceDef)

		sourceFilterName := ext.Name + "_filter"
		backRefQueryDir := referencesQueryDirective(ext.Name, dirName, isM2MRef, m2mName, pos)

		backRefExt := &ast.Definition{
			Kind:     ast.Object,
			Name:     refName,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name:      refQuery,
					Type:      ast.ListType(ast.NamedType(ext.Name, pos), pos),
					Arguments: SubQueryArgs(sourceFilterName, pos),
					Directives: ast.DirectiveList{
						backRefQueryDir,
						targetCatalogDirective(sourceDef),
					},
					Position: pos,
				},
			},
		}
		ctx.AddExtension(backRefExt)

		// Add back-reference to target's filter input
		addReferenceToFilterInput(ctx, refName, refQuery, sourceFilterName, true, backRefQueryDir, backOpts, pos)

		// Add aggregation + bucket_aggregation reference fields on target (base object)
		addReferenceAggregationFields(ctx, refName, refQuery, ext.Name, sourceFilterName, backOpts, pos)

		// Add back-reference fields to target's aggregation type
		addReferenceToAggregationType(ctx, refName, refQuery, ext.Name, sourceFilterName, true, backOpts, pos)
	}
}

// targetCatalogDirective creates a @catalog directive from the target definition's
// existing @catalog directive. Used for extension fields where @catalog must reference
// the target type's catalog (where the data lives), not the extension source's.
func targetCatalogDirective(targetDef *ast.Definition) *ast.Directive {
	if targetDef == nil {
		return nil
	}
	catDir := targetDef.Directives.ForName(base.CatalogDirectiveName)
	if catDir == nil {
		return nil
	}
	name := base.DirectiveArgString(catDir, "name")
	engine := base.DirectiveArgString(catDir, "engine")
	return catalogDirective(name, engine)
}

// optsFromTargetCatalog creates base.Options with catalog info extracted from
// the target definition. Used to pass to shared helpers (addReferenceAggregationFields,
// addReferenceToFilterInput, etc.) that use optsCatalogDirective(opts) internally.
// extensionSubQueryArgs returns subquery arguments for an extension @join field.
// If the target is a parameterized view (has @args directive), the `args` parameter
// is prepended so the user can pass view arguments through the extension field.
func extensionSubQueryArgs(targetDef *ast.Definition, filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	args := SubQueryArgs(filterName, pos)
	if targetDef == nil {
		return args
	}
	argsDir := targetDef.Directives.ForName(base.ViewArgsDirectiveName)
	if argsDir == nil {
		return args
	}
	inputArgsName := base.DirectiveArgString(argsDir, base.ArgName)
	if inputArgsName == "" {
		return args
	}
	required := base.DirectiveArgString(argsDir, base.ArgRequired) == "true"
	var argType *ast.Type
	if required {
		argType = ast.NonNullNamedType(inputArgsName, pos)
	} else {
		argType = ast.NamedType(inputArgsName, pos)
	}
	return append(ast.ArgumentDefinitionList{
		{Name: "args", Description: base.DescArgs, Type: argType, Position: pos},
	}, args...)
}

func optsFromTargetCatalog(targetDef *ast.Definition) base.Options {
	if targetDef == nil {
		return base.Options{}
	}
	catDir := targetDef.Directives.ForName(base.CatalogDirectiveName)
	if catDir == nil {
		return base.Options{}
	}
	return base.Options{
		Name:       base.DirectiveArgString(catDir, "name"),
		EngineType: base.DirectiveArgString(catDir, "engine"),
	}
}
