package rules

import (
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*ReferencesRule)(nil)

type ReferencesRule struct{}

func (r *ReferencesRule) Name() string     { return "ReferencesRule" }
func (r *ReferencesRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *ReferencesRule) Match(def *ast.Definition) bool {
	// Match definitions that have @references or fields with @field_references
	if def.Directives.ForName("references") != nil {
		return true
	}
	for _, f := range def.Fields {
		if f.Directives.ForName("field_references") != nil {
			return true
		}
	}
	return false
}

func (r *ReferencesRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	info := ctx.GetObject(def.Name)
	if info == nil {
		info = &base.ObjectInfo{Name: def.Name, OriginalName: def.Name}
	}
	opts := ctx.CompileOptions()
	pos := compiledPos(def.Name)

	// Process @field_references on individual fields first — convert to object-level @references
	// Also add @field_references directive to the corresponding filter field
	for _, f := range def.Fields {
		for _, dir := range f.Directives.ForNames("field_references") {
			refDir := fieldReferencesToReferences(f.Name, dir, def, ctx, pos)
			if refDir != nil {
				def.Directives = append(def.Directives, refDir)
			}
			// Add @field_references directive to the filter field
			addFieldReferencesToFilter(ctx, def.Name, f.Name, dir, pos)
		}
	}

	// Check if this is an M2M table
	isM2M := isM2MObject(def)

	if isM2M {
		// Enrich M2M table's @references directives with descriptions and default args
		for _, dir := range def.Directives.ForNames("references") {
			refName := base.DirectiveArgString(dir, "references_name")
			if refName == "" {
				continue
			}
			targetDef := ctx.LookupType(refName)
			if targetDef == nil {
				targetDef = ctx.Source().ForName(ctx.Context(), refName)
			}
			// Add default args
			if dir.Arguments.ForName("is_m2m") == nil {
				dir.Arguments = append(dir.Arguments, &ast.Argument{
					Name: "is_m2m", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos,
				})
			}
			if dir.Arguments.ForName("m2m_name") == nil {
				dir.Arguments = append(dir.Arguments, &ast.Argument{
					Name: "m2m_name", Value: &ast.Value{Raw: "", Kind: ast.StringValue, Position: pos}, Position: pos,
				})
			}
			if dir.Arguments.ForName("description") == nil {
				descVal := ""
				if targetDef != nil {
					descVal = targetDef.Description
				}
				dir.Arguments = append(dir.Arguments, &ast.Argument{
					Name: "description", Value: &ast.Value{Raw: descVal, Kind: ast.StringValue, Position: pos}, Position: pos,
				})
			}
			if dir.Arguments.ForName("references_description") == nil {
				dir.Arguments = append(dir.Arguments, &ast.Argument{
					Name: "references_description", Value: &ast.Value{Raw: def.Description, Kind: ast.StringValue, Position: pos}, Position: pos,
				})
			}
		}

		// M2M tables: add references to both sides
		addM2MReferences(ctx, def, pos)
		return nil
	}

	for _, dir := range def.Directives.ForNames("references") {
		refName := base.DirectiveArgString(dir, "references_name")
		if refName == "" {
			continue
		}

		targetDef := ctx.LookupType(refName)
		if targetDef == nil {
			targetDef = ctx.Source().ForName(ctx.Context(), refName)
		}
		if targetDef == nil {
			continue
		}
		targetInfo := ctx.GetObject(refName)
		if targetInfo == nil {
			targetInfo = &base.ObjectInfo{Name: refName, OriginalName: refName}
		}

		// Extract fields
		sourceFields := base.DirectiveArgStrings(dir, "source_fields")
		refFields := base.DirectiveArgStrings(dir, "references_fields")
		if len(sourceFields) == 0 || len(refFields) == 0 {
			continue
		}

		// Extract names
		query := base.DirectiveArgString(dir, "query")
		if query == "" {
			query = refName
		}
		refQuery := base.DirectiveArgString(dir, "references_query")
		if refQuery == "" {
			refQuery = def.Name
		}
		isM2MRef := base.DirectiveArgString(dir, "is_m2m") == "true"
		m2mName := base.DirectiveArgString(dir, "m2m_name")
		dirName := base.DirectiveArgString(dir, "name")
		if dirName == "" {
			dirName = refName
			if len(sourceFields) == 1 {
				dirName += "_" + sourceFields[0]
			}
		}

		// Enrich @references directive with missing default args
		if dir.Arguments.ForName("is_m2m") == nil {
			dir.Arguments = append(dir.Arguments, &ast.Argument{
				Name: "is_m2m", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos,
			})
		}
		if dir.Arguments.ForName("m2m_name") == nil {
			dir.Arguments = append(dir.Arguments, &ast.Argument{
				Name: "m2m_name", Value: &ast.Value{Raw: "", Kind: ast.StringValue, Position: pos}, Position: pos,
			})
		}
		if dir.Arguments.ForName("description") == nil {
			dir.Arguments = append(dir.Arguments, &ast.Argument{
				Name: "description", Value: &ast.Value{Raw: targetDef.Description, Kind: ast.StringValue, Position: pos}, Position: pos,
			})
		}
		if dir.Arguments.ForName("references_description") == nil {
			dir.Arguments = append(dir.Arguments, &ast.Argument{
				Name: "references_description", Value: &ast.Value{Raw: def.Description, Kind: ast.StringValue, Position: pos}, Position: pos,
			})
		}

		refQueryDir := referencesQueryDirective(refName, dirName, isM2MRef, m2mName, pos)
		targetFilterName := refName + "_filter"

		// Forward reference field on source object
		var forwardType *ast.Type
		var forwardArgs ast.ArgumentDefinitionList
		if isM2MRef {
			forwardType = ast.ListType(ast.NamedType(refName, pos), pos)
			forwardArgs = subQueryArgs(targetFilterName, pos)
		} else {
			forwardType = &ast.Type{NamedType: refName}
			forwardArgs = ast.ArgumentDefinitionList{
				{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos},
			}
		}

		forwardExt := &ast.Definition{
			Kind:     ast.Object,
			Name:     def.Name,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name:      query,
					Type:      forwardType,
					Arguments: forwardArgs,
					Directives: ast.DirectiveList{
						refQueryDir,
						optsCatalogDirective(opts),
					},
					Position: pos,
				},
			},
		}
		ctx.AddExtension(forwardExt)

		// Add forward reference to filter input
		addReferenceToFilterInput(ctx, def.Name, query, targetFilterName, isM2MRef, refQueryDir, opts, pos)

		// Add reference subquery field to mutation insert input
		if opts.SupportInsertReferences() && !isM2MRef {
			addReferenceToMutInput(ctx, def.Name, query, refName, pos)
		}

		// Add forward reference field to source's aggregation type
		addReferenceToAggregationType(ctx, def.Name, query, refName, targetFilterName, isM2MRef, opts, pos)

		// Back-reference field on target object (if references_query is set)
		if refQuery != "" && !isM2MRef {
			sourceFilterName := def.Name + "_filter"
			backRefQueryDir := referencesQueryDirective(def.Name, dirName, isM2MRef, m2mName, pos)

			backRefExt := &ast.Definition{
				Kind:     ast.Object,
				Name:     refName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:      refQuery,
						Type:      ast.ListType(ast.NamedType(def.Name, pos), pos),
						Arguments: subQueryArgs(sourceFilterName, pos),
						Directives: ast.DirectiveList{
							backRefQueryDir,
							optsCatalogDirective(opts),
						},
						Position: pos,
					},
				},
			}
			ctx.AddExtension(backRefExt)

			// Add back-reference to target's filter input (isList=true for back-references)
			addReferenceToFilterInput(ctx, refName, refQuery, sourceFilterName, true, backRefQueryDir, opts, pos)

			// Add back-reference subquery to mutation insert input (list)
			if opts.SupportInsertReferences() {
				addReferenceToMutInput(ctx, refName, refQuery, def.Name, pos, true)
			}

			// Add aggregation + bucket_aggregation reference fields on target (base object)
			addReferenceAggregationFields(ctx, refName, refQuery, def.Name, sourceFilterName, opts, pos)

			// Add back-reference fields to target's aggregation type
			addReferenceToAggregationType(ctx, refName, refQuery, def.Name, sourceFilterName, true, opts, pos)
		}
	}

	return nil
}

// addM2MReferences handles M2M reference propagation to both sides.
func addM2MReferences(ctx base.CompilationContext, def *ast.Definition, pos *ast.Position) {
	refs := def.Directives.ForNames("references")
	if len(refs) < 2 {
		return
	}
	opts := ctx.CompileOptions()

	aInfo := refDirectiveInfo(refs[0])
	bInfo := refDirectiveInfo(refs[1])

	// Add M2M references to both referenced objects
	// A gets access to B through M2M table
	addM2MReferenceSide(ctx, def, aInfo, bInfo, opts, pos)
	// B gets access to A through M2M table
	addM2MReferenceSide(ctx, def, bInfo, aInfo, opts, pos)
}

type refInfo struct {
	referencesName string
	sourceFields   []string
	refFields      []string
	query          string
	refQuery       string
	name           string
}

func refDirectiveInfo(dir *ast.Directive) refInfo {
	return refInfo{
		referencesName: base.DirectiveArgString(dir, "references_name"),
		sourceFields:   base.DirectiveArgStrings(dir, "source_fields"),
		refFields:      base.DirectiveArgStrings(dir, "references_fields"),
		query:          base.DirectiveArgString(dir, "query"),
		refQuery:       base.DirectiveArgString(dir, "references_query"),
		name:           base.DirectiveArgString(dir, "name"),
	}
}

func addM2MReferenceSide(ctx base.CompilationContext, m2mDef *ast.Definition, sideA, sideB refInfo, opts base.Options, pos *ast.Position) {
	// sideA's referenced object gets a field pointing to sideB's referenced object
	sourceObj := sideA.referencesName
	targetObj := sideB.referencesName
	fieldName := sideA.refQuery
	if fieldName == "" {
		fieldName = targetObj
	}
	dirName := sideA.name

	targetFilterName := targetObj + "_filter"
	refQueryDir := referencesQueryDirective(targetObj, dirName, true, m2mDef.Name, pos)

	// Add to source object: reference field to target through M2M
	ext := &ast.Definition{
		Kind:     ast.Object,
		Name:     sourceObj,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name:      fieldName,
				Type:      ast.ListType(ast.NamedType(targetObj, pos), pos),
				Arguments: subQueryArgs(targetFilterName, pos),
				Directives: ast.DirectiveList{
					refQueryDir,
					optsCatalogDirective(opts),
				},
				Position: pos,
			},
		},
	}
	ctx.AddExtension(ext)

	// Add to source's filter input
	addReferenceToFilterInput(ctx, sourceObj, fieldName, targetFilterName, true, refQueryDir, opts, pos)

	// Add aggregation variants
	addReferenceAggregationFields(ctx, sourceObj, fieldName, targetObj, targetFilterName, opts, pos)

	// Add M2M reference subquery field to mutation insert input (list)
	if opts.SupportInsertReferences() {
		addReferenceToMutInput(ctx, sourceObj, fieldName, targetObj, pos, true)
	}

	// Add M2M reference fields to source's aggregation type
	addReferenceToAggregationType(ctx, sourceObj, fieldName, targetObj, targetFilterName, true, opts, pos)

	// Add M2M @references directive to source object's definition
	if sourceDef := ctx.LookupType(sourceObj); sourceDef != nil {
		sourceDef.Directives = append(sourceDef.Directives, m2mReferencesDirective(
			dirName, targetObj, sideA.refFields, sideA.sourceFields,
			fieldName, sideB.refQuery, true, m2mDef.Name, pos,
			m2mDef.Description, m2mDef.Description,
		))
	}
}

// addReferenceToFilterInput adds a reference field to the filter input type.
// Parameterized views are excluded from filter nesting — if the target is a
// parameterized view (has @args / InputArgsName), the filter field is skipped.
func addReferenceToFilterInput(ctx base.CompilationContext, objectName, fieldName, targetFilterName string, isList bool, refDir *ast.Directive, opts base.Options, pos *ast.Position) {
	// Skip if target object is a parameterized view
	targetObjName := targetFilterName[:len(targetFilterName)-len("_filter")]
	if info := ctx.GetObject(targetObjName); info != nil && info.InputArgsName != "" {
		return
	}

	filterName := objectName + "_filter"
	filterTypeName := targetFilterName
	if isList {
		// Use list_filter for list references (based on the target object)
		targetObjName := targetFilterName[:len(targetFilterName)-len("_filter")]
		filterTypeName = targetObjName + "_list_filter"

		// Lazily create the _list_filter type if it doesn't exist
		if ctx.LookupType(filterTypeName) == nil {
			listFilterDef := generateListFilterInput(targetObjName, targetFilterName, filterTypeName, pos)
			ctx.AddDefinition(listFilterDef)

			// Add @filter_list_input directive to the target object's definition
			if targetDef := ctx.LookupType(targetObjName); targetDef != nil {
				targetDef.Directives = append(targetDef.Directives, &ast.Directive{
					Name: "filter_list_input",
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: filterTypeName, Kind: ast.StringValue, Position: pos}, Position: pos},
					},
					Position: pos,
				})
			}
		}
	}

	filterExt := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     filterName,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name: fieldName,
				Type: ast.NamedType(filterTypeName, pos),
				Directives: ast.DirectiveList{
					refDir,
					optsCatalogDirective(opts),
				},
				Position: pos,
			},
		},
	}
	ctx.AddExtension(filterExt)
}

// addReferenceAggregationFields adds aggregation and bucket_aggregation fields
// for a reference on the parent object.
func addReferenceAggregationFields(ctx base.CompilationContext, parentObject, refFieldName, targetObject, targetFilterName string, opts base.Options, pos *ast.Position) {
	aggTypeName := "_" + targetObject + "_aggregation"
	bucketAggTypeName := "_" + targetObject + "_aggregation_bucket"

	// Only add if the aggregation types exist
	if ctx.LookupType(aggTypeName) == nil {
		return
	}

	// Aggregation field on parent object
	aggExt := &ast.Definition{
		Kind:     ast.Object,
		Name:     parentObject,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name:      refFieldName + "_aggregation",
				Type:      ast.NamedType(aggTypeName, pos),
				Arguments: subQueryArgs(targetFilterName, pos),
				Directives: ast.DirectiveList{
					{Name: "aggregation_query", Arguments: ast.ArgumentList{
						{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
						{Name: "name", Value: &ast.Value{Raw: refFieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
					optsCatalogDirective(opts),
				},
				Position: pos,
			},
			{
				Name:      refFieldName + "_bucket_aggregation",
				Type:      ast.ListType(ast.NamedType(bucketAggTypeName, pos), pos),
				Arguments: subQueryArgs(targetFilterName, pos),
				Directives: ast.DirectiveList{
					{Name: "aggregation_query", Arguments: ast.ArgumentList{
						{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
						{Name: "name", Value: &ast.Value{Raw: refFieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
					optsCatalogDirective(opts),
				},
				Position: pos,
			},
		},
	}
	ctx.AddExtension(aggExt)
}

// fieldReferencesToReferences converts a @field_references directive on a field
// to an object-level @references directive.
func fieldReferencesToReferences(fieldName string, dir *ast.Directive, def *ast.Definition, ctx base.CompilationContext, pos *ast.Position) *ast.Directive {
	refName := base.DirectiveArgString(dir, "references_name")
	if refName == "" {
		return nil
	}

	field := base.DirectiveArgString(dir, "field")
	if field == "" {
		// Default to PK of referenced object
		targetDef := ctx.LookupType(refName)
		if targetDef == nil {
			targetDef = ctx.Source().ForName(ctx.Context(), refName)
		}
		if targetDef == nil {
			return nil
		}
		targetInfo := ctx.GetObject(refName)
		if targetInfo != nil && len(targetInfo.PrimaryKey) == 1 {
			field = targetInfo.PrimaryKey[0]
		}
	}
	if field == "" {
		return nil
	}

	query := base.DirectiveArgString(dir, "query")
	if query == "" {
		query = refName
	}
	refQuery := base.DirectiveArgString(dir, "references_query")
	if refQuery == "" {
		refQuery = def.Name
	}
	name := base.DirectiveArgString(dir, "name")
	if name == "" {
		name = refName + "_" + fieldName
	}

	// Look up target description for the @references directive
	targetDesc := ""
	refTargetDef := ctx.LookupType(refName)
	if refTargetDef == nil {
		refTargetDef = ctx.Source().ForName(ctx.Context(), refName)
	}
	if refTargetDef != nil {
		targetDesc = refTargetDef.Description
	}
	return referencesDirective(name, refName, []string{fieldName}, []string{field}, query, refQuery, false, "", pos, targetDesc, def.Description)
}

// referencesDirective builds a @references directive.
func referencesDirective(name, refName string, sourceFields, refFields []string, query, refQuery string, isM2M bool, m2mName string, pos *ast.Position, descriptions ...string) *ast.Directive {
	var sfChildren, rfChildren ast.ChildValueList
	for _, f := range sourceFields {
		sfChildren = append(sfChildren, &ast.ChildValue{Value: &ast.Value{Raw: f, Kind: ast.StringValue}})
	}
	for _, f := range refFields {
		rfChildren = append(rfChildren, &ast.ChildValue{Value: &ast.Value{Raw: f, Kind: ast.StringValue}})
	}
	isM2MStr := "false"
	if isM2M {
		isM2MStr = "true"
	}
	args := ast.ArgumentList{
		{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
		{Name: "references_name", Value: &ast.Value{Raw: refName, Kind: ast.StringValue, Position: pos}, Position: pos},
		{Name: "source_fields", Value: &ast.Value{Children: sfChildren, Kind: ast.ListValue, Position: pos}, Position: pos},
		{Name: "references_fields", Value: &ast.Value{Children: rfChildren, Kind: ast.ListValue, Position: pos}, Position: pos},
		{Name: "query", Value: &ast.Value{Raw: query, Kind: ast.StringValue, Position: pos}, Position: pos},
		{Name: "references_query", Value: &ast.Value{Raw: refQuery, Kind: ast.StringValue, Position: pos}, Position: pos},
		{Name: "is_m2m", Value: &ast.Value{Raw: isM2MStr, Kind: ast.BooleanValue, Position: pos}, Position: pos},
		{Name: "m2m_name", Value: &ast.Value{Raw: m2mName, Kind: ast.StringValue, Position: pos}, Position: pos},
	}
	// Always include description and references_description (old compiler always includes them)
	desc := ""
	if len(descriptions) > 0 {
		desc = descriptions[0]
	}
	refDesc := ""
	if len(descriptions) > 1 {
		refDesc = descriptions[1]
	}
	args = append(args, &ast.Argument{Name: "description", Value: &ast.Value{Raw: desc, Kind: ast.StringValue, Position: pos}, Position: pos})
	args = append(args, &ast.Argument{Name: "references_description", Value: &ast.Value{Raw: refDesc, Kind: ast.StringValue, Position: pos}, Position: pos})
	return &ast.Directive{
		Name:      "references",
		Arguments: args,
		Position:  pos,
	}
}

func m2mReferencesDirective(name, refName string, sourceFields, refFields []string, query, refQuery string, isM2M bool, m2mName string, pos *ast.Position, descriptions ...string) *ast.Directive {
	return referencesDirective(name, refName, sourceFields, refFields, query, refQuery, isM2M, m2mName, pos, descriptions...)
}

// referencesQueryDirective creates a @references_query directive for fields.
func referencesQueryDirective(refName, name string, isM2M bool, m2mName string, pos *ast.Position) *ast.Directive {
	isM2MStr := "false"
	if isM2M {
		isM2MStr = "true"
	}
	return &ast.Directive{
		Name: "references_query",
		Arguments: ast.ArgumentList{
			{Name: "references_name", Value: &ast.Value{Raw: refName, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "is_m2m", Value: &ast.Value{Raw: isM2MStr, Kind: ast.BooleanValue, Position: pos}, Position: pos},
			{Name: "m2m_name", Value: &ast.Value{Raw: m2mName, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// addReferenceToMutInput adds a reference subquery field to the source object's
// mutation insert input type (_mut_input_data).
// isList indicates whether the field should be a list type (for back-references and M2M).
func addReferenceToMutInput(ctx base.CompilationContext, sourceObject, fieldName, targetObject string, pos *ast.Position, isList ...bool) {
	inputName := sourceObject + "_mut_input_data"
	if ctx.LookupType(inputName) == nil {
		return
	}
	targetInputName := targetObject + "_mut_input_data"

	var fieldType *ast.Type
	if len(isList) > 0 && isList[0] {
		fieldType = ast.ListType(ast.NamedType(targetInputName, pos), pos)
	} else {
		fieldType = ast.NamedType(targetInputName, pos)
	}

	mutInputExt := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     inputName,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name:     fieldName,
				Type:     fieldType,
				Position: pos,
			},
		},
	}
	ctx.AddExtension(mutInputExt)
}

// maxAggDepth limits sub-aggregation recursion (matches old compiler's maxAggLevel=2).
const maxAggDepth = 2

// addReferenceToAggregationType adds reference fields to the parent object's aggregation type.
// For single (forward) references: adds the aggregation type field.
// For list (back/M2M) references: adds the aggregation type field + _aggregation sub-field.
func addReferenceToAggregationType(ctx base.CompilationContext, parentObject, refFieldName, targetObject, targetFilterName string, isList bool, opts base.Options, pos *ast.Position) {
	addRefToAggAtDepth(ctx, parentObject, refFieldName, targetObject, targetFilterName, isList, opts, pos, 0)
}

// addRefToAggAtDepth adds reference fields to an aggregation type at a given depth.
// depth 0 = base aggregation (_Type_aggregation)
// depth 1 = sub-aggregation (_Type_aggregation_sub_aggregation)
// depth >= maxAggDepth = stop recursion
func addRefToAggAtDepth(ctx base.CompilationContext, parentObject, refFieldName, targetObject, targetFilterName string, isList bool, opts base.Options, pos *ast.Position, depth int) {
	parentAggName := aggTypeNameAtDepth(parentObject, depth)
	if ctx.LookupType(parentAggName) == nil {
		return
	}

	// At depth > 0, skip single reference fields (only list refs are added to sub-aggs)
	if depth > 0 && !isList {
		return
	}

	var fields ast.FieldList

	if isList {
		// Direct reference field: uses target's agg at same depth
		targetAggName := aggTypeNameAtDepth(targetObject, depth)
		// Ensure the target agg type exists at this depth
		if depth > 0 {
			baseTargetAgg := "_" + targetObject + "_aggregation"
			ensureSubAggregationType(ctx, targetObject, targetAggName, baseTargetAgg, depth, pos)
		}

		fields = append(fields, &ast.FieldDefinition{
			Name:      refFieldName,
			Type:      ast.NamedType(targetAggName, pos),
			Arguments: aggRefArgs(targetFilterName, pos),
			Directives: ast.DirectiveList{
				fieldAggregationDirective(refFieldName, pos),
			},
			Position: pos,
		})

		// The _aggregation sub-field uses a sub-aggregation type (one level deeper)
		targetSubAggName := aggTypeNameAtDepth(targetObject, depth+1)
		// For the _aggregation sub-field, the parent is the current depth's target agg type
		parentTargetAgg := aggTypeNameAtDepth(targetObject, depth)
		ensureSubAggregationType(ctx, targetObject, targetSubAggName, parentTargetAgg, depth+1, pos)

		// At depth > 0, the @field_aggregation uses full field name including _aggregation
		aggFieldDirectiveName := refFieldName
		if depth > 0 {
			aggFieldDirectiveName = refFieldName + "_aggregation"
		}

		fields = append(fields, &ast.FieldDefinition{
			Name:      refFieldName + "_aggregation",
			Type:      ast.NamedType(targetSubAggName, pos),
			Arguments: aggSubRefArgs(targetFilterName, pos),
			Directives: ast.DirectiveList{
				fieldAggregationDirective(aggFieldDirectiveName, pos),
			},
			Position: pos,
		})
	} else {
		// Single references (depth 0 only): just the aggregation type field
		targetAggName := "_" + targetObject + "_aggregation"
		fields = append(fields, &ast.FieldDefinition{
			Name: refFieldName,
			Type: ast.NamedType(targetAggName, pos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				fieldAggregationDirective(refFieldName, pos),
			},
			Position: pos,
		})
	}

	ctx.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     parentAggName,
		Position: pos,
		Fields:   fields,
	})

	// Recurse: add reference fields to the parent's sub-aggregation type if it already exists.
	// Sub-aggregation types are only created by ensureSubAggregationType when another
	// reference's *_aggregation suffix field needs them. We don't force-create here.
	if depth+1 < maxAggDepth {
		parentSubAggName := aggTypeNameAtDepth(parentObject, depth+1)
		if ctx.LookupType(parentSubAggName) != nil {
			addRefToAggAtDepth(ctx, parentObject, refFieldName, targetObject, targetFilterName, isList, opts, pos, depth+1)
		}
	}
}

// aggTypeNameAtDepth returns the aggregation type name at a given depth.
// depth 0: _Type_aggregation
// depth 1: _Type_aggregation_sub_aggregation
// depth 2: _Type_aggregation_sub_aggregation_sub_aggregation
func aggTypeNameAtDepth(objectName string, depth int) string {
	name := "_" + objectName + "_aggregation"
	for i := 0; i < depth; i++ {
		name += "_sub_aggregation"
	}
	return name
}

// ensureSubAggregationType lazily creates a sub-aggregation type from the base
// aggregation type's scalar fields, mapping them to SubAggregation variants.
// depth 1: includes _rows_count + scalar fields (SubAgg types) + extra fields
// depth >= 2: only _rows_count
func ensureSubAggregationType(ctx base.CompilationContext, objectName, subAggTypeName, _ string, depth int, pos *ast.Position) {
	if ctx.LookupType(subAggTypeName) != nil {
		return // already created
	}

	// Always source scalar fields from the base aggregation type
	baseAggName := "_" + objectName + "_aggregation"
	baseAgg := ctx.LookupType(baseAggName)
	if baseAgg == nil {
		return
	}

	// The @aggregation directive references the parent (one level up)
	parentAggName := aggTypeNameAtDepth(objectName, depth-1)

	// level = depth + 1 (base = level 1, sub = level 2, sub-sub = level 3)
	level := depth + 1

	var fields ast.FieldList

	// _rows_count: at sub-level use BigIntAggregation, at sub-sub use BigIntSubAggregation
	rowsCountType := "BigIntAggregation"
	if depth >= maxAggDepth {
		rowsCountType = "BigIntSubAggregation"
	}
	fields = append(fields, &ast.FieldDefinition{
		Name:     "_rows_count",
		Type:     ast.NamedType(rowsCountType, pos),
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "field_aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "_rows_count", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	})

	// At max depth (sub-sub-aggregation), only _rows_count is included
	if depth < maxAggDepth {
		// Add scalar fields from base aggregation, mapped to SubAggregation types
		for _, f := range baseAgg.Fields {
			if f.Name == "_rows_count" {
				continue
			}
			subTypeName := scalarSubAggTypeName(f.Type.Name())
			if subTypeName == "" {
				continue // skip non-scalar fields (references, etc.)
			}

			subField := &ast.FieldDefinition{
				Name:     f.Name,
				Type:     ast.NamedType(subTypeName, pos),
				Position: pos,
			}
			// Copy directives
			if len(f.Directives) > 0 {
				subField.Directives = make(ast.DirectiveList, len(f.Directives))
				copy(subField.Directives, f.Directives)
			}
			// Copy arguments
			if len(f.Arguments) > 0 {
				subField.Arguments = make(ast.ArgumentDefinitionList, len(f.Arguments))
				copy(subField.Arguments, f.Arguments)
			}
			fields = append(fields, subField)
		}

		// Add extra fields (e.g., _founded_part for Date, _booking_time_part for Timestamp)
		// by checking the original object's fields for ExtraFieldProvider scalars
		origObj := ctx.LookupType(objectName)
		if origObj != nil {
			for _, f := range origObj.Fields {
				if f.Name == "_stub" {
					continue
				}
				s := ctx.ScalarLookup(f.Type.Name())
				if s == nil {
					continue
				}
				efp, ok := s.(types.ExtraFieldProvider)
				if !ok {
					continue
				}
				extraField := efp.GenerateExtraField(f.Name)
				if extraField == nil {
					continue
				}
				// Map extra field's return type to SubAggregation
				retScalar := ctx.ScalarLookup(extraField.Type.Name())
				if retScalar == nil {
					continue
				}
				retAgg, ok := retScalar.(types.Aggregatable)
				if !ok {
					continue
				}
				subTypeName := scalarSubAggTypeName(retAgg.AggregationTypeName())
				if subTypeName == "" {
					continue
				}
				extraAggField := &ast.FieldDefinition{
					Name:     extraField.Name,
					Type:     ast.NamedType(subTypeName, pos),
					Position: pos,
					Directives: ast.DirectiveList{
						fieldAggregationDirective(extraField.Name, pos),
					},
				}
				if len(extraField.Arguments) > 0 {
					extraAggField.Arguments = make(ast.ArgumentDefinitionList, len(extraField.Arguments))
					copy(extraAggField.Arguments, extraField.Arguments)
				}
				fields = append(fields, extraAggField)
			}
		}
	}

	subAgg := &ast.Definition{
		Kind:     ast.Object,
		Name:     subAggTypeName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: parentAggName, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: fmt.Sprintf("%d", level), Kind: ast.IntValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
		Fields: fields,
	}
	ctx.AddDefinition(subAgg)
}

// ensureSubAggregationTypeNoExtra creates a sub-aggregation type that includes only
// scalar fields from the base aggregation (no ExtraFieldProvider extra fields).
// This matches the old compiler's behavior for table_function_call_join-triggered sub-aggs,
// which are created during field iteration before extra fields are added to the base agg type.
func ensureSubAggregationTypeNoExtra(ctx base.CompilationContext, objectName, subAggTypeName string, depth int, pos *ast.Position) {
	if ctx.LookupType(subAggTypeName) != nil {
		return // already created
	}

	baseAggName := "_" + objectName + "_aggregation"
	baseAgg := ctx.LookupType(baseAggName)
	if baseAgg == nil {
		return
	}

	parentAggName := aggTypeNameAtDepth(objectName, depth-1)
	level := depth + 1

	var fields ast.FieldList

	rowsCountType := "BigIntAggregation"
	if depth >= maxAggDepth {
		rowsCountType = "BigIntSubAggregation"
	}
	fields = append(fields, &ast.FieldDefinition{
		Name:     "_rows_count",
		Type:     ast.NamedType(rowsCountType, pos),
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "field_aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "_rows_count", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	})

	if depth < maxAggDepth {
		for _, f := range baseAgg.Fields {
			if f.Name == "_rows_count" {
				continue
			}
			subTypeName := scalarSubAggTypeName(f.Type.Name())
			if subTypeName == "" {
				continue
			}
			subField := &ast.FieldDefinition{
				Name:     f.Name,
				Type:     ast.NamedType(subTypeName, pos),
				Position: pos,
			}
			if len(f.Directives) > 0 {
				subField.Directives = make(ast.DirectiveList, len(f.Directives))
				copy(subField.Directives, f.Directives)
			}
			if len(f.Arguments) > 0 {
				subField.Arguments = make(ast.ArgumentDefinitionList, len(f.Arguments))
				copy(subField.Arguments, f.Arguments)
			}
			fields = append(fields, subField)
		}
		// No extra fields from ExtraFieldProvider — intentionally omitted
	}

	subAgg := &ast.Definition{
		Kind:     ast.Object,
		Name:     subAggTypeName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: parentAggName, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: fmt.Sprintf("%d", level), Kind: ast.IntValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
		Fields: fields,
	}
	ctx.AddDefinition(subAgg)
}

// scalarSubAggTypeName maps a scalar aggregation type to its SubAggregation variant.
// Returns "" if the type is not a known scalar aggregation type.
func scalarSubAggTypeName(aggTypeName string) string {
	switch aggTypeName {
	case "StringAggregation":
		return "StringSubAggregation"
	case "IntAggregation":
		return "IntSubAggregation"
	case "BigIntAggregation":
		return "BigIntSubAggregation"
	case "FloatAggregation":
		return "FloatSubAggregation"
	case "BooleanAggregation":
		return "BooleanSubAggregation"
	case "DateAggregation":
		return "DateSubAggregation"
	case "TimestampAggregation":
		return "TimestampSubAggregation"
	case "TimeAggregation":
		return "TimeSubAggregation"
	case "JSONAggregation":
		return "JSONSubAggregation"
	case "GeometryAggregation":
		return "GeometrySubAggregation"
	default:
		return ""
	}
}

// propagateRefFieldsToSubAgg copies reference fields from a parent aggregation
// type's extension into an already-created sub-aggregation type. This handles
// the timing issue where references are processed before a sub-agg type exists
// (e.g., table_function_call_join creates the sub-agg type after references were
// already added to the parent agg).
func propagateRefFieldsToSubAgg(ctx base.CompilationContext, objectName, subAggTypeName string, depth int, pos *ast.Position) {
	if depth >= maxAggDepth {
		return
	}
	parentAggName := aggTypeNameAtDepth(objectName, depth-1)
	parentAggExt := ctx.LookupExtension(parentAggName)
	if parentAggExt == nil {
		return
	}

	opts := ctx.CompileOptions()
	var fields ast.FieldList

	for _, f := range parentAggExt.Fields {
		// Skip scalar aggregation fields — only propagate reference fields.
		// Reference fields on agg types come in pairs: {ref} and {ref}_aggregation.
		// They point to agg types like _Route_aggregation or sub-agg types.
		typeName := f.Type.Name()
		if scalarSubAggTypeName(typeName) != "" {
			continue // scalar aggregation type, skip
		}
		if typeName == "" {
			continue
		}

		// Check if this looks like a reference field on an aggregation type.
		// Reference fields have @field_aggregation directive.
		if f.Directives.ForName("field_aggregation") == nil {
			continue
		}

		// Get the reference field name from the @field_aggregation directive
		fieldAggDir := f.Directives.ForName("field_aggregation")
		refFieldName := base.DirectiveArgString(fieldAggDir, "name")
		if refFieldName == "" {
			continue
		}

		// Determine if this is a reference field or a reference _aggregation field
		isAggSuffix := len(f.Name) > len("_aggregation") && f.Name[len(f.Name)-len("_aggregation"):] == "_aggregation"

		if isAggSuffix {
			// This is a {ref}_aggregation field — it points to a sub-agg type.
			// For the new sub-agg level, we need to create the deeper sub-agg type.
			// Extract the target object name from the agg type name.
			targetSubAggName := f.Type.Name()
			// The target object's sub-agg at this depth should already exist
			// (created by the {ref} field's ensureSubAggregationType call).
			if ctx.LookupType(targetSubAggName) == nil {
				continue
			}

			// Create the deeper sub-agg for the target if needed
			// targetSubAggName is at depth, we need depth+1
			// Extract target object name
			targetObjName := extractObjectNameFromAggType(targetSubAggName)
			if targetObjName == "" {
				continue
			}
			deeperSubAggName := aggTypeNameAtDepth(targetObjName, depth+1)
			parentTargetAgg := aggTypeNameAtDepth(targetObjName, depth)
			ensureSubAggregationType(ctx, targetObjName, deeperSubAggName, parentTargetAgg, depth+1, pos)

			// Use the deeper sub-agg name for the field at this depth
			aggFieldDirectiveName := refFieldName
			if depth > 0 {
				aggFieldDirectiveName = refFieldName + "_aggregation"
			}

			fields = append(fields, &ast.FieldDefinition{
				Name:      f.Name,
				Type:      ast.NamedType(deeperSubAggName, pos),
				Arguments: cloneArgDefs(f.Arguments, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(aggFieldDirectiveName, pos),
				},
				Position: pos,
			})
		} else {
			// This is a direct reference field — it points to an agg type at the same depth.
			// For the sub-agg level, point to the target's agg at this depth.
			targetObjName := extractObjectNameFromAggType(f.Type.Name())
			if targetObjName == "" {
				continue
			}
			targetAggAtDepth := aggTypeNameAtDepth(targetObjName, depth)
			// Ensure target's sub-agg exists
			if depth > 0 {
				baseTargetAgg := "_" + targetObjName + "_aggregation"
				ensureSubAggregationType(ctx, targetObjName, targetAggAtDepth, baseTargetAgg, depth, pos)
			}

			fields = append(fields, &ast.FieldDefinition{
				Name:      f.Name,
				Type:      ast.NamedType(targetAggAtDepth, pos),
				Arguments: cloneArgDefs(f.Arguments, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(refFieldName, pos),
				},
				Position: pos,
			})
		}
	}

	if len(fields) > 0 {
		ctx.AddExtension(&ast.Definition{
			Kind:     ast.Object,
			Name:     subAggTypeName,
			Position: pos,
			Fields:   fields,
		})
	}

	// Also propagate extra fields (measurement, part) from the parent agg extension
	propagateExtraFieldsToSubAgg(ctx, objectName, subAggTypeName, depth, parentAggExt, opts, pos)
}

// propagateExtraFieldsToSubAgg copies extra fields (@measurement, @part) from the
// parent aggregation type's extension to a sub-aggregation type.
func propagateExtraFieldsToSubAgg(ctx base.CompilationContext, objectName, subAggTypeName string, depth int, parentAggExt *ast.Definition, _ base.Options, pos *ast.Position) {
	if depth >= maxAggDepth {
		return
	}

	var extraFields ast.FieldList
	for _, f := range parentAggExt.Fields {
		typeName := f.Type.Name()
		subTypeName := scalarSubAggTypeName(typeName)
		if subTypeName == "" {
			continue // not a scalar aggregation type
		}
		// Only copy extra fields that have @field_aggregation
		if f.Directives.ForName("field_aggregation") == nil {
			continue
		}

		// Check if the base aggregation definition already has this field
		baseAgg := ctx.LookupType(aggTypeNameAtDepth(objectName, 0))
		if baseAgg != nil && baseAgg.Fields.ForName(f.Name) != nil {
			continue // Already on the base agg, will be picked up by ensureSubAggregationType
		}

		subField := &ast.FieldDefinition{
			Name:     f.Name,
			Type:     ast.NamedType(subTypeName, pos),
			Position: pos,
		}
		if len(f.Directives) > 0 {
			subField.Directives = make(ast.DirectiveList, len(f.Directives))
			copy(subField.Directives, f.Directives)
		}
		if len(f.Arguments) > 0 {
			subField.Arguments = make(ast.ArgumentDefinitionList, len(f.Arguments))
			copy(subField.Arguments, f.Arguments)
		}
		extraFields = append(extraFields, subField)
	}

	if len(extraFields) > 0 {
		ctx.AddExtension(&ast.Definition{
			Kind:     ast.Object,
			Name:     subAggTypeName,
			Position: pos,
			Fields:   extraFields,
		})
	}
}

// extractObjectNameFromAggType extracts the object name from an aggregation type name.
// E.g., "_Route_aggregation" → "Route", "_Route_aggregation_sub_aggregation" → "Route"
func extractObjectNameFromAggType(aggTypeName string) string {
	if len(aggTypeName) < 2 || aggTypeName[0] != '_' {
		return ""
	}
	name := aggTypeName[1:] // strip leading _
	if idx := indexOf(name, "_aggregation"); idx >= 0 {
		return name[:idx]
	}
	return ""
}

// indexOf returns the index of the first occurrence of substr in s, or -1.
func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// cloneArgDefs creates a shallow copy of an argument definition list.
func cloneArgDefs(args ast.ArgumentDefinitionList, _ *ast.Position) ast.ArgumentDefinitionList {
	if len(args) == 0 {
		return nil
	}
	out := make(ast.ArgumentDefinitionList, len(args))
	copy(out, args)
	return out
}

// cloneASTType creates a shallow copy of an ast.Type tree.
func cloneASTType(t *ast.Type) *ast.Type {
	if t == nil {
		return nil
	}
	c := &ast.Type{
		NamedType: t.NamedType,
		NonNull:   t.NonNull,
		Position:  t.Position,
	}
	if t.Elem != nil {
		c.Elem = cloneASTType(t.Elem)
	}
	return c
}

// aggRefArgs returns reference field args on aggregation types:
// filter + order_by + distinct_on + inner + nested_*.
func aggRefArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "distinct_on", Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// aggSubRefArgs returns args for the _aggregation sub-field on aggregation types.
// Includes filter + order_by + limit/offset + distinct_on + inner + nested_*.
func aggSubRefArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "limit", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "2000", Kind: ast.IntValue}},
		{Name: "offset", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "0", Kind: ast.IntValue}},
		{Name: "distinct_on", Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// addFieldReferencesToFilter adds a @field_references directive to the filter field
// matching a @field_references directive on the source object field.
func addFieldReferencesToFilter(ctx base.CompilationContext, objectName, fieldName string, dir *ast.Directive, pos *ast.Position) {
	filterName := objectName + "_filter"
	filterDef := ctx.LookupType(filterName)
	if filterDef == nil {
		return
	}
	filterField := filterDef.Fields.ForName(fieldName)
	if filterField == nil {
		return
	}
	// Copy the @field_references directive from the source to the filter field
	filterField.Directives = append(filterField.Directives, dir)
}

func isM2MObject(def *ast.Definition) bool {
	d := def.Directives.ForName("table")
	if d == nil {
		return false
	}
	if a := d.Arguments.ForName("is_m2m"); a != nil {
		return a.Value.Raw == "true"
	}
	return false
}
