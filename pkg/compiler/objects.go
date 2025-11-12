package compiler

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	objectHyperTableDirectiveName = "hypertable"
	objectCubeDirectiveName       = "cube"

	objectUniqueDirectiveName         = "unique"
	fieldJoinDirectiveName            = "join"
	fieldReferencesQueryDirectiveName = "references_query"

	mutationInsertPrefix = "insert_"
	mutationUpdatePrefix = "update_"
	mutationDeletePrefix = "delete_"

	OperationResultTypeName = "OperationResult"

	ObjectQueryByPKSuffix = "_by_pk"
)

func addObjectPrefix(defs Definitions, def *ast.Definition, opt *Options, addOriginal bool) {
	if def.Kind != ast.Object {
		return
	}
	if def.Name == base.FunctionTypeName || def.Name == base.FunctionMutationTypeName {
		addFunctionsPrefix(defs, def, opt)
		return
	}

	if IsSystemType(def) {
		return
	}
	if addOriginal {
		def.Directives = append(def.Directives, base.OriginalNameDirective(def.Name))
	}
	prefix := opt.Prefix + "_"

	def.Name = prefix + def.Name

	for _, d := range def.Directives.ForNames(referencesDirectiveName) {
		if a := d.Arguments.ForName("references_name"); a != nil {
			a.Value.Raw = prefix + a.Value.Raw
		}
	}
	if d := def.Directives.ForName(base.ViewArgsDirectiveName); d != nil {
		if a := d.Arguments.ForName("name"); a != nil {
			a.Value.Raw = prefix + a.Value.Raw
		}
	}

	for _, field := range def.Fields {
		for _, d := range field.Directives {
			switch d.Name {
			case fieldReferencesDirectiveName, JoinDirectiveName:
				if a := d.Arguments.ForName("references_name"); a != nil {
					a.Value.Raw = prefix + a.Value.Raw
				}
			case functionCallDirectiveName, functionCallTableJoinDirectiveName:
				a := d.Arguments.ForName("references_name")
				if a == nil { // this is a schema error, but it will be reported on the validation stage
					continue
				}
				if !opt.AsModule {
					a.Value.Raw = prefix + a.Value.Raw
				}
			}
		}
		field.Type = typeWithPrefix(defs, field.Type, prefix)
	}

	for i := range def.Interfaces {
		def.Interfaces[i] = prefix + def.Interfaces[i]
	}
}

func validateObject(defs Definitions, def *ast.Definition, opt *Options) error {
	if IsSystemType(def) {
		return nil
	}
	var objectType string

	for _, d := range def.Directives {
		switch d.Name {
		case base.ModuleDirectiveName:
		case base.ObjectTableDirectiveName, base.ObjectViewDirectiveName:
			if objectType != "" {
				return ErrorPosf(d.Position, "object %s can't have multiple type directives", def.Name)
			}
			objectType = d.Name
			if directiveArgValue(d, "soft_delete") == "true" {
				if directiveArgValue(d, "soft_delete_cond") == "" {
					return ErrorPosf(d.Position, "object %s should have soft_delete_cond argument", def.Name)
				}
				if directiveArgValue(d, "soft_delete_set") == "" {
					return ErrorPosf(d.Position, "object %s should have soft_delete_set argument", def.Name)
				}
			}
		case referencesDirectiveName, objectHyperTableDirectiveName, objectCubeDirectiveName, base.CacheDirectiveName:
			if objectType == "" {
				return ErrorPosf(d.Position, "object %s should be an data object (table or view) directive before @%s", def.Name, d.Name)
			}
			if d.Name != referencesDirectiveName {
				continue
			}
			// check references object and references field
			if err := validateObjectReferences(defs, def, d); err != nil {
				return err
			}
		case objectUniqueDirectiveName:
			if objectType == "" {
				return ErrorPosf(d.Position, "object %s should have type directive before @%s", def.Name, d.Name)
			}
			if err := validateObjectUnique(defs, def, d); err != nil {
				return err
			}
		case base.ViewArgsDirectiveName:
			if objectType != base.ObjectViewDirectiveName {
				return ErrorPosf(d.Position, "object %s should have @%s directive before @%s", def.Name, base.ObjectViewDirectiveName, d.Name)
			}
			argName := directiveArgValue(d, "name")
			it := defs.ForName(argName)
			if it == nil {
				return ErrorPosf(d.Position, "object %s have @%s directive. Input object %s definition not found", def.Name, base.ObjectViewDirectiveName, argName)
			}
			required := false
			for _, field := range it.Fields {
				if field.Type.NonNull {
					required = true
					break
				}
			}
			a := d.Arguments.ForName("required")
			if a == nil {
				d.Arguments = append(d.Arguments, &ast.Argument{
					Name:     "required",
					Value:    &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: d.Position},
					Position: d.Position,
				})
				a = d.Arguments.ForName("required")
			}
			if a.Value == nil {
				a.Value = &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: d.Position}
			}
			if a.Value.Raw == "true" || required {
				a.Value.Raw = "true"
			}
		case base.DependencyDirectiveName:
			// extension object that has dependencies can be only a view
			if objectType != base.ObjectViewDirectiveName {
				return ErrorPosf(d.Position, "object %s should have @%s directive before @%s", def.Name, base.ObjectViewDirectiveName, d.Name)
			}
		case base.CatalogDirectiveName, base.OriginalNameDirectiveName:
		case base.EmbeddingsDirectiveName:
			if objectType != base.ObjectTableDirectiveName && objectType != base.ObjectViewDirectiveName {
				return ErrorPosf(d.Position, "object %s should be defined as Data Object (table or view) before @%s", def.Name, d.Name)
			}
			// validate embeddings fields
			fieldName := directiveArgValue(d, "vector")
			if fieldName == "" {
				return ErrorPosf(d.Position, "object %s should have vector field defined for @%s", def.Name, d.Name)
			}
			field := def.Fields.ForName(fieldName)
			if field == nil {
				return ErrorPosf(d.Position, "object %s should have vector field %s defined for @%s", def.Name, fieldName, d.Name)
			}
			if field.Type.NamedType != base.VectorTypeName {
				return ErrorPosf(d.Position, "object %s field %s should be of type %s for @%s", def.Name, fieldName, base.VectorTypeName, d.Name)
			}
			if directiveArgValue(d, "distance") == "" {
				return ErrorPosf(d.Position, "object %s should have distance argument defined for @%s", def.Name, d.Name)
			}
		case base.GisWFSDirectiveName:
			if objectType != base.ObjectViewDirectiveName && objectType != base.ObjectTableDirectiveName {
				return ErrorPosf(d.Position, "object %s should be defined as Data Object (table or view) before @%s", def.Name, d.Name)
			}
			err := validateGisDirectives(def, opt)
			if err != nil {
				return err
			}
		default:
			return ErrorPosf(d.Position, "object %s has unknown directive %s", def.Name, d.Name)
		}
	}

	for _, field := range def.Fields {
		err := validateObjectField(defs, def, field)
		if err != nil {
			return err
		}
	}

	// add embedding distance extra field to calculate distance to the query vector
	if vecFieldName := objectDirectiveArgValue(def, base.EmbeddingsDirectiveName, "vector"); vecFieldName != "" {
		def.Fields = append(def.Fields, base.VectorEmbeddingsExtraField(vecFieldName))
	}

	if opt.AsModule && IsDataObject(def) {
		if d := def.Directives.ForName(base.ModuleDirectiveName); d != nil {
			if a := d.Arguments.ForName("name"); a != nil {
				if a.Value.Raw == "" {
					a.Value.Raw = opt.Name
				} else {
					a.Value.Raw = opt.Name + "." + a.Value.Raw
				}
			}
		} else {
			def.Directives = append(def.Directives, base.ModuleDirective(opt.Name, compiledPos()))
		}
	}

	if isM2MTable(def) {
		// check only 2 references
		if len(def.Directives.ForNames(referencesDirectiveName)) > 2 {
			return ErrorPosf(def.Position, "object %s should have only 2 @references directive", def.Name)
		}
	}

	if len(def.Interfaces) > 1 {
		return ErrorPosf(def.Position, "object %s should have only one interface AttributeValues", def.Name)
	}

	if len(def.Interfaces) == 1 && objectType != "" {
		return ErrorPosf(def.Position, "data object %s can't implement any interfaces", def.Name)
	}

	if len(def.Directives.ForNames(referencesDirectiveName)) > 0 {
		err := validateObjectReferencesDirectives(def)
		if err != nil {
			return err
		}
	}

	return nil
}

func addObjectReferencesQuery(schema *ast.SchemaDocument, def *ast.Definition, opt *Options) {
	if isM2MTable(def) { // skip m2m table
		return
	}

	for _, ref := range def.Directives.ForNames(referencesDirectiveName) {
		info := ReferencesInfo(ref)
		if info == nil {
			continue
		}
		references := schema.Definitions.ForName(info.ReferencesName)
		// add references query
		t := &ast.Type{NamedType: info.ReferencesName}
		var args ast.ArgumentDefinitionList
		if info.IsM2M { // request table
			t = ast.ListType(t, compiledPos())
			args = inputObjectQueryArgs(schema, references, true)
		}
		if !info.IsM2M {
			args = append(args, &ast.ArgumentDefinition{
				Name:        "inner",
				Description: "Apply inner join to reference record",
				Type:        ast.NamedType("Boolean", compiledPos()),
				Position:    compiledPos(),
			})
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        info.Query,
			Description: info.Description,
			Arguments:   args,
			Type:        t,
			Directives: ast.DirectiveList{
				referenceQueryDirective(info.ReferencesName, info.Name, info.IsM2M, info.M2MName),
				opt.catalog,
			},
			Position: CompiledPosName("add-ref-" + def.Name),
		})
		appendFilterInputObject(schema, def, def.Fields[len(def.Fields)-1])
		if info.IsM2M || info.ReferencesQuery == "" { // will add automatically by references directive
			continue
		}
		references.Fields = append(references.Fields, &ast.FieldDefinition{
			Name:        info.ReferencesQuery,
			Description: info.ReferencesDescription,
			Arguments:   inputObjectQueryArgs(schema, def, true),
			Type:        ast.ListType(ast.NamedType(def.Name, compiledPos()), compiledPos()),
			Directives: ast.DirectiveList{
				referenceQueryDirective(def.Name, info.Name, info.IsM2M, info.M2MName),
				opt.catalog,
			},
			Position: CompiledPosName("add-ref-" + def.Name),
		})
		// add filters to the input to references filter input if it is not present
		appendFilterInputObject(schema, references, references.Fields[len(references.Fields)-1])
		// add to references object
	}
}

func addObjectQuery(schema *ast.SchemaDocument, def *ast.Definition, opt *Options) error {
	if !IsDataObject(def) {
		return nil
	}
	isM2M := isM2MTable(def)

	cacheDirective := def.Directives.ForName(base.CacheDirectiveName)

	// add query
	dd := ast.DirectiveList{
		objectQueryDirective(def.Name, QueryTypeSelect),
		opt.catalog,
	}
	if cacheDirective != nil {
		dd = append(dd, cacheDirective)
	}
	// module directive
	moduleObject, err := moduleType(schema, ObjectModule(def), ModuleQuery)
	if err != nil {
		return err
	}
	name := def.Name
	if opt.AsModule {
		if on := objectDirectiveArgValue(def, base.OriginalNameDirectiveName, "name"); on != "" {
			name = on
		}
	}
	moduleObject.Fields = append(moduleObject.Fields, &ast.FieldDefinition{
		Name:        name,
		Description: def.Description,
		Arguments:   inputObjectQueryArgs(schema, def, false),
		Type:        ast.ListType(ast.NamedType(def.Name, compiledPos()), compiledPos()),
		Directives:  dd,
		Position:    compiledPos(),
	})
	def.Directives = append(def.Directives, objectQueryDirective(name, QueryTypeSelect))
	if !isM2M {
		// add to join object
		addObjectQueryToJoinsObject(opt.catalog, schema, def)

		uniqueArgs := inputObjectUniquesArgs(def)
		for suffix, arg := range uniqueArgs {
			moduleObject.Fields = append(moduleObject.Fields, &ast.FieldDefinition{
				Name:        name + suffix,
				Description: def.Description,
				Arguments:   arg,
				Type:        ast.NamedType(def.Name, compiledPos()),
				Directives: ast.DirectiveList{
					objectQueryDirective(def.Name, QueryTypeSelectOne),
					opt.catalog,
				},
				Position: compiledPos(),
			})
			def.Directives = append(def.Directives, objectQueryDirective(name+suffix, QueryTypeSelectOne))
		}
	}

	if opt.ReadOnly || def.Directives.ForName(base.ObjectTableDirectiveName) == nil {
		return nil
	}
	// add insert mutation
	dataInputName := inputObjectMutationInsertName(schema, def)
	insertArgs := inputObjectMutationInsertArgs(schema, def)
	if len(insertArgs) == 0 {
		return ErrorPosf(def.Position, "object %s should have at least one insertable field to create insert mutation", def.Name)
	}
	dd = ast.DirectiveList{
		objectMutationDirective(def.Name, MutationTypeInsert, dataInputName),
		opt.catalog,
	}
	if cacheDirective != nil {
		dd = append(dd, cacheDirective)
	}
	outType := ast.NamedType(def.Name, compiledPos())
	if len(objectPrimaryKeys(def)) == 0 || isM2M {
		outType = ast.NamedType(OperationResultTypeName, compiledPos())
	}

	moduleObject, err = moduleType(schema, ObjectModule(def), ModuleMutation)
	if err != nil {
		return err
	}
	moduleObject.Fields = append(moduleObject.Fields, &ast.FieldDefinition{
		Name:        mutationInsertPrefix + name,
		Description: def.Description,
		Arguments:   insertArgs,
		Type:        outType,
		Directives:  dd,
		Position:    compiledPos(),
	})
	def.Directives = append(def.Directives, objectMutationDirective(mutationInsertPrefix+name, MutationTypeInsert, dataInputName))

	// add update mutation
	dataInputName = inputObjectMutationDataName(schema, def)
	updateArgs := inputObjectMutationUpdateArgs(schema, def)
	dd = ast.DirectiveList{
		objectMutationDirective(def.Name, MutationTypeUpdate, dataInputName),
		opt.catalog,
	}
	if cacheDirective != nil {
		dd = append(dd, cacheDirective)
	}
	moduleObject.Fields = append(moduleObject.Fields, &ast.FieldDefinition{
		Name:        mutationUpdatePrefix + name,
		Description: def.Description,
		Arguments:   updateArgs,
		Type:        ast.NamedType(OperationResultTypeName, compiledPos()),
		Directives:  dd,
		Position:    compiledPos(),
	})
	def.Directives = append(def.Directives, objectMutationDirective(mutationUpdatePrefix+name, MutationTypeUpdate, dataInputName))

	// add delete mutation
	dd = ast.DirectiveList{
		objectMutationDirective(def.Name, MutationTypeDelete, ""),
		opt.catalog,
	}
	if cacheDirective != nil {
		dd = append(dd, cacheDirective)
	}
	moduleObject.Fields = append(moduleObject.Fields, &ast.FieldDefinition{
		Name:        mutationDeletePrefix + name,
		Description: def.Description,
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:     "filter",
				Type:     ast.NamedType(inputObjectFilterName(schema, def, false), nil),
				Position: compiledPos(),
			},
		},
		Type:       ast.NamedType(OperationResultTypeName, compiledPos()),
		Directives: dd,
		Position:   compiledPos(),
	})
	def.Directives = append(def.Directives, objectMutationDirective(mutationDeletePrefix+name, MutationTypeDelete, ""))
	return nil
}

func objectPrimaryKeys(def *ast.Definition) []string {
	var fields []string
	for _, f := range def.Fields {
		if f.Directives.ForName("pk") != nil {
			fields = append(fields, f.Name)
		}
	}
	return fields
}

func objectFieldByPath(defs Definitions, objectName string, path string, noSubQuery, noFuncs bool) *ast.FieldDefinition {
	pp := strings.Split(path, ".")
	if len(pp) == 0 {
		return nil
	}
	def := defs.ForName(objectName)
	if def == nil {
		return nil
	}
	field := def.Fields.ForName(pp[0])
	if field == nil {
		return nil
	}
	if noFuncs && IsFunctionCall(field) {
		return nil
	}
	if noSubQuery && IsSubQuery(field) {
	}
	if len(pp) == 1 {
		return field
	}
	if field.Type.NamedType == "" {
		return nil
	}
	return objectFieldByPath(defs, field.Type.NamedType, strings.Join(pp[1:], "."), noSubQuery, noFuncs)
}

func isM2MTable(def *ast.Definition) bool {
	d := def.Directives.ForName(base.ObjectTableDirectiveName)
	if d == nil {
		return false
	}
	if a := d.Arguments.ForName("is_m2m"); a != nil {
		return a.Value.Raw == "true"
	}
	return false
}

func extendObjectDefinition(schema *ast.SchemaDocument, origin, from *ast.Definition) error {
	if origin.Name != from.Name {
		return ErrorPosf(from.Position, "can't extend object %s with %s", origin.Name, from.Name)
	}
	if len(from.Interfaces) > 0 {
		return ErrorPosf(from.Position, "can't extend object %s with interfaces", from.Name)
	}
	for _, d := range from.Directives {
		// only dependencies and WFS directives are allowed
		if d.Name != base.DependencyDirectiveName &&
			d.Name != base.GisWFSDirectiveName {
			return ErrorPosf(from.Position, "can't extend object %s with directive %s", from.Name, d.Name)
		}
		if d.Name == base.DependencyDirectiveName {
			continue
		}
		od := origin.Directives.ForName(d.Name)
		if od != nil {
			return ErrorPosf(from.Position, "WFS directive %s already exists in object %s", d.Name, origin.Name)
		}
		origin.Directives = append(origin.Directives, d)
		err := validateGisDirectives(origin, nil)
		if err != nil {
			return err
		}
		// add WFS collection field
		err = addWFSCollectionField(schema, origin)
		if err != nil {
			return err
		}
	}

	if from.AfterDescriptionComment != nil && from.AfterDescriptionComment.Dump() != "" {
		origin.Description = ""
		for i, comment := range from.AfterDescriptionComment.List {
			if i > 0 {
				origin.Description += "\n"
			}
			origin.Description += strings.TrimSpace(comment.Text())
		}
	}
	var catalog *ast.Directive
	for _, field := range from.Fields {
		// skip stub field
		if field.Name == base.StubFieldName {
			continue
		}
		var err error
		switch {
		case IsSubQuery(field):
			if !IsDataObject(origin) {
				return ErrorPosf(field.Position, "subquery %s can be added only to data object", field.Name)
			}
			err = validateJoin(schema.Definitions, origin, field)
			if err != nil {
				return err
			}
			// add arguments to field
			def := schema.Definitions.ForName(field.Type.Name())
			if def == nil {
				return ErrorPosf(field.Position, "subquery %s not found", field.Type.Name())
			}
			info := DataObjectInfo(def)
			if info == nil {
				return ErrorPosf(field.Position, "subquery %s should be a data object", field.Type.Name())
			}
			field.Arguments = info.subQueryArguments()
			catalog = def.Directives.ForName(base.CatalogDirectiveName)
			if catalog == nil {
				return ErrorPosf(field.Position, "object %s should have @%s directive", origin.Name, base.CatalogDirectiveName)
			}
			catalog = base.CatalogDirective(
				directiveArgValue(catalog, "name"),
				directiveArgValue(catalog, "engine"),
			)
		case IsFunctionCall(field):
			if !IsDataObject(origin) {
				return ErrorPosf(field.Position, "function call %s can be added only to data object", field.Name)
			}
			err = validateFunctionCall(schema.Definitions, origin, field, true)
			if err != nil {
				return err
			}
			catalog = field.Directives.ForName(base.CatalogDirectiveName)
		case field.Directives.ForName(base.GisWFSFieldDirectiveName) != nil ||
			field.Directives.ForName(base.GisWFSExcludeDirectiveName) != nil ||
			field.Directives.ForName(base.FieldExcludeMCPDirectiveName) != nil:
			// add field directives to origin field
			if !IsDataObject(origin) {
				return ErrorPosf(field.Position, "Field %s directives can be added only to data object", field.Name)
			}
			originField := origin.Fields.ForName(field.Name)
			if originField == nil {
				return ErrorPosf(field.Position, "extended field %s not found in object %s", field.Name, origin.Name)
			}
			// wfs directives add it to the origin field
			if d := field.Directives.ForName(base.GisWFSFieldDirectiveName); d != nil &&
				originField.Directives.ForName(base.GisWFSFieldDirectiveName) == nil {
				originField.Directives = append(originField.Directives, d)
			}
			// wfs exclude directive add it to the origin field
			if d := field.Directives.ForName(base.GisWFSExcludeDirectiveName); d != nil &&
				originField.Directives.ForName(base.GisWFSExcludeDirectiveName) == nil {
				originField.Directives = append(originField.Directives, d)
			}
			// mcp exclude directive add it to the origin field
			if d := field.Directives.ForName(base.FieldExcludeMCPDirectiveName); d != nil &&
				originField.Directives.ForName(base.FieldExcludeMCPDirectiveName) == nil {
				originField.Directives = append(originField.Directives, d)
			}
			continue
		default:
			if field.Description == "" {
				return ErrorPosf(field.Position, "as a field %s only function calls or joins allowed or WFS directives or new comments", field.Name)
			}
			// find original field and add description if it is set in the extension
			originField := origin.Fields.ForName(field.Name)
			if originField == nil {
				return ErrorPosf(field.Position, "extended field %s not found in object %s", field.Name, origin.Name)
			}
			originField.Description = field.Description
			if field.BeforeDescriptionComment != nil && field.BeforeDescriptionComment.Dump() != "" {
				originField.BeforeDescriptionComment = field.BeforeDescriptionComment
			}
			if field.AfterDescriptionComment != nil && field.AfterDescriptionComment.Dump() != "" {
				originField.AfterDescriptionComment = field.AfterDescriptionComment
			}
			continue
		}
		newFieldIdx := len(origin.Fields)
		origin.Fields = append(origin.Fields, field)
		opt := &Options{}
		if catalog != nil {
			opt.catalog = catalog
		}
		// add aggregation fields for object
		// skip if field returns array
		var aggTypeName string
		if field.Type.NamedType != "" && IsScalarType(field.Type.Name()) {
			aggTypeName = ScalarTypes[field.Type.Name()].AggType
		}
		if !IsScalarType(field.Type.Name()) {
			def := schema.Definitions.ForName(field.Type.Name())
			if def == nil {
				return ErrorPosf(field.Position, "extension: object %s not found", field.Type.Name())
			}
			aggTypeName = objectAggregationTypeName(schema, opt, def, false)
		}
		if aggTypeName == "" {
			continue
		}
		if field.Type.NamedType == "" {
			origin.Fields = append(origin.Fields, &ast.FieldDefinition{
				Name:        field.Name + AggregationSuffix,
				Type:        ast.NamedType(aggTypeName, CompiledPosName("extension")),
				Arguments:   field.Arguments,
				Description: "The aggregation for " + field.Name,
				Directives:  ast.DirectiveList{aggQueryDirective(field, false), catalog},
				Position:    CompiledPosName("extension"),
			})
		}
		// add aggregation fields aggregation
		originCatalog := origin.Directives.ForName(base.CatalogDirectiveName)
		originAggTypeName := objectAggregationTypeName(schema, &Options{catalog: originCatalog}, origin, false)
		if originAT := schema.Definitions.ForName(originAggTypeName); originAT != nil {
			originAT.Fields = append(originAT.Fields, &ast.FieldDefinition{
				Name:        field.Name,
				Type:        ast.NamedType(aggTypeName, CompiledPosName("extension")),
				Arguments:   field.Arguments,
				Description: "The aggregation for " + field.Name,
				Directives: ast.DirectiveList{
					aggObjectFieldAggregationDirective(origin.Fields[newFieldIdx]),
					catalog},
				Position: CompiledPosName("extension"),
			})
			// add sub aggregation
			if field.Type.NamedType == "" {
				var subAggTypeName string
				if IsScalarType(field.Type.Name()) {
					subAggTypeName = subAggregationTypes[field.Type.Name()]
				}
				if !IsScalarType(field.Type.Name()) {
					subAggTypeName = objectAggregationTypeName(schema, &Options{
						catalog: catalog,
					}, schema.Definitions.ForName(aggTypeName), false)
				}
				if subAggTypeName != "" {
					originAT.Fields = append(originAT.Fields, &ast.FieldDefinition{
						Name:        field.Name + AggregationSuffix,
						Type:        ast.NamedType(subAggTypeName, CompiledPosName("extension")),
						Arguments:   field.Arguments,
						Description: "The aggregation for " + field.Name,
						Directives: ast.DirectiveList{
							aggObjectFieldAggregationDirective(origin.Fields[newFieldIdx+1]),
							catalog,
						},
						Position: CompiledPosName("extension"),
					})
				}
			}
		}
		if IsScalarType(field.Type.Name()) || field.Type.NamedType != "" {
			continue
		}
		// add bucket aggregation fields for object
		aggTypeName = objectAggregationTypeName(schema, opt, schema.Definitions.ForName(field.Type.Name()), true)
		if aggTypeName == "" {
			continue
		}
		origin.Fields = append(origin.Fields, &ast.FieldDefinition{
			Name: field.Name + BucketAggregationSuffix,
			Type: ast.ListType(
				ast.NamedType(aggTypeName, CompiledPosName("extension")),
				CompiledPosName("extension"),
			),
			Arguments:   field.Arguments,
			Description: "The bucket aggregation for " + field.Name,
			Directives:  ast.DirectiveList{aggQueryDirective(field, true), catalog},
			Position:    compiledPos(),
		})
	}
	return nil
}

type Object struct {
	Name                string
	sql                 string
	Type                string
	Catalog             string
	SoftDelete          bool
	IsM2M               bool
	softDeleteCondition string
	softDeleteSet       string
	IsCube              bool
	IsHypertable        bool

	inputFilterName     string
	inputFilterListName string

	InputArgsName string
	RequiredArgs  bool
	HasVectors    bool
	functionCall  bool

	def *ast.Definition
}

const (
	TableDataObject = "table"
	ViewDataObject  = "view"
)

func IsVectorSearcheble(def *ast.Definition) bool {
	if def == nil && !IsDataObject(def) {
		return false
	}
	for _, f := range def.Fields {
		if f.Type.NamedType == base.VectorTypeName {
			return true
		}
	}
	return false
}

func IsEmbeddedObject(def *ast.Definition) bool {
	if def == nil {
		return false
	}
	return def.Directives.ForName(base.EmbeddingsDirectiveName) != nil
}

func DataObjectInfo(def *ast.Definition) *Object {
	if !IsDataObject(def) {
		return nil
	}

	info := Object{
		Catalog: base.CatalogName(def),
		def:     def,
	}

	if def.Directives.ForName(base.ObjectTableDirectiveName) != nil {
		info.Name = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "name")
		info.SoftDelete = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "soft_delete") == "true"
		info.softDeleteCondition = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "soft_delete_cond")
		info.softDeleteSet = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "soft_delete_set")
		info.IsM2M = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "is_m2m") == "true"
		info.Type = TableDataObject
	}

	if def.Directives.ForName(base.ObjectViewDirectiveName) != nil {
		info.Name = objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "name")
		info.sql = objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "sql")
		info.Type = ViewDataObject
	}

	info.inputFilterName = objectDirectiveArgValue(def, FilterInputDirectiveName, "name")
	info.inputFilterListName = objectDirectiveArgValue(def, FilterListInputDirectiveName, "name")
	info.IsCube = def.Directives.ForName(objectCubeDirectiveName) != nil
	info.IsHypertable = def.Directives.ForName(objectHyperTableDirectiveName) != nil
	info.InputArgsName = objectDirectiveArgValue(def, base.ViewArgsDirectiveName, "name")
	info.RequiredArgs = objectDirectiveArgValue(def, base.ViewArgsDirectiveName, "required") == "true"
	info.HasVectors = IsVectorSearcheble(def)

	return &info
}

func (info *Object) SoftDeleteCondition(prefix string) string {
	if prefix != "" {
		prefix += "."
	}
	sql := info.softDeleteCondition
	for _, f := range ExtractFieldsFromSQL(info.softDeleteCondition) {
		sql = strings.ReplaceAll(sql, "["+f+"]", prefix+f)
	}
	return sql
}

func (info *Object) SoftDeleteSet(prefix string) string {
	if prefix != "" {
		prefix += "."
	}
	sql := info.softDeleteSet
	for _, f := range ExtractFieldsFromSQL(info.softDeleteSet) {
		sql = strings.ReplaceAll(sql, "["+f+"]", prefix+f)
	}
	return sql
}

func (info *Object) HasArguments() bool {
	return info.InputArgsName != ""
}

func (info *Object) subQueryArguments() ast.ArgumentDefinitionList {
	var args ast.ArgumentDefinitionList
	if info.InputArgsName != "" {
		if !info.RequiredArgs {
			args = append(args, &ast.ArgumentDefinition{
				Name:        "args",
				Description: "Arguments",
				Type:        ast.NamedType(info.InputArgsName, compiledPos()),
				Position:    compiledPos(),
			})
		}
		if info.RequiredArgs {
			args = append(args, &ast.ArgumentDefinition{
				Name:        "args",
				Description: "Arguments",
				Type:        ast.NonNullNamedType(info.InputArgsName, compiledPos()),
				Position:    compiledPos(),
			})
		}
	}
	return append(args,
		&ast.ArgumentDefinition{
			Name:        "filter",
			Description: "Filter",
			Type:        ast.NamedType(info.inputFilterName, compiledPos()),
			Position:    compiledPos(),
		},
		&ast.ArgumentDefinition{
			Name:        "order_by",
			Description: "Sort options for the result set",
			Type:        ast.ListType(ast.NamedType("OrderByField", compiledPos()), compiledPos()),
		},
		&ast.ArgumentDefinition{
			Name:         "limit",
			Description:  "Limit the number of returned objects",
			DefaultValue: &ast.Value{Raw: "2000", Kind: ast.IntValue},
			Type:         ast.NamedType("Int", compiledPos()),
			Position:     compiledPos(),
		},
		&ast.ArgumentDefinition{
			Name:         "offset",
			Description:  "Skip the first n objects",
			DefaultValue: &ast.Value{Raw: "0", Kind: ast.IntValue},
			Type:         ast.NamedType("Int", compiledPos()),
			Position:     compiledPos(),
		},
		&ast.ArgumentDefinition{
			Name:        "distinct_on",
			Description: "Distinct on the given fields",
			Type:        ast.ListType(ast.NamedType("String", compiledPos()), compiledPos()),
			Position:    compiledPos(),
		},
		&ast.ArgumentDefinition{
			Name:        "nested_order_by",
			Description: "Sort options for the result set",
			Type:        ast.ListType(ast.NamedType("OrderByField", compiledPos()), compiledPos()),
		},
		&ast.ArgumentDefinition{
			Name:        "nested_limit",
			Description: "Limit the number of returned objects",
			Type:        ast.NamedType("Int", compiledPos()),
			Position:    compiledPos(),
		},
		&ast.ArgumentDefinition{
			Name:        "nested_offset",
			Description: "Skip the first n objects",
			Type:        ast.NamedType("Int", compiledPos()),
			Position:    compiledPos(),
		},
	)
}

type sqlBuilder interface {
	SQLValue(any) (string, error)
	FunctionCall(name string, positional []any, named map[string]any) (string, error)
}

func (info *Object) ApplyArguments(defs Definitions, args map[string]any, builder sqlBuilder) (err error) {
	if !info.HasArguments() || len(args) == 0 {
		return nil
	}
	it := defs.ForName(info.InputArgsName)
	if it == nil {
		return ErrorPosf(info.def.Position, "input object %s not found", info.InputArgsName)
	}

	var posArgs []any
	namedArgs := make(map[string]any)

	for _, field := range it.Fields {
		val := args[field.Name]
		if val == nil && field.Type.NonNull {
			return ErrorPosf(field.Position, "argument %s is required", field.Name)
		}
		if info.sql != "" {
			sv, err := builder.SQLValue(val)
			if err != nil {
				return ErrorPosf(field.Position, "wrong argument %s value: %s", field.Name, err.Error())
			}
			info.sql = strings.ReplaceAll(info.sql, "[$"+field.Name+"]", sv)
			continue
		}
		if d := field.Directives.ForName(inputFieldNamedArgDirectiveName); d != nil {
			name := field.Name
			if fn := directiveArgValue(d, "name"); fn != "" {
				name = fn
			}
			namedArgs[name] = val
			continue
		}
		posArgs = append(posArgs, val)
	}
	if info.sql != "" {
		return nil
	}
	info.sql, err = builder.FunctionCall(info.Name, posArgs, namedArgs)
	info.functionCall = true
	return err
}

func (info *Object) SQL(ctx context.Context, prefix string) string {
	if prefix != "" {
		prefix += "."
	}

	if info.sql != "" {
		if info.functionCall {
			return info.sql
		}
		sql := info.sql
		if !strings.HasPrefix(sql, "(") || !strings.HasSuffix(sql, ")") {
			sql = "(" + sql + ")"
		}
		// prefix for the objects
		for _, dbObject := range ExtractFieldsFromSQL(sql) {
			if dbObject == catalogSystemVariableName {
				sql = strings.ReplaceAll(sql, "["+dbObject+"]", info.Catalog)
			}
			sql = strings.ReplaceAll(sql, "["+dbObject+"]", prefix+dbObject)
		}
		return sql
	}

	return prefix + info.Name
}

type ObjectQuery struct {
	Name string
	Type ObjectQueryType
}

func (info *Object) Queries() (queries []ObjectQuery) {
	for _, d := range info.def.Directives.ForNames(QueryDirectiveName) {
		name := directiveArgValue(d, "name")
		switch directiveArgValue(d, "type") {
		case queryTypeTextSelect:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeSelect})
		case queryTypeTextSelectOne:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeSelectOne})
		case queryTypeTextAggregate:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeAggregate})
		case queryTypeTextAggregateBucket:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeAggregateBucket})
		}
	}
	return queries
}

func (info *Object) AggregationTypeName(defs Definitions) string {
	name := buildObjectAggregationTypeName(info.def.Name, false, false)
	if defs.ForName(name) == nil {
		return ""
	}
	return name
}

func (info *Object) BucketAggregationTypeName(defs Definitions) string {
	name := buildObjectAggregationTypeName(info.def.Name, false, true)
	if defs.ForName(name) == nil {
		return ""
	}
	return name
}

func (info *Object) SubAggregationTypeName(defs Definitions) string {
	name := buildObjectAggregationTypeName(info.def.Name, true, false)
	if defs.ForName(name) == nil {
		return ""
	}
	return name
}

func (info *Object) InputFilterName() string {
	return objectDirectiveArgValue(info.def, "filter_input", "name")
}

func (info *Object) InputInsertDataName() string {
	for _, d := range info.def.Directives.ForNames(MutationDirectiveName) {
		if directiveArgValue(d, "type") == MutationTypeTextInsert {
			return directiveArgValue(d, DataInputDirectiveName)
		}
	}
	return ""
}

func (info *Object) InsertMutationName() string {
	for _, d := range info.def.Directives.ForNames(MutationDirectiveName) {
		if directiveArgValue(d, "type") == MutationTypeTextInsert {
			return directiveArgValue(d, "name")
		}
	}
	return ""
}

func (info *Object) InputUpdateDataName() string {
	for _, d := range info.def.Directives.ForNames(MutationDirectiveName) {
		if directiveArgValue(d, "type") == MutationTypeTextUpdate {
			return directiveArgValue(d, DataInputDirectiveName)
		}
	}
	return ""
}

func (info *Object) UpdateMutationName() string {
	for _, d := range info.def.Directives.ForNames(MutationDirectiveName) {
		if directiveArgValue(d, "type") == MutationTypeTextUpdate {
			return directiveArgValue(d, "name")
		}
	}
	return ""
}

func (info *Object) DeleteMutationName() string {
	for _, d := range info.def.Directives.ForNames(MutationDirectiveName) {
		if directiveArgValue(d, "type") == MutationTypeTextDelete {
			return directiveArgValue(d, "name")
		}
	}
	return ""
}

func (info *Object) FieldForName(name string) *Field {
	for _, field := range info.def.Fields {
		if field.Name == name {
			return FieldDefinitionInfo(field, info.def)
		}
	}
	return nil
}

func (info *Object) Definition() *ast.Definition {
	return info.def
}

func (info *Object) ReferencesQueryInfo(defs Definitions, name string) *References {
	field := info.FieldForName(name)
	if field == nil {
		return nil
	}
	refName := fieldDirectiveArgValue(field.def, fieldReferencesQueryDirectiveName, "name")
	def := info.def
	for _, ref := range def.Directives.ForNames(referencesDirectiveName) {
		if ri := ReferencesInfo(ref); ri.Name == refName {
			if ri.Query != name && ri.ReferencesName == info.def.Name {
				// recursive reference
				return referencesInfo(ref, def.Name, true)
			}
			return referencesInfo(ref, def.Name, false)
		}
	}
	refObject := fieldDirectiveArgValue(field.def, fieldReferencesQueryDirectiveName, "references_name")
	def = defs.ForName(refObject)
	if def == nil {
		return nil
	}
	for _, ref := range def.Directives.ForNames(referencesDirectiveName) {
		if ReferencesInfo(ref).Name == refName {
			return referencesInfo(ref, def.Name, true)
		}
	}
	return nil
}

func (info *Object) ReferencesQueryInfoByName(defs Definitions, name string) *References {
	for _, ref := range info.def.Directives.ForNames(referencesDirectiveName) {
		if ReferencesInfo(ref).Name == name {
			return referencesInfo(ref, info.def.Name, false)
		}
	}
	return nil
}

func (info *Object) M2MReferencesQueryInfo(defs Definitions, name string) *References {
	for _, ref := range info.def.Directives.ForNames(referencesDirectiveName) {
		if ReferencesInfo(ref).Name != name {
			return referencesInfo(ref, info.def.Name, false)
		}
	}
	return nil
}
