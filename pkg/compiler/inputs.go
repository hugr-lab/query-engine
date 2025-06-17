package compiler

import (
	"log"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const (
	filterInputDirectiveName        = "filter_input"
	filterListInputDirectiveName    = "filter_list_input"
	inputFieldNamedArgDirectiveName = "named"
)

func addInputPrefix(defs Definitions, def *ast.Definition, opt *Options, addOriginal bool) {
	if def.Kind != ast.InputObject {
		return
	}
	if addOriginal {
		def.Directives = append(def.Directives, base.OriginalNameDirective(def.Name))
	}
	def.Name = opt.Prefix + "_" + def.Name

	for _, field := range def.Fields {
		typeName := field.Type.Name()
		if IsScalarType(typeName) {
			continue
		}
		field.Type = typeWithPrefix(defs, field.Type, opt.Prefix+"_")
	}
}

func validateInputObject(def *ast.Definition) error {
	for _, d := range def.Directives {
		switch d.Name {
		case base.OriginalNameDirectiveName:
		default:
			return ErrorPosf(def.Position, "input object %s shouldn't have any directive", def.Name)
		}
	}
	var errs gqlerror.List
	for _, field := range def.Fields {
		// check shouldn't any field have any directive
		for _, d := range field.Directives {
			switch d.Name {
			case base.DeprecatedDirectiveName,
				base.FieldSourceDirectiveName,
				inputFieldNamedArgDirectiveName:
			default:
				errs = append(errs, ErrorPosf(d.Position, "input object field %s shouldn't have directive %s", field.Name, d.Name))
			}
		}
	}
	if len(errs) != 0 {
		return &errs
	}

	return nil
}

func inputObjectQueryArgs(schema *ast.SchemaDocument, def *ast.Definition, forSubQuery bool) ast.ArgumentDefinitionList {
	if !IsDataObject(def) {
		return nil
	}
	var args ast.ArgumentDefinitionList
	if d := def.Directives.ForName(base.ViewArgsDirectiveName); d != nil {
		argInput := directiveArgValue(d, "name")
		required := directiveArgValue(d, "required") == "true"
		var argType *ast.Type
		if required {
			argType = ast.NonNullNamedType(argInput, compiledPos())
		} else {
			argType = ast.NamedType(argInput, compiledPos())
		}
		args = append(args,
			&ast.ArgumentDefinition{
				Name:        "args",
				Description: "Arguments for the view",
				Type:        argType,
				Position:    compiledPos(),
			},
		)
	}
	args = append(args,
		&ast.ArgumentDefinition{
			Name:        "filter",
			Description: "Filter",
			Type:        ast.NamedType(inputObjectFilterName(schema, def, false), compiledPos()),
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
	)
	if forSubQuery {
		args = append(args,
			&ast.ArgumentDefinition{
				Name:         "inner",
				Description:  "Apply inner join to the result set",
				Type:         ast.NamedType("Boolean", compiledPos()),
				DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue},
				Position:     compiledPos(),
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
	return args
}

func inputObjectFilterName(schema *ast.SchemaDocument, def *ast.Definition, isList bool) string {
	inputName := def.Name + "_filter"
	if d := def.Directives.ForName(filterInputDirectiveName); d != nil {
		inputName = directiveArgValue(d, "name")
	}
	if isList {
		inputName = def.Name + "_list_filter"
		if d := def.Directives.ForName(filterListInputDirectiveName); d != nil {
			inputName = directiveArgValue(d, "name")
		}
	}
	if t := schema.Definitions.ForName(inputName); t != nil {
		return inputName
	}

	inputObject := &ast.Definition{
		Kind:        ast.InputObject,
		Name:        inputName,
		Description: "Filter for " + def.Name + " objects",
		Directives:  []*ast.Directive{objectFilterInputDirective(def.Name, isList)},
		Position:    CompiledPosName(inputName),
	}
	schema.Definitions = append(schema.Definitions, inputObject)
	def.Directives = append(def.Directives, objectFilterInputDirective(inputName, isList))

	if isList {
		objectInput := inputObjectFilterName(schema, def, false)
		inputObject.Fields = append(inputObject.Fields,
			&ast.FieldDefinition{Name: "any_of", Type: ast.NamedType(objectInput, compiledPos())},
			&ast.FieldDefinition{Name: "all_of", Type: ast.NamedType(objectInput, compiledPos())},
			&ast.FieldDefinition{Name: "none_of", Type: ast.NamedType(objectInput, compiledPos())},
		)
		return inputName
	}

	for _, field := range def.Fields {
		if IsFunctionCall(field) { // skip function calls
			continue
		}
		if field.Directives.ForName(fieldJoinDirectiveName) != nil || IsExtraField(field) {
			continue
		}
		exclude := field.Directives.ForName(fieldExcludeFilterDirectiveName)
		if exclude != nil {
			if a := exclude.Arguments.ForName("operators"); a != nil {
				if len(a.Value.Children) == 0 {
					continue
				}
			}
		}
		var typeName string
		t := schema.Definitions.ForName(field.Type.Name())
		if t == nil {
			log.Printf("warn: type %s not found for field %s", field.Type.Name(), field.Name)
			continue
		}
		switch {
		case t.Kind == ast.Object:
			// skip parametrized views
			if t.Directives.ForName(objectViewDirectiveName) != nil &&
				t.Directives.ForName(base.ViewArgsDirectiveName) != nil {
				continue
			}
			if !IsDataObject(t) && field.Type.NamedType == "" {
				// skip non-data objects list types for filtering
				// only data objects lists are allowed to filter
				continue
			}
			typeName = inputObjectFilterName(schema, t, field.Type.NamedType == "")
		case t.Kind == ast.Scalar && exclude == nil:
			typeName, _ = scalarFilterInputTypeName(field.Type.Name(), field.Type.NamedType == "")
		case t.Kind == ast.Scalar && exclude != nil:
			filter := shrunkScalarFilterType(def.Name, field.Name, field.Type.Name(), field.Type.NamedType == "", exclude)
			if filter != nil {
				schema.Definitions = append(schema.Definitions, filter)
				typeName = filter.Name
			}
		}
		if typeName == "" {
			continue
		}
		ft := ast.NamedType(typeName, compiledPos())
		if d := field.Directives.ForName(fieldFilterRequiredDirectiveName); d != nil {
			ft.NonNull = true
		}
		inputObject.Fields = append(inputObject.Fields, &ast.FieldDefinition{
			Name:        field.Name,
			Description: field.Description,
			Type:        ft,
			Directives:  field.Directives,
			Position:    CompiledPosName(field.Name),
		})
	}
	if !IsDataObject(def) {
		return inputName
	}

	inputObject.Fields = append(inputObject.Fields,
		&ast.FieldDefinition{
			Name:     "_and",
			Type:     ast.ListType(ast.NamedType(inputName, compiledPos()), compiledPos()),
			Position: compiledPos(),
		},
		&ast.FieldDefinition{
			Name:     "_or",
			Type:     ast.ListType(ast.NamedType(inputName, compiledPos()), compiledPos()),
			Position: compiledPos(),
		},
		&ast.FieldDefinition{
			Name:     "_not",
			Type:     ast.NamedType(inputName, compiledPos()),
			Position: compiledPos(),
		},
	)

	return inputName
}

func appendFilterInputObject(schema *ast.SchemaDocument, def *ast.Definition, field *ast.FieldDefinition) {
	// skip parametrized views
	t := schema.Definitions.ForName(field.Type.Name())
	if t.Directives.ForName(objectViewDirectiveName) != nil &&
		t.Directives.ForName(base.ViewArgsDirectiveName) != nil {
		return
	}
	inputName := inputObjectFilterName(schema, def, false)
	input := schema.Definitions.ForName(inputName)
	if input == nil {
		return
	}
	if input.Fields.ForName(field.Name) != nil {
		return
	}
	tn := inputObjectFilterName(schema, schema.Definitions.ForName(field.Type.Name()), field.Type.NamedType == "")
	input.Fields = append(input.Fields, &ast.FieldDefinition{
		Name:        field.Name,
		Description: field.Description,
		Type:        ast.NamedType(tn, compiledPos()),
		Directives:  field.Directives,
		Position:    CompiledPosName(field.Name),
	})
}

func inputObjectMutationInsertArgs(schema *ast.SchemaDocument, def *ast.Definition) ast.ArgumentDefinitionList {
	if !IsDataObject(def) {
		return nil
	}
	return ast.ArgumentDefinitionList{
		{
			Name:        "data",
			Description: "Inserted data",
			Type:        ast.NonNullNamedType(inputObjectMutationInsertName(schema, def), nil),
			Position:    compiledPos(),
		},
	}
}

func inputObjectMutationInsertName(schema *ast.SchemaDocument, def *ast.Definition) string {
	inputName := def.Name + "_mut_input_data"
	if d := def.Directives.ForName("insert_input"); d != nil {
		inputName = directiveArgValue(d, "name")
	}
	if t := schema.Definitions.ForName(inputName); t != nil {
		return inputName
	}
	input := &ast.Definition{
		Kind:        ast.InputObject,
		Name:        inputName,
		Description: "Insert input for " + def.Name + " objects",
		Directives:  []*ast.Directive{objectDataInputDirective(def.Name)},
		Position:    CompiledPosName(inputName),
	}
	schema.Definitions = append(schema.Definitions, input)
	def.Directives = append(def.Directives, objectDataInputDirective(inputName))

	for _, field := range def.Fields {
		if IsFunctionCall(field) {
			continue
		}
		if field.Directives.ForName(JoinDirectiveName) != nil ||
			field.Directives.ForName(base.FieldSqlDirectiveName) != nil ||
			IsExtraField(field) {
			continue
		}
		t := copyType(field.Type)
		if !IsScalarType(t.Name()) {
			td := schema.Definitions.ForName(t.Name())
			if td == nil {
				log.Printf("warn: type %s not found for field %s", t.Name(), field.Name)
				continue
			}
			if td.Directives.ForName(objectViewDirectiveName) != nil {
				continue
			}
			tn := inputObjectMutationInsertName(schema, td)
			isList := t.NamedType == ""
			t = ast.NamedType(tn, compiledPos())
			if isList {
				t = ast.ListType(t, compiledPos())
			}
		}
		t.NonNull = false
		input.Fields = append(input.Fields, &ast.FieldDefinition{
			Name:        field.Name,
			Description: field.Description,
			Type:        t,
			Position:    CompiledPosName(field.Name),
		})
	}
	return inputName
}

func inputObjectUniquesArgs(def *ast.Definition) map[string]ast.ArgumentDefinitionList {
	var uniques = make(map[string]ast.ArgumentDefinitionList)
	pk := objectPrimaryKeys(def)
	if len(pk) != 0 {
		args := ast.ArgumentDefinitionList{}
		for _, key := range pk {
			field := def.Fields.ForName(key)
			args = append(args, &ast.ArgumentDefinition{
				Name:        key,
				Description: field.Description,
				Type:        ast.NonNullNamedType(field.Type.Name(), compiledPos()),
				Position:    compiledPos(),
			})
		}
		uniques[ObjectQueryByPKSuffix] = args
	}

	for _, d := range def.Directives.ForNames(objectUniqueDirectiveName) {
		u := UniqueInfo(d)
		if u.Skip {
			continue
		}
		args := ast.ArgumentDefinitionList{}
		for _, key := range u.Fields {
			field := def.Fields.ForName(key)
			args = append(args, &ast.ArgumentDefinition{
				Name:        key,
				Description: field.Description,
				Type:        ast.NonNullNamedType(field.Type.Name(), compiledPos()),
				Position:    CompiledPosName(field.Name),
			})
		}
		uniques["_"+u.QuerySuffix] = args
	}

	return uniques
}

func inputObjectMutationDataName(schema *ast.SchemaDocument, def *ast.Definition) string {
	inputName := def.Name + "_mut_data"
	if t := schema.Definitions.ForName(inputName); t != nil {
		return inputName
	}
	input := &ast.Definition{
		Kind:        ast.InputObject,
		Name:        inputName,
		Description: "Data input for " + def.Name + " objects",
		Directives:  []*ast.Directive{objectDataInputDirective(def.Name)},
		Position:    CompiledPosName(inputName),
	}
	schema.Definitions = append(schema.Definitions, input)
	def.Directives = append(def.Directives, objectDataInputDirective(inputName))

	for _, field := range def.Fields {
		if IsFunctionCall(field) || IsReferencesSubquery(field) {
			continue
		}
		if field.Directives.ForName(fieldJoinDirectiveName) != nil ||
			field.Directives.ForName(base.FieldSqlDirectiveName) != nil ||
			IsExtraField(field) {
			continue
		}
		t := copyType(field.Type)
		if !IsScalarType(field.Type.Name()) {
			t = ast.NamedType(inputObjectMutationDataName(schema, schema.Definitions.ForName(field.Type.Name())), compiledPos())
			if field.Type.NamedType == "" {
				t = ast.ListType(t, compiledPos())
			}
		}
		t.NonNull = false
		input.Fields = append(input.Fields, &ast.FieldDefinition{
			Name:        field.Name,
			Description: field.Description,
			Type:        t,
			Position:    CompiledPosName(field.Name),
		})
	}

	return inputName
}

func objectFilterInputDirective(name string, isList bool) *ast.Directive {
	if isList {
		return &ast.Directive{Name: filterListInputDirectiveName,
			Arguments: []*ast.Argument{
				{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}, Position: compiledPos()},
			},
			Position: compiledPos(),
		}
	}
	return &ast.Directive{Name: filterInputDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}, Position: compiledPos()},
		},
		Position: compiledPos(),
	}
}

func objectDataInputDirective(name string) *ast.Directive {
	return &ast.Directive{Name: "data_input",
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}, Position: compiledPos()},
		},
		Position: compiledPos(),
	}
}
