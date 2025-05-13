package compiler

import (
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"

	_ "embed"
)

type Options struct {
	Name       string
	ReadOnly   bool
	Prefix     string
	EngineType string
	AsModule   bool

	catalog *ast.Directive
}

// Compile compiles the source schema document with the given prefix.
func Compile(source *ast.SchemaDocument, opt Options) (*ast.Schema, error) {
	opt.catalog = base.CatalogDirective(opt.Name, opt.EngineType)

	if opt.Prefix != "" {
		source = addPrefix(source, &opt)
	}

	err := validateSource(source)
	if err != nil {
		return nil, err
	}

	doc := &ast.SchemaDocument{}
	for _, source := range base.Sources() {
		sd, err := parser.ParseSchema(source)
		if err != nil {
			return nil, err
		}
		doc.Merge(sd)
	}

	source.Merge(doc)

	err = validateSourceSchema(source, &opt)
	if err != nil {
		return nil, err
	}

	addReferencesQuery(source, &opt)

	err = addQueries(source, &opt)
	if err != nil {
		return nil, err
	}

	addQueryFields(source)

	addAggregationQueries(source, &opt)

	if opt.ReadOnly {
		source.Definitions = slices.DeleteFunc(source.Definitions, func(def *ast.Definition) bool {
			return def.Name == mutationBaseName || def.Name == base.FunctionMutationTypeName
		})
		for _, s := range source.Schema {
			s.OperationTypes = slices.DeleteFunc(s.OperationTypes, func(op *ast.OperationTypeDefinition) bool {
				return op.Type == mutationBaseName
			})
		}
	}

	for i, s := range source.Schema {
		if i == 0 && s.OperationTypes.ForType(queryBaseName) == nil &&
			source.Definitions.ForName(queryBaseName) != nil {
			source.Schema[0].OperationTypes = append(source.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
				Operation: ast.Query,
				Type:      queryBaseName,
				Position:  compiledPos(),
			})
		}

		if i == 0 && s.OperationTypes.ForType(mutationBaseName) == nil &&
			source.Definitions.ForName(mutationBaseName) != nil {
			source.Schema[0].OperationTypes = append(source.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
				Operation: ast.Mutation,
				Type:      mutationBaseName,
				Position:  compiledPos(),
			})
		}
	}

	if def := source.Definitions.ForName(queryBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionMutationTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	if def := source.Definitions.ForName(mutationBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionMutationTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionMutationTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	return validator.ValidateSchemaDocument(source)
}

// MergeSchema merges the given schemas to one common schema.
// Not system type names should be unique, except module query/mutation root objects.
func MergeSchema(schemas ...*ast.Schema) (*ast.Schema, error) {
	doc := &ast.SchemaDocument{}
	for _, source := range base.Sources() {
		sd, err := parser.ParseSchema(source)
		if err != nil {
			return nil, err
		}
		doc.Merge(sd)
	}

	// copy definitions and queries from the given schemas
	for _, schema := range schemas {
		for _, def := range schema.Types {
			def = copyDefinitions(doc, def)
			if info := ModuleRootInfo(def); info != nil {
				// what is here is not clear
			}
			// copy joins and spatial fields
			if def.Kind == ast.Object &&
				(strings.HasPrefix(def.Name, QueryTimeJoinObjectName) || strings.HasPrefix(def.Name, QueryTimeSpatialObject)) {
				if doc.Definitions.ForName(def.Name) == nil {
					doc.Definitions = append(doc.Definitions, def)
					continue
				}
				for _, field := range def.Fields {
					if strings.HasPrefix(field.Name, "_") {
						continue
					}
					doc.Definitions.ForName(def.Name).Fields = append(
						doc.Definitions.ForName(def.Name).Fields, field)
				}
				continue
			}
			if doc.Definitions.ForName(def.Name) == nil {
				doc.Definitions = append(doc.Definitions, def)
				continue
			}
			if info := ModuleRootInfo(def); info != nil {
				moduleRoot, err := moduleType(doc, info.Name, info.Type)
				if err != nil {
					return nil, err
				}
				for _, field := range def.Fields {
					if strings.HasPrefix(field.Name, "__") {
						continue
					}
					if info.Name == "" && field.Name == queryBaseFunctionFieldName {
						continue
					}
					if field.Directives.ForName(systemDirective.Name) != nil {
						continue
					}
					if moduleRoot.Fields.ForName(field.Name) != nil {
						continue
					}
					moduleRoot.Fields = append(moduleRoot.Fields, field)
				}
				continue
			}
			if IsScalarType(def.Name) || IsSystemType(def) {
				continue
			}
			return nil, ErrorPosf(def.Position, "definition %s already exists", def.Name)
		}
	}

	// sort queries
	for _, def := range doc.Definitions {
		if info := ModuleRootInfo(def); info == nil {
			continue
		}
		// sort fields
		slices.SortFunc(def.Fields, func(a, b *ast.FieldDefinition) int {
			// first modules root sorted by alphabet
			aType := doc.Definitions.ForName(a.Type.Name())
			bType := doc.Definitions.ForName(b.Type.Name())
			isAModuleRoot := ModuleRootInfo(aType) != nil
			isBModuleRoot := ModuleRootInfo(bType) != nil
			if isAModuleRoot && isBModuleRoot ||
				!isAModuleRoot && !isBModuleRoot {
				if a.Name < b.Name {
					return -1
				}
				if a.Name > b.Name {
					return 1
				}
				return 0
			}
			if isAModuleRoot {
				return -1
			}
			return 1

		})
	}

	if len(doc.Schema) != 1 {
		return nil, ErrorPosf(compiledPos(), "only one schema definition is allowed")
	}

	if doc.Schema[0].OperationTypes.ForType(queryBaseName) == nil &&
		doc.Definitions.ForName(queryBaseName) != nil {
		doc.Schema[0].OperationTypes = append(doc.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
			Operation: ast.Query,
			Type:      queryBaseName,
			Position:  compiledPos(),
		})
	}

	if doc.Schema[0].OperationTypes.ForType(mutationBaseName) == nil &&
		doc.Definitions.ForName(mutationBaseName) != nil {
		doc.Schema[0].OperationTypes = append(doc.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
			Operation: ast.Mutation,
			Type:      mutationBaseName,
			Position:  compiledPos(),
		})
	}

	if def := doc.Definitions.ForName(queryBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	if def := doc.Definitions.ForName(mutationBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionMutationTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionMutationTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	return validator.ValidateSchemaDocument(doc)
}

// graphql extensions can contains new types and fields
// it can't add new directives to the existing types
// fields can have only additional join/function_call/table_function_call_join directives
// in the future can be added role based access control directives to the fields or types
func AddExtensions(schema *ast.Schema, extension *ast.SchemaDocument) error {
	// copy definitions
	for _, def := range extension.Definitions {
		if _, ok := schema.Types[def.Name]; ok {
			return ErrorPosf(def.Position, "definition %s already exists", def.Name)
		}
		if len(def.Directives) != 0 {
			return ErrorPosf(def.Position, "definition %s shouldn't have any directive", def.Name)
		}
		if def.Kind != ast.Object && def.Kind != ast.InputObject {
			return ErrorPosf(def.Position, "definition %s should be object or input object", def.Name)
		}
		err := validateDefinition(SchemaDefs(schema), def, &Options{})
		if err != nil {
			return err
		}
		schema.Types[def.Name] = def
		schema.PossibleTypes[def.Name] = append(schema.PossibleTypes[def.Name], def)
	}

	// copy extensions
	for _, def := range extension.Extensions {
		origin := schema.Types[def.Name]
		if origin == nil {
			return ErrorPosf(def.Position, "extended definition %s not found", def.Name)
		}
		if origin.Kind != ast.Object {
			return ErrorPosf(def.Position, "extended definition %s should be object", def.Name)
		}
		if origin.Kind != def.Kind {
			return ErrorPosf(def.Position, "extended definition %s kind mismatch", def.Name)
		}

		err := extendObjectDefinition(SchemaDefs(schema), origin, def)
		if err != nil {
			return err
		}
	}

	return nil
}

func addPrefix(schema *ast.SchemaDocument, opt *Options) *ast.SchemaDocument {
	newSchema := &ast.SchemaDocument{
		Definitions: []*ast.Definition{},
		Extensions:  []*ast.Definition{},
	}
	for _, def := range schema.Definitions {
		addDefinitionPrefix(schema.Definitions, def, opt, true)
		newSchema.Definitions = append(newSchema.Definitions, def)
	}
	for _, def := range schema.Extensions {
		addDefinitionPrefix(schema.Definitions, def, opt, false)
		newSchema.Extensions = append(newSchema.Extensions, def)
	}

	return newSchema
}

func addReferencesQuery(schema *ast.SchemaDocument, opt *Options) {
	for _, def := range schema.Definitions {
		if !isM2MTable(def) {
			continue
		}
		addM2MReferences(schema.Definitions, def)
	}

	for _, def := range schema.Definitions {
		if !IsDataObject(def) {
			continue
		}
		addObjectReferencesQuery(schema, def, opt)
		addJoinsFilter(schema, def)
	}
}

func addQueryFields(schema *ast.SchemaDocument) {
	for _, def := range schema.Definitions {
		if !IsDataObject(def) {
			continue
		}
		addJoinsQueryFields(schema, def)
	}
}

func addQueries(schema *ast.SchemaDocument, opt *Options) error {
	for _, def := range schema.Definitions {
		if !IsDataObject(def) {
			continue
		}
		err := addObjectQuery(schema, def, opt)
		if err != nil {
			return err
		}
	}
	return nil
}

func addAggregationQueries(schema *ast.SchemaDocument, opt *Options) {
	for _, def := range schema.Definitions {
		mInfo := ModuleRootInfo(def)
		if !IsDataObject(def) && mInfo == nil &&
			def.Name != QueryTimeJoinObjectName &&
			def.Name != QueryTimeSpatialObject {
			continue
		}
		if mInfo != nil && mInfo.Type != ModuleQuery && mInfo.Type != ModuleFunction {
			continue
		}
		addAggregationQuery(schema, def, opt)
	}
}

func rootType(schema *ast.SchemaDocument, objectType ModuleObjectType) (*ast.Definition, error) {
	name := ""
	description := "The root query object of the module"
	switch objectType {
	case ModuleQuery:
		name = queryBaseName
		description = "The root query object of the module"
	case ModuleMutation:
		name = mutationBaseName
		description = "The root mutation object of the module"
		if len(schema.Schema) != 1 {
			return nil, ErrorPosf(compiledPos(), "only one schema definition is allowed")
		}
		if schema.Schema[0].OperationTypes.ForType(name) == nil {
			schema.Schema[0].OperationTypes = append(schema.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
				Operation: ast.Mutation,
				Type:      mutationBaseName,
				Position:  compiledPos(),
			})
		}
	case ModuleFunction:
		name = base.FunctionTypeName
		description = "The root function object of the module"
		qr, err := rootType(schema, ModuleQuery)
		if err != nil {
			return nil, err
		}
		if qr.Fields.ForName(queryBaseFunctionFieldName) == nil {
			qr.Fields = append(qr.Fields, &ast.FieldDefinition{
				Name:        queryBaseFunctionFieldName,
				Description: "The root function object of the module",
				Type:        ast.NamedType(name, nil),
				Position:    compiledPos(),
			})
		}
	case ModuleMutationFunction:
		name = base.FunctionMutationTypeName
		description = "The root function mutation object of the module"
		mr, err := rootType(schema, ModuleMutation)
		if err != nil {
			return nil, err
		}
		if mr.Fields.ForName(queryBaseFunctionFieldName) == nil {
			mr.Fields = append(mr.Fields, &ast.FieldDefinition{
				Name:        queryBaseFunctionFieldName,
				Description: "The root function mutation object of the module",
				Type:        ast.NamedType(name, nil),
				Position:    compiledPos(),
			})
		}
	}
	if def := schema.Definitions.ForName(name); def != nil {
		return def, nil
	}

	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        name,
		Description: description,
		Directives:  ast.DirectiveList{systemDirective},
		Position:    compiledPos(),
	}
	schema.Definitions = append(schema.Definitions, def)
	return def, nil
}
