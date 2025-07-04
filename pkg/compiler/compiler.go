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
			return def.Name == base.MutationBaseName || def.Name == base.FunctionMutationTypeName
		})
		for _, s := range source.Schema {
			s.OperationTypes = slices.DeleteFunc(s.OperationTypes, func(op *ast.OperationTypeDefinition) bool {
				return op.Type == base.MutationBaseName
			})
		}
	}

	for i, s := range source.Schema {
		if i == 0 && s.OperationTypes.ForType(base.QueryBaseName) == nil &&
			source.Definitions.ForName(base.QueryBaseName) != nil {
			source.Schema[0].OperationTypes = append(source.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
				Operation: ast.Query,
				Type:      base.QueryBaseName,
				Position:  compiledPos(),
			})
		}

		if i == 0 && s.OperationTypes.ForType(base.MutationBaseName) == nil &&
			source.Definitions.ForName(base.MutationBaseName) != nil {
			source.Schema[0].OperationTypes = append(source.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
				Operation: ast.Mutation,
				Type:      base.MutationBaseName,
				Position:  compiledPos(),
			})
		}
	}

	if def := source.Definitions.ForName(base.QueryBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionMutationTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	if def := source.Definitions.ForName(base.MutationBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionMutationTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionMutationTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	addH3Queries(source)

	return validator.ValidateSchemaDocument(source)
}

// MergeSchema merges the given schemas to one common schema.
// Not system type names should be unique, except module query/mutation root objects.
func MergeSchema(schemas ...*ast.Schema) (*ast.SchemaDocument, error) {
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
				(strings.HasPrefix(def.Name, base.QueryTimeJoinsTypeName) ||
					strings.HasPrefix(def.Name, base.QueryTimeSpatialTypeName) ||
					strings.HasPrefix(def.Name, base.H3DataQueryTypeName)) {
				if len(def.Fields) == 0 {
					continue
				}
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
					if HasSystemDirective(field.Directives) && field.Name != base.H3QueryFieldName {
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

	if doc.Schema[0].OperationTypes.ForType(base.QueryBaseName) == nil &&
		doc.Definitions.ForName(base.QueryBaseName) != nil {
		doc.Schema[0].OperationTypes = append(doc.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
			Operation: ast.Query,
			Type:      base.QueryBaseName,
			Position:  compiledPos(),
		})
	}

	if doc.Schema[0].OperationTypes.ForType(base.MutationBaseName) == nil &&
		doc.Definitions.ForName(base.MutationBaseName) != nil {
		doc.Schema[0].OperationTypes = append(doc.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
			Operation: ast.Mutation,
			Type:      base.MutationBaseName,
			Position:  compiledPos(),
		})
	}

	if def := doc.Definitions.ForName(base.QueryBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	if def := doc.Definitions.ForName(base.MutationBaseName); def != nil &&
		doc.Definitions.ForName(base.FunctionMutationTypeName) != nil &&
		def.Fields.ForName("function") == nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "function",
			Description: "Functions",
			Type:        ast.NamedType(base.FunctionMutationTypeName, compiledPos()),
			Position:    compiledPos(),
		})
	}

	return doc, nil
}

// CompileExtension validates the given extension schema document against the base schema.
// It checks for duplicate definitions, directive usage, and type compatibility.
// It returns the validated extension schema document, a list of depended catalogs, and an error if any validation fails.
func CompileExtension(schema *ast.Schema, extension *ast.SchemaDocument, opt Options) (*ast.Schema, *ast.SchemaDocument, []string, error) {
	opt.catalog = base.CatalogDirective(opt.Name, opt.EngineType)

	extSource := &ast.SchemaDocument{}
	dependencies := make([]string, 0)

	// rules:
	// - extension definitions can't contain data objects except views
	// - extension definitions can't contains modules, and functions
	// - extension definitions can't contains system types
	for _, def := range extension.Definitions {
		if def.Kind == ast.Object {
			if IsDataObject(def) {
				info := DataObjectInfo(def)
				if info.Type != ViewDataObject {
					return nil, nil, nil, ErrorPosf(def.Position, "extension definition %s can't contain data objects", def.Name)
				}
				for _, d := range def.Directives.ForNames(base.DependencyDirectiveName) {
					dn := directiveArgValue(d, "name")
					if !slices.Contains(dependencies, dn) {
						dependencies = append(dependencies, dn)
					}
				}
			}
			if ModuleRootInfo(def) != nil {
				return nil, nil, nil, ErrorPosf(def.Position, "extension definition %s can't contain modules", def.Name)
			}
			// shouldn't refer to the base schema types
		}
		if IsScalarType(def.Name) {
			return nil, nil, nil, ErrorPosf(def.Position, "extension definition %s can't contain system types", def.Name)
		}
		if IsSystemType(def) {
			return nil, nil, nil, ErrorPosf(def.Position, "extension definition %s can't contain system types", def.Name)
		}
	}

	//split to the schema extensions and extension data source schema
	var ee ast.DefinitionList
	for _, ext := range extension.Extensions {
		if len(ext.Directives.ForNames(base.DependencyDirectiveName)) != 0 {
			extSource.Extensions = append(extSource.Extensions, ext)
			for _, d := range ext.Directives.ForNames(base.DependencyDirectiveName) {
				dn := directiveArgValue(d, "name")
				if !slices.Contains(dependencies, dn) {
					dependencies = append(dependencies, dn)
				}
			}
			continue
		}
		ee = append(ee, ext)
	}
	// remove duplicate dependency

	extension.Extensions = ee
	if len(extension.Definitions) == 0 {
		return nil, extSource, dependencies, nil
	}
	schema, err := Compile(extension, opt)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(extSource.Extensions) != 0 {
		return schema, extSource, nil, nil
	}
	return schema, nil, nil, nil
}

// graphql extensions can contains new types and fields
// it can't add new directives to the existing types
// fields can have only additional join/function_call/table_function_call_join directives
func AddExtensions(origin *ast.SchemaDocument, extension *ast.SchemaDocument) (*ast.Schema, error) {
	// copy definitions
	for _, def := range extension.Definitions {
		if origin.Definitions.ForName(def.Name) != nil {
			return nil, ErrorPosf(def.Position, "definition %s already exists", def.Name)
		}
		if len(def.Directives) != 0 {
			return nil, ErrorPosf(def.Position, "definition %s shouldn't have any directive", def.Name)
		}
		if def.Kind != ast.Object && def.Kind != ast.InputObject {
			return nil, ErrorPosf(def.Position, "definition %s should be object or input object", def.Name)
		}
		err := validateDefinition(origin.Definitions, def, &Options{})
		if err != nil {
			return nil, err
		}
		origin.Definitions = append(origin.Definitions, def)
	}

	// make a copy of definition, before change
	for _, def := range extension.Extensions {
		originDef := origin.Definitions.ForName(def.Name)
		if originDef == nil {
			return nil, ErrorPosf(def.Position, "extended definition %s not found", def.Name)
		}
		if originDef.Kind != ast.Object {
			return nil, ErrorPosf(def.Position, "extended definition %s should be object", def.Name)
		}
		if originDef.Kind != def.Kind {
			return nil, ErrorPosf(def.Position, "extended definition %s kind mismatch", def.Name)
		}
		err := extendObjectDefinition(origin, originDef, def)
		if err != nil {
			return nil, err
		}
	}

	return validator.ValidateSchemaDocument(origin)
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
		if d := def.Directives.ForName(objectTableDirectiveName); d != nil &&
			directiveArgValue(d, "is_m2m") == "true" {
			continue
		}
		addJoinsQueryFields(schema, def)

		// add calculation fields
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
			def.Name != base.QueryTimeJoinsTypeName &&
			def.Name != base.QueryTimeSpatialTypeName {
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
		name = base.QueryBaseName
		description = "The root query object of the module"
	case ModuleMutation:
		name = base.MutationBaseName
		description = "The root mutation object of the module"
		if len(schema.Schema) != 1 {
			return nil, ErrorPosf(compiledPos(), "only one schema definition is allowed")
		}
		if schema.Schema[0].OperationTypes.ForType(name) == nil {
			schema.Schema[0].OperationTypes = append(schema.Schema[0].OperationTypes, &ast.OperationTypeDefinition{
				Operation: ast.Mutation,
				Type:      base.MutationBaseName,
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
		Directives:  ast.DirectiveList{base.SystemDirective},
		Position:    compiledPos(),
	}
	schema.Definitions = append(schema.Definitions, def)
	return def, nil
}
