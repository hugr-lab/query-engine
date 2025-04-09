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

// Compile compiles the source schema document with the given prefix.
func Compile(catalog *ast.Directive, source *ast.SchemaDocument, prefix string, readOnly bool) (*ast.Schema, error) {
	if prefix != "" {
		source = addPrefix(source, prefix+"_")
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

	err = validateSourceSchema(catalog, source, readOnly)
	if err != nil {
		return nil, err
	}

	addReferencesQuery(catalog, source)

	addQueries(catalog, source, readOnly)

	addQueryFields(source)

	addAggregationQueries(catalog, source)

	if readOnly {
		source.Definitions = slices.DeleteFunc(source.Definitions, func(def *ast.Definition) bool {
			return def.Name == mutationBaseName || def.Name == base.FunctionMutationTypeName
		})
		for _, s := range source.Schema {
			s.OperationTypes = slices.DeleteFunc(s.OperationTypes, func(op *ast.OperationTypeDefinition) bool {
				return op.Type == mutationBaseName
			})
		}
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
				moduleRoot := moduleType(doc, info.Name, info.Type)
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
		err := validateDefinition(SchemaDefs(schema), def)
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

func addPrefix(schema *ast.SchemaDocument, prefix string) *ast.SchemaDocument {
	newSchema := &ast.SchemaDocument{
		Definitions: []*ast.Definition{},
		Extensions:  []*ast.Definition{},
	}
	for _, def := range schema.Definitions {
		addDefinitionPrefix(schema.Definitions, def, prefix, true)
		newSchema.Definitions = append(newSchema.Definitions, def)
	}
	for _, def := range schema.Extensions {
		addDefinitionPrefix(schema.Definitions, def, prefix, false)
		newSchema.Extensions = append(newSchema.Extensions, def)
	}

	return newSchema
}

func addReferencesQuery(catalog *ast.Directive, schema *ast.SchemaDocument) {
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
		addObjectReferencesQuery(catalog, schema, def)
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

func addQueries(catalog *ast.Directive, schema *ast.SchemaDocument, readOnly bool) {
	for _, def := range schema.Definitions {
		if !IsDataObject(def) {
			continue
		}
		addObjectQuery(catalog, schema, def, readOnly)
	}
	assignFunctionByModules(schema)
}

func addAggregationQueries(catalog *ast.Directive, schema *ast.SchemaDocument) {
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
		addAggregationQuery(catalog, schema, def)
	}
}
