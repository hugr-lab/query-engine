package compiler

import "github.com/vektah/gqlparser/v2/ast"

const (
	QueryTimeJoinFieldName  = "_join"
	QueryTimeJoinObjectName = "_join"

	QueryTimeSpatialFieldName = "_spatial"
	QueryTimeSpatialObject    = "_spatial"
)

func spatialJoinObject(schema *ast.SchemaDocument) *ast.Definition {
	if def := schema.Definitions.ForName(QueryTimeSpatialObject); def != nil {
		return def
	}
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        QueryTimeSpatialObject,
		Description: "Spatial query object",
		Position:    &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
		Directives:  ast.DirectiveList{systemDirective},
	}
	schema.Definitions = append(schema.Definitions, def)
	return def
}

func joinObject(schema *ast.SchemaDocument) *ast.Definition {
	if def := schema.Definitions.ForName(QueryTimeJoinObjectName); def != nil {
		return def
	}
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        QueryTimeJoinObjectName,
		Description: "Join query object",
		Position:    &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
		Directives:  ast.DirectiveList{systemDirective},
	}
	schema.Definitions = append(schema.Definitions, def)
	return def
}

func addJoinsQueryFields(schema *ast.SchemaDocument, def *ast.Definition) {
	join := schema.Definitions.ForName(QueryTimeJoinObjectName)
	if join != nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        QueryTimeJoinFieldName,
			Description: "field for joining",
			Arguments: ast.ArgumentDefinitionList{
				{
					Name:        "fields",
					Description: "Source fields to use in join operation",
					Type:        ast.NonNullListType(ast.NonNullNamedType("String", compiledPos()), compiledPos()),
					Position:    compiledPos(),
				},
			},
			Type:     ast.NamedType(QueryTimeJoinObjectName, compiledPos()),
			Position: compiledPos(),
		})
	}

	if !hasGeomFields(def) {
		return
	}

	def.Fields = append(def.Fields, &ast.FieldDefinition{
		Name:        QueryTimeSpatialFieldName,
		Description: "field for spatial operations",
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:        "field",
				Description: "field for spatial operations",
				Type:        ast.NonNullNamedType("String", compiledPos()),
				Position:    compiledPos(),
			},
			{
				Name:        "type",
				Description: "type of spatial operation",
				Type:        ast.NonNullNamedType("GeometrySpatialQueryType", compiledPos()),
				Position:    compiledPos(),
			},
			{
				Name:        "buffer",
				Description: "buffer in meters for spatial operations",
				Type:        ast.NamedType("Int", compiledPos()),
				Position:    compiledPos(),
			},
		},
		Type:     ast.NamedType(QueryTimeSpatialObject, compiledPos()),
		Position: compiledPos(),
	})
}

func hasGeomFields(def *ast.Definition) bool {
	for _, f := range def.Fields {
		if f.Type.Name() == GeometryTypeName {
			return true
		}
	}
	return false
}

func addObjectQueryToJoinsObject(catalog *ast.Directive, schema *ast.SchemaDocument, def *ast.Definition) {
	if join := joinObject(schema); join != nil {
		args := inputObjectQueryArgs(schema, def, true)
		args = append(args,
			&ast.ArgumentDefinition{
				Name:        "fields",
				Description: "References fields for join in the same order",
				Type:        ast.NonNullListType(ast.NonNullNamedType("String", compiledPos()), compiledPos()),
				Position:    compiledPos(),
			},
		)
		join.Fields = append(join.Fields, &ast.FieldDefinition{
			Name:        def.Name,
			Description: def.Description,
			Arguments:   args,
			Type:        ast.ListType(ast.NamedType(def.Name, compiledPos()), compiledPos()),
			Directives: ast.DirectiveList{
				objectQueryDirective(def.Name, QueryTypeSelect),
				catalog,
			},
			Position: compiledPos(),
		})
	}

	// add to spatial if needed
	if hasGeomFields(def) {
		spatial := spatialJoinObject(schema)
		if spatial == nil {
			return
		}
		args := inputObjectQueryArgs(schema, def, true)
		args = append(args,
			&ast.ArgumentDefinition{
				Name:        "field",
				Description: "field for spatial operations",
				Type:        ast.NonNullNamedType("String", compiledPos()),
				Position:    compiledPos(),
			},
			&ast.ArgumentDefinition{
				Name:        "inner",
				Description: "inner join data",
				Type:        ast.NamedType("Boolean", compiledPos()),
				Position:    compiledPos(),
			},
		)

		spatial.Fields = append(spatial.Fields, &ast.FieldDefinition{
			Name:        def.Name,
			Description: def.Description,
			Arguments:   args,
			Type:        ast.ListType(ast.NamedType(def.Name, compiledPos()), compiledPos()),
			Directives: ast.DirectiveList{
				objectQueryDirective(def.Name, QueryTypeSelect),
				catalog,
			},
			Position: compiledPos(),
		})
	}
}
