package static

import (
	"fmt"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"
)

// initSystemSchema parses and validates all system type sources into a compiled
// *ast.Schema. This includes:
//   - base.Sources(): prelude, system_types, base directives, query directives, GIS directives
//   - types.Sources(): all registered scalar types with filter/aggregation inputs
//   - DDL control directive definitions: @if_not_exists, @drop, @replace, @drop_directive
func initSystemSchema() (*ast.Schema, error) {
	if err := types.Build(); err != nil {
		return nil, fmt.Errorf("validate scalar types: %w", err)
	}

	doc := &ast.SchemaDocument{}

	// Parse and merge all system sources.
	sources := base.Sources()
	sources = append(sources, types.Sources()...)
	for _, src := range sources {
		parsed, err := parser.ParseSchema(src)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", src.Name, err)
		}
		doc.Merge(parsed)
	}

	pos := &ast.Position{Src: &ast.Source{Name: "system-init"}}

	// Add types used by base.graphql directives but not in any .graphql source.
	doc.Definitions = append(doc.Definitions, extraSystemDefinitions(pos)...)

	// Add DDL control directive definitions.
	// These are used by compiler rules and Provider.Update() but are not
	// declared in any .graphql file since they are internal DDL markers.
	doc.Directives = append(doc.Directives, ddlDirectiveDefinitions(pos)...)

	// Ensure schema definition with Query operation type exists.
	if len(doc.Schema) == 0 {
		doc.Schema = append(doc.Schema, &ast.SchemaDefinition{})
	}
	if doc.Schema[0].OperationTypes.ForType("Query") == nil &&
		doc.Definitions.ForName("Query") != nil {
		doc.Schema[0].OperationTypes = append(doc.Schema[0].OperationTypes,
			&ast.OperationTypeDefinition{Operation: ast.Query, Type: "Query"})
	}

	schema, errs := validator.ValidateSchemaDocument(doc)
	if errs != nil {
		return nil, fmt.Errorf("validate system schema: %w", errs)
	}

	// Strip placeholder fields (_stub, _placeholder) from all Object types.
	// These exist only to satisfy the GraphQL parser (empty types are invalid)
	// but should never appear in the compiled schema.
	stripPlaceholderFields(schema)

	return schema, nil
}

// stripPlaceholderFields removes _stub and _placeholder fields from all Object
// types in the schema. These fields are used as parser placeholders in system_types.graphql.
func stripPlaceholderFields(schema *ast.Schema) {
	for _, def := range schema.Types {
		if def.Kind != ast.Object {
			continue
		}
		def.Fields = slices.DeleteFunc(def.Fields, func(f *ast.FieldDefinition) bool {
			return f.Name == "_stub" || f.Name == "_placeholder"
		})
	}
}

// ddlDirectiveDefinitions returns the DDL control directive definitions that
// must be present in the schema for gqlparser validation to accept them.
func ddlDirectiveDefinitions(pos *ast.Position) []*ast.DirectiveDefinition {
	return []*ast.DirectiveDefinition{
		{
			Name: base.IfNotExistsDirectiveName,
			Locations: []ast.DirectiveLocation{
				ast.LocationObject,
			},
			Position: pos,
		},
		{
			Name: base.DropDirectiveName,
			Locations: []ast.DirectiveLocation{
				ast.LocationObject,
				ast.LocationFieldDefinition,
				ast.LocationEnumValue,
				ast.LocationInputFieldDefinition,
			},
			Arguments: ast.ArgumentDefinitionList{
				{
					Name: "if_exists",
					Type: ast.NamedType("Boolean", pos),
				},
			},
			Position: pos,
		},
		{
			Name: base.ReplaceDirectiveName,
			Locations: []ast.DirectiveLocation{
				ast.LocationObject,
				ast.LocationFieldDefinition,
				ast.LocationEnumValue,
				ast.LocationInputFieldDefinition,
			},
			Position: pos,
		},
		{
			Name: base.DropDirectiveDirectiveName,
			Locations: []ast.DirectiveLocation{
				ast.LocationObject,
				ast.LocationFieldDefinition,
			},
			Arguments: ast.ArgumentDefinitionList{
				{
					Name: "name",
					Type: ast.NonNullNamedType("String", pos),
				},
			},
			Position: pos,
		},
	}
}

// extraSystemDefinitions returns type definitions that are referenced by
// base.graphql directives but not declared in any .graphql source file.
func extraSystemDefinitions(pos *ast.Position) ast.DefinitionList {
	systemDir := &ast.Directive{Name: "system", Position: pos}
	return ast.DefinitionList{
		// "any" scalar — used by @default(value: any) in base.graphql.
		{
			Kind:     ast.Scalar,
			Name:     "any",
			Position: pos,
		},
		// FilterOperator enum — used by @exclude_filter(operations: [FilterOperator!]) in base.graphql.
		{
			Kind:       ast.Enum,
			Name:       "FilterOperator",
			Directives: ast.DirectiveList{systemDir},
			EnumValues: ast.EnumValueList{
				{Name: "eq", Position: pos},
				{Name: "ne", Position: pos},
				{Name: "gt", Position: pos},
				{Name: "gte", Position: pos},
				{Name: "lt", Position: pos},
				{Name: "lte", Position: pos},
				{Name: "in", Position: pos},
				{Name: "between", Position: pos},
				{Name: "like", Position: pos},
				{Name: "ilike", Position: pos},
				{Name: "regex", Position: pos},
				{Name: "is_null", Position: pos},
				{Name: "contains", Position: pos},
				{Name: "intersects", Position: pos},
				{Name: "includes", Position: pos},
				{Name: "excludes", Position: pos},
				{Name: "upper", Position: pos},
				{Name: "lower", Position: pos},
				{Name: "upper_inclusive", Position: pos},
				{Name: "lower_inclusive", Position: pos},
				{Name: "upper_inf", Position: pos},
				{Name: "lower_inf", Position: pos},
				{Name: "within", Position: pos},
				{Name: "has", Position: pos},
				{Name: "has_all", Position: pos},
			},
			Position: pos,
		},
	}
}
