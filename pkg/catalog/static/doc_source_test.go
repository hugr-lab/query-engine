package static_test

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func TestDocSource_ForName(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	src := static.SourceFromDocument(doc)

	def := src.ForName(context.Background(), "Query")
	require.NotNil(t, def)
	assert.Equal(t, "Query", def.Name)

	assert.Nil(t, src.ForName(context.Background(), "NonExistent"))
}

func TestDocSource_DirectiveForName(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on FIELD
	`)
	src := static.SourceFromDocument(doc)

	dir := src.DirectiveForName(context.Background(), "cached")
	require.NotNil(t, dir)
	assert.Equal(t, "cached", dir.Name)

	assert.Nil(t, src.DirectiveForName(context.Background(), "unknown"))
}

func TestDocSource_AddDefinition(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	src := static.SourceFromDocument(doc)

	src.AddDefinition(&ast.Definition{
		Kind: ast.Object,
		Name: "User",
		Fields: ast.FieldList{
			{Name: "id", Type: ast.NamedType("ID", nil)},
		},
	})

	def := src.ForName(context.Background(), "User")
	require.NotNil(t, def)
	assert.Equal(t, "User", def.Name)
}

func TestDocSource_RemoveDefinitions(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		type User { id: ID }
	`)
	src := static.SourceFromDocument(doc)

	require.NotNil(t, src.ForName(context.Background(), "User"))

	src.RemoveDefinitions(func(d *ast.Definition) bool {
		return d.Name == "User"
	})

	assert.Nil(t, src.ForName(context.Background(), "User"))
	assert.NotNil(t, src.ForName(context.Background(), "Query"))
}

func TestDocSource_Extensions(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	src := static.SourceFromDocument(doc)

	// Initially no extensions
	count := 0
	src.Extensions(func(d *ast.Definition) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count)

	// Add extension
	src.AddExtension(&ast.Definition{
		Kind: ast.Object,
		Name: "Query",
		Fields: ast.FieldList{
			{Name: "name", Type: ast.NamedType("String", nil)},
		},
	})

	count = 0
	src.Extensions(func(d *ast.Definition) bool {
		count++
		return true
	})
	assert.Equal(t, 1, count)

	// Clear extensions
	src.ClearExtensions()
	count = 0
	src.Extensions(func(d *ast.Definition) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count)
}

func TestDocSource_Definitions_Iterator(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		type User { id: ID }
	`)
	src := static.SourceFromDocument(doc)

	var names []string
	src.Definitions(func(d *ast.Definition) bool {
		names = append(names, d.Name)
		return true
	})
	assert.Contains(t, names, "Query")
	assert.Contains(t, names, "User")
}

func TestDocSource_Definitions_EarlyStop(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		type User { id: ID }
		type Post { id: ID }
	`)
	src := static.SourceFromDocument(doc)

	count := 0
	src.Definitions(func(d *ast.Definition) bool {
		count++
		return false // stop after first
	})
	assert.Equal(t, 1, count)
}

func TestDocSource_DirectiveDefinitions_Iterator(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on FIELD
		directive @auth(role: String!) on FIELD_DEFINITION
	`)
	src := static.SourceFromDocument(doc)

	var names []string
	src.DirectiveDefinitions(func(d *ast.DirectiveDefinition) bool {
		names = append(names, d.Name)
		return true
	})
	assert.Contains(t, names, "cached")
	assert.Contains(t, names, "auth")
}

func TestDocSource_AddDirectiveDefinition(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	src := static.SourceFromDocument(doc)

	src.AddDirectiveDefinition(&ast.DirectiveDefinition{
		Name:      "custom",
		Locations: []ast.DirectiveLocation{ast.LocationField},
	})

	dir := src.DirectiveForName(context.Background(), "custom")
	require.NotNil(t, dir)
	assert.Equal(t, "custom", dir.Name)
}

func TestDocSource_Provider(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	src := static.SourceFromDocument(doc)

	require.NotNil(t, src)

	qType := src.ForName(context.Background(), "Query")
	require.NotNil(t, qType)
	assert.Equal(t, "Query", qType.Name)
}

// --- helpers ---

func parseSchemaDoc(t *testing.T, sdl string) *ast.SchemaDocument {
	t.Helper()
	doc, err := parser.ParseSchema(&ast.Source{Input: sdl})
	require.NoError(t, err)
	return doc
}
