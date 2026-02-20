package validator_test

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func TestWalker_FieldEnrichment(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user(id: ID!): User }
		type User { id: ID!, name: String }
	`)
	doc := parseTestQuery(t, `query { user(id: "1") { id name } }`)

	v := validator.New() // no rules, only enrichment
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	// Check operation enrichment
	require.Len(t, doc.Operations, 1)
	op := doc.Operations[0]
	require.Len(t, op.SelectionSet, 1)

	userField := op.SelectionSet[0].(*ast.Field)
	assert.Equal(t, "user", userField.Name)
	assert.NotNil(t, userField.Definition, "field.Definition should be set")
	assert.NotNil(t, userField.ObjectDefinition, "field.ObjectDefinition should be set")
	assert.Equal(t, "Query", userField.ObjectDefinition.Name)

	// Check nested field enrichment
	require.Len(t, userField.SelectionSet, 2)
	idField := userField.SelectionSet[0].(*ast.Field)
	assert.Equal(t, "id", idField.Name)
	assert.NotNil(t, idField.Definition)
	assert.NotNil(t, idField.ObjectDefinition)
	assert.Equal(t, "User", idField.ObjectDefinition.Name)
}

func TestWalker_TypenameEnrichment(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)
	doc := parseTestQuery(t, `query { user { __typename id } }`)

	v := validator.New()
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	userField := doc.Operations[0].SelectionSet[0].(*ast.Field)
	typenameField := userField.SelectionSet[0].(*ast.Field)
	assert.Equal(t, "__typename", typenameField.Name)
	assert.NotNil(t, typenameField.Definition)
	assert.Equal(t, "String", typenameField.Definition.Type.Name())
}

func TestWalker_ArgumentEnrichment(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user(id: ID!): User }
		type User { id: ID!, name: String }
	`)
	doc := parseTestQuery(t, `query { user(id: "1") { id } }`)

	v := validator.New()
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	field := doc.Operations[0].SelectionSet[0].(*ast.Field)
	require.Len(t, field.Arguments, 1)
	arg := field.Arguments[0]
	assert.Equal(t, "id", arg.Name)
	assert.NotNil(t, arg.Value.ExpectedType)
	assert.Equal(t, "ID", arg.Value.ExpectedType.Name())
}

func TestWalker_VariableEnrichment(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user(id: ID!): User }
		type User { id: ID!, name: String }
	`)
	doc := parseTestQuery(t, `query GetUser($id: ID!) { user(id: $id) { id } }`)

	v := validator.New()
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	op := doc.Operations[0]
	require.Len(t, op.VariableDefinitions, 1)
	varDef := op.VariableDefinitions[0]
	assert.NotNil(t, varDef.Definition, "variable definition should be enriched")

	// Check that variable usage is marked
	field := op.SelectionSet[0].(*ast.Field)
	arg := field.Arguments[0]
	assert.NotNil(t, arg.Value.VariableDefinition)
	assert.True(t, varDef.Used)
}

func TestWalker_FragmentSpreadEnrichment(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user: User }
		type User { id: ID!, name: String }
	`)
	doc := parseTestQuery(t, `
		query { user { ...UserFields } }
		fragment UserFields on User { id name }
	`)

	v := validator.New()
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	// Check fragment definition enrichment
	require.Len(t, doc.Fragments, 1)
	frag := doc.Fragments[0]
	assert.NotNil(t, frag.Definition, "fragment.Definition should be set")

	// Check spread enrichment
	userField := doc.Operations[0].SelectionSet[0].(*ast.Field)
	spread := userField.SelectionSet[0].(*ast.FragmentSpread)
	assert.NotNil(t, spread.Definition, "spread.Definition should be set")
	assert.NotNil(t, spread.ObjectDefinition)
}

func TestWalker_InlineFragmentEnrichment(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user: User }
		type User { id: ID!, name: String }
	`)
	doc := parseTestQuery(t, `query { user { ... on User { id } } }`)

	v := validator.New()
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	userField := doc.Operations[0].SelectionSet[0].(*ast.Field)
	inlineFrag := userField.SelectionSet[0].(*ast.InlineFragment)
	assert.NotNil(t, inlineFrag.ObjectDefinition)

	idField := inlineFrag.SelectionSet[0].(*ast.Field)
	assert.NotNil(t, idField.Definition)
	assert.Equal(t, "User", idField.ObjectDefinition.Name)
}

func TestWalker_DirectiveEnrichment(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user: User }
		type User { id: ID!, name: String }
		directive @skip(if: Boolean!) on FIELD
	`)
	doc := parseTestQuery(t, `query { user { id @skip(if: true) } }`)

	v := validator.New()
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	userField := doc.Operations[0].SelectionSet[0].(*ast.Field)
	idField := userField.SelectionSet[0].(*ast.Field)
	require.Len(t, idField.Directives, 1)
	dir := idField.Directives[0]
	assert.NotNil(t, dir.Definition, "directive.Definition should be set")
	assert.Equal(t, ast.LocationField, dir.Location)
}

func TestWalker_MutationOperation(t *testing.T) {
	p := newTestProvider(t, `
		type Query { ok: Boolean }
		type Mutation { createUser(name: String!): User }
		type User { id: ID!, name: String }
	`)
	doc := parseTestQuery(t, `mutation { createUser(name: "test") { id } }`)

	v := validator.New()
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	field := doc.Operations[0].SelectionSet[0].(*ast.Field)
	assert.Equal(t, "createUser", field.Name)
	assert.NotNil(t, field.Definition)
	assert.Equal(t, "Mutation", field.ObjectDefinition.Name)
}

// --- test helpers ---

func parseTestQuery(t *testing.T, query string) *ast.QueryDocument {
	t.Helper()
	doc, err := parser.ParseQuery(&ast.Source{Input: query})
	require.NoError(t, err)
	return doc
}

// testProvider wraps a compiled *ast.Schema and implements validator.TypeResolver.
type testProvider struct {
	schema *ast.Schema
}

func newTestProvider(t *testing.T, sdl string) *testProvider {
	t.Helper()
	src := &ast.Source{Input: sdl, BuiltIn: false}
	doc, parseErr := parser.ParseSchema(src)
	require.NoError(t, parseErr)

	schema, err := compileSchema(doc)
	require.NoError(t, err)
	return &testProvider{schema: schema}
}

func compileSchema(doc *ast.SchemaDocument) (*ast.Schema, error) {
	// Merge built-in types
	builtinSrc := &ast.Source{Input: builtinSchema, BuiltIn: true}
	builtinDoc, err := parser.ParseSchema(builtinSrc)
	if err != nil {
		return nil, err
	}
	doc.Merge(builtinDoc)

	schema := &ast.Schema{
		Types:         make(map[string]*ast.Definition),
		Directives:    make(map[string]*ast.DirectiveDefinition),
		Implements:    make(map[string][]*ast.Definition),
		PossibleTypes: make(map[string][]*ast.Definition),
	}

	for _, def := range doc.Definitions {
		schema.Types[def.Name] = def
	}
	for _, dir := range doc.Directives {
		schema.Directives[dir.Name] = dir
	}

	// Set root types
	if q := schema.Types["Query"]; q != nil {
		schema.Query = q
	}
	if m := schema.Types["Mutation"]; m != nil {
		schema.Mutation = m
	}
	if s := schema.Types["Subscription"]; s != nil {
		schema.Subscription = s
	}

	// Compute implements and possible types
	for _, def := range doc.Definitions {
		if def.Kind == ast.Object {
			for _, iface := range def.Interfaces {
				ifaceDef := schema.Types[iface]
				if ifaceDef != nil {
					schema.AddImplements(def.Name, ifaceDef)
					schema.AddPossibleType(iface, def)
				}
			}
		}
	}

	return schema, nil
}

func (p *testProvider) ForName(_ context.Context, name string) *ast.Definition {
	return p.schema.Types[name]
}

func (p *testProvider) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	return p.schema.Directives[name]
}

func (p *testProvider) QueryType(_ context.Context) *ast.Definition {
	return p.schema.Query
}

func (p *testProvider) MutationType(_ context.Context) *ast.Definition {
	return p.schema.Mutation
}

func (p *testProvider) SubscriptionType(_ context.Context) *ast.Definition {
	return p.schema.Subscription
}

func (p *testProvider) PossibleTypes(_ context.Context, def *ast.Definition) []*ast.Definition {
	return p.schema.GetPossibleTypes(def)
}

func (p *testProvider) Implements(_ context.Context, def *ast.Definition) []*ast.Definition {
	return p.schema.GetImplements(def)
}

const builtinSchema = `
scalar Int
scalar Float
scalar String
scalar Boolean
scalar ID

directive @skip(if: Boolean!) on FIELD | FRAGMENT_SPREAD | INLINE_FRAGMENT
directive @include(if: Boolean!) on FIELD | FRAGMENT_SPREAD | INLINE_FRAGMENT
directive @deprecated(reason: String = "No longer supported") on FIELD_DEFINITION | ARGUMENT_DEFINITION | INPUT_FIELD_DEFINITION | ENUM_VALUE
directive @specifiedBy(url: String!) on SCALAR
`
