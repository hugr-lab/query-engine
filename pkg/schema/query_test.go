package schema_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func newStaticProvider(t *testing.T, sdl string) schema.Provider {
	t.Helper()
	src := &ast.Source{Input: sdl, BuiltIn: false}
	doc, err := parser.ParseSchema(src)
	require.NoError(t, err)

	builtinDoc, err := parser.ParseSchema(&ast.Source{Input: builtinSDL, BuiltIn: true})
	require.NoError(t, err)
	doc.Merge(builtinDoc)

	s := &ast.Schema{
		Types:         make(map[string]*ast.Definition),
		Directives:    make(map[string]*ast.DirectiveDefinition),
		Implements:    make(map[string][]*ast.Definition),
		PossibleTypes: make(map[string][]*ast.Definition),
	}
	for _, def := range doc.Definitions {
		s.Types[def.Name] = def
	}
	for _, dir := range doc.Directives {
		s.Directives[dir.Name] = dir
	}
	if q := s.Types["Query"]; q != nil {
		s.Query = q
	}
	if m := s.Types["Mutation"]; m != nil {
		s.Mutation = m
	}
	if sub := s.Types["Subscription"]; sub != nil {
		s.Subscription = sub
	}
	for _, def := range doc.Definitions {
		if def.Kind == ast.Object {
			for _, iface := range def.Interfaces {
				if ifaceDef := s.Types[iface]; ifaceDef != nil {
					s.AddImplements(def.Name, ifaceDef)
					s.AddPossibleType(iface, def)
				}
			}
		}
	}
	return static.New(s)
}

const builtinSDL = `
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

func TestParseQuery_SimpleQuery(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user(id: ID!): User }
		type User { id: ID!, name: String }
	`)

	op, err := schema.ParseQuery(context.Background(), p, `query { user(id: "1") { id name } }`, nil)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, ast.Query, op.Definition.Operation)
	assert.Len(t, op.Definition.SelectionSet, 1)
}

func TestParseQuery_WithVariables(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user(id: ID!): User }
		type User { id: ID!, name: String }
	`)

	vars := map[string]any{"id": "123"}
	op, err := schema.ParseQuery(context.Background(), p,
		`query GetUser($id: ID!) { user(id: $id) { id } }`, vars)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, vars, op.Variables)
}

func TestParseQuery_OperationName(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)

	op, err := schema.ParseQuery(context.Background(), p,
		`query A { user { id } } query B { user { id } }`, nil,
		schema.WithOperationName("B"))
	require.NoError(t, err)
	assert.Equal(t, "B", op.Definition.Name)
}

func TestParseQuery_InvalidQuery(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)

	_, err := schema.ParseQuery(context.Background(), p, `query {`, nil)
	require.Error(t, err)
}

func TestParseQuery_ValidationError(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)

	// unknownField doesn't exist on User
	_, err := schema.ParseQuery(context.Background(), p,
		`query { user { unknownField } }`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknownField")
}

func TestParseQuery_MultipleOperationsNoName(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)

	// Without operationName, returns the first operation
	op, err := schema.ParseQuery(context.Background(), p,
		`query A { user { id } } query B { user { id } }`, nil)
	require.NoError(t, err)
	assert.Equal(t, "A", op.Definition.Name)
}

func TestParseQuery_OperationNotFound(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)

	_, err := schema.ParseQuery(context.Background(), p,
		`query A { user { id } }`, nil,
		schema.WithOperationName("NonExistent"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistent")
}

func TestParseQuery_Fragments(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID!, name: String }
	`)

	op, err := schema.ParseQuery(context.Background(), p,
		`query { user { ...UserFields } } fragment UserFields on User { id name }`, nil)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Len(t, op.Fragments, 1)
	assert.Equal(t, "UserFields", op.Fragments[0].Name)
}

// --- helpers ---

type denyChecker struct {
	denied map[string]map[string]bool
}

func (c *denyChecker) CheckFieldAccess(typeName, fieldName string) error {
	if fields, ok := c.denied[typeName]; ok {
		if fields[fieldName] {
			return fmt.Errorf("access denied for %s.%s", typeName, fieldName)
		}
	}
	return nil
}
