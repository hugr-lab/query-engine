package static_test

import (
	"context"
	"slices"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func newTestSchema(t *testing.T, sdl string) *ast.Schema {
	t.Helper()
	doc, err := parser.ParseSchema(&ast.Source{Input: sdl})
	require.NoError(t, err)

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
	s.Description = "test schema"
	return s
}

func TestStaticProvider_ForName(t *testing.T) {
	s := newTestSchema(t, `
		type Query { user: User }
		type User { id: ID! }
	`)
	p := static.NewWithSchema(s)

	def := p.ForName(context.Background(), "User")
	require.NotNil(t, def)
	assert.Equal(t, "User", def.Name)

	assert.Nil(t, p.ForName(context.Background(), "Missing"))
}

func TestStaticProvider_DirectiveForName(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on FIELD
	`)
	p := static.NewWithSchema(s)

	dir := p.DirectiveForName(context.Background(), "cached")
	require.NotNil(t, dir)
	assert.Equal(t, "cached", dir.Name)

	assert.Nil(t, p.DirectiveForName(context.Background(), "unknown"))
}

func TestStaticProvider_QueryType(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.NewWithSchema(s)

	q := p.QueryType(context.Background())
	require.NotNil(t, q)
	assert.Equal(t, "Query", q.Name)
}

func TestStaticProvider_MutationType(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type Mutation { create: Boolean }
	`)
	p := static.NewWithSchema(s)

	m := p.MutationType(context.Background())
	require.NotNil(t, m)
	assert.Equal(t, "Mutation", m.Name)
}

func TestStaticProvider_SubscriptionType(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type Subscription { event: String }
	`)
	p := static.NewWithSchema(s)

	sub := p.SubscriptionType(context.Background())
	require.NotNil(t, sub)
	assert.Equal(t, "Subscription", sub.Name)
}

func TestStaticProvider_PossibleTypes(t *testing.T) {
	s := newTestSchema(t, `
		type Query { node: Node }
		interface Node { id: ID! }
		type User implements Node { id: ID!, name: String }
		type Post implements Node { id: ID!, title: String }
	`)
	p := static.NewWithSchema(s)

	nodeDef := p.ForName(context.Background(), "Node")
	require.NotNil(t, nodeDef)

	possibles := slices.Collect(p.PossibleTypes(context.Background(), nodeDef.Name))
	require.Len(t, possibles, 2)

	names := make(map[string]bool)
	for _, d := range possibles {
		names[d.Name] = true
	}
	assert.True(t, names["User"])
	assert.True(t, names["Post"])
}

func TestStaticProvider_Implements(t *testing.T) {
	s := newTestSchema(t, `
		type Query { user: User }
		interface Node { id: ID! }
		type User implements Node { id: ID!, name: String }
	`)
	p := static.NewWithSchema(s)

	userDef := p.ForName(context.Background(), "User")
	require.NotNil(t, userDef)

	impls := slices.Collect(p.Implements(context.Background(), userDef.Name))
	require.Len(t, impls, 1)
	assert.Equal(t, "Node", impls[0].Name)
}

func TestStaticProvider_Types(t *testing.T) {
	s := newTestSchema(t, `
		type Query { user: User }
		type User { id: ID! }
	`)
	p := static.NewWithSchema(s)

	var names []string
	for name := range p.Types(context.Background()) {
		names = append(names, name)
	}
	assert.Contains(t, names, "Query")
	assert.Contains(t, names, "User")
}

func TestStaticProvider_DirectiveDefinitions(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on FIELD
	`)
	p := static.NewWithSchema(s)

	var names []string
	for name := range p.DirectiveDefinitions(context.Background()) {
		names = append(names, name)
	}
	assert.Contains(t, names, "cached")
}

func TestStaticProvider_Description(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.NewWithSchema(s)
	assert.Equal(t, "test schema", p.Description(context.Background()))
}
