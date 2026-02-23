package static_test

import (
	"context"
	"slices"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestDocProvider_ForName(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { user: User }
		type User { id: ID!, name: String }
	`)
	p := static.NewDocumentProvider(doc)

	def := p.ForName(context.Background(), "User")
	require.NotNil(t, def)
	assert.Equal(t, "User", def.Name)
	assert.Equal(t, ast.Object, def.Kind)

	assert.Nil(t, p.ForName(context.Background(), "Missing"))
}

func TestDocProvider_DirectiveForName(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on FIELD
	`)
	p := static.NewDocumentProvider(doc)

	dir := p.DirectiveForName(context.Background(), "cached")
	require.NotNil(t, dir)
	assert.Equal(t, "cached", dir.Name)

	assert.Nil(t, p.DirectiveForName(context.Background(), "unknown"))
}

func TestDocProvider_QueryType(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	p := static.NewDocumentProvider(doc)

	q := p.QueryType(context.Background())
	require.NotNil(t, q)
	assert.Equal(t, "Query", q.Name)
}

func TestDocProvider_MutationType(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		type Mutation { createUser(name: String!): User }
		type User { id: ID! }
	`)
	p := static.NewDocumentProvider(doc)

	m := p.MutationType(context.Background())
	require.NotNil(t, m)
	assert.Equal(t, "Mutation", m.Name)
}

func TestDocProvider_SubscriptionType(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		type Subscription { event: String }
	`)
	p := static.NewDocumentProvider(doc)

	s := p.SubscriptionType(context.Background())
	require.NotNil(t, s)
	assert.Equal(t, "Subscription", s.Name)
}

func TestDocProvider_PossibleTypes_Interface(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { node: Node }
		interface Node { id: ID! }
		type User implements Node { id: ID!, name: String }
		type Post implements Node { id: ID!, title: String }
	`)
	p := static.NewDocumentProvider(doc)

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

func TestDocProvider_PossibleTypes_Union(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { result: SearchResult }
		union SearchResult = User | Post
		type User { id: ID! }
		type Post { id: ID! }
	`)
	p := static.NewDocumentProvider(doc)

	unionDef := p.ForName(context.Background(), "SearchResult")
	require.NotNil(t, unionDef)

	possibles := slices.Collect(p.PossibleTypes(context.Background(), unionDef.Name))
	require.Len(t, possibles, 2)

	names := make(map[string]bool)
	for _, d := range possibles {
		names[d.Name] = true
	}
	assert.True(t, names["User"])
	assert.True(t, names["Post"])
}

func TestDocProvider_PossibleTypes_Object(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	p := static.NewDocumentProvider(doc)

	qDef := p.ForName(context.Background(), "Query")
	possibles := slices.Collect(p.PossibleTypes(context.Background(), qDef.Name))
	require.Len(t, possibles, 1)
	assert.Equal(t, "Query", possibles[0].Name)
}

func TestDocProvider_Implements(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { user: User }
		interface Node { id: ID! }
		type User implements Node { id: ID!, name: String }
	`)
	p := static.NewDocumentProvider(doc)

	userDef := p.ForName(context.Background(), "User")
	require.NotNil(t, userDef)

	impls := slices.Collect(p.Implements(context.Background(), userDef.Name))
	require.Len(t, impls, 1)
	assert.Equal(t, "Node", impls[0].Name)
}

func TestDocProvider_Types_Iterator(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { user: User }
		type User { id: ID! }
	`)
	p := static.NewDocumentProvider(doc)

	var names []string
	for name := range p.Types(context.Background()) {
		names = append(names, name)
	}
	assert.Contains(t, names, "Query")
	assert.Contains(t, names, "User")
}

func TestDocProvider_DirectiveDefinitions_Iterator(t *testing.T) {
	doc := parseSchemaDoc(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on FIELD
	`)
	p := static.NewDocumentProvider(doc)

	var names []string
	for name := range p.DirectiveDefinitions(context.Background()) {
		names = append(names, name)
	}
	assert.Contains(t, names, "cached")
}

func TestDocProvider_Description(t *testing.T) {
	doc := parseSchemaDoc(t, `type Query { id: ID }`)
	p := static.NewDocumentProvider(doc)
	assert.Equal(t, "", p.Description(context.Background()))
}
