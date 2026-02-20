package schema_test

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/hugr-lab/query-engine/pkg/schema/validator/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryService_ParseQuery(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user(id: ID!): User }
		type User { id: ID!, name: String }
	`)

	svc := schema.NewService(p,
		schema.WithServiceValidator(validator.New(rules.DefaultRules()...)),
	)

	op, err := svc.ParseQuery(context.Background(), `query { user(id: "1") { id name } }`, nil, "")
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Len(t, op.Definition.SelectionSet, 1)
}

func TestQueryService_ParseQuery_WithOperationName(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)

	svc := schema.NewService(p)
	op, err := svc.ParseQuery(context.Background(),
		`query A { user { id } } query B { user { id } }`, nil, "A")
	require.NoError(t, err)
	assert.Equal(t, "A", op.Definition.Name)
}

func TestQueryService_SetProvider(t *testing.T) {
	p1 := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)
	p2 := newStaticProvider(t, `
		type Query { post: Post }
		type Post { id: ID!, title: String }
	`)

	svc := schema.NewService(p1)

	// Works with p1
	_, err := svc.ParseQuery(context.Background(), `query { user { id } }`, nil, "")
	require.NoError(t, err)

	// Switch to p2
	svc.SetProvider(p2)
	assert.Equal(t, p2, svc.Provider())

	// Works with p2
	_, err = svc.ParseQuery(context.Background(), `query { post { id title } }`, nil, "")
	require.NoError(t, err)

	// Old query fails with p2
	_, err = svc.ParseQuery(context.Background(), `query { user { id } }`, nil, "")
	require.Error(t, err)
}

func TestQueryService_NoValidator(t *testing.T) {
	p := newStaticProvider(t, `
		type Query { user: User }
		type User { id: ID! }
	`)

	// No validator set — should use default
	svc := schema.NewService(p)
	op, err := svc.ParseQuery(context.Background(), `query { user { id } }`, nil, "")
	require.NoError(t, err)
	require.NotNil(t, op)
}
