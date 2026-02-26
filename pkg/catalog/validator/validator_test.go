package validator_test

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/hugr-lab/query-engine/pkg/catalog/validator/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestValidator_InvalidQueries(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		errContain string
	}{
		{
			name: "FieldsOnCorrectType",
			schema: `
				type Query { user: User }
				type User { id: ID!, name: String }`,
			query:      `query { user { nonexistent } }`,
			errContain: "nonexistent",
		},
		{
			name: "ScalarLeafs/NoSelectionOnScalar",
			schema: `
				type Query { user: User }
				type User { id: ID!, name: String }`,
			query:      `query { user { id { sub } } }`,
			errContain: "must not have a selection",
		},
		{
			name: "ScalarLeafs/MissingSelectionOnComposite",
			schema: `
				type Query { user: User }
				type User { id: ID!, name: String }`,
			query:      `query { user }`,
			errContain: "must have a selection",
		},
		{
			name: "FragmentsOnCompositeTypes",
			schema: `
				type Query { user: User }
				type User { id: ID!, name: String }`,
			query:      `query { user { ... on String { x } } }`,
			errContain: "non composite type",
		},
		{
			name: "KnownDirectives",
			schema: `
				type Query { user: User }
				type User { id: ID! }`,
			query:      `query { user { id @unknown } }`,
			errContain: "Unknown directive",
		},
		{
			name: "KnownArgumentNames",
			schema: `
				type Query { user(id: ID!): User }
				type User { id: ID! }`,
			query:      `query { user(unknown: "1") { id } }`,
			errContain: `Unknown argument "unknown"`,
		},
		{
			name: "KnownTypeNames",
			schema: `
				type Query { user: User }
				type User { id: ID! }`,
			query:      `query { user { ... on NonExistent { id } } }`,
			errContain: `Unknown type "NonExistent"`,
		},
		{
			name:       "LoneAnonymousOperation",
			schema:     `type Query { a: String, b: String }`,
			query:      `query { a } query Named { b }`,
			errContain: "anonymous operation must be the only",
		},
		{
			name:       "UniqueOperationNames",
			schema:     `type Query { a: String }`,
			query:      `query Dup { a } query Dup { a }`,
			errContain: `only one operation named "Dup"`,
		},
		{
			name: "UniqueFragmentNames",
			schema: `
				type Query { a: A }
				type A { x: String }`,
			query: `
				query { a { ...F } }
				fragment F on A { x }
				fragment F on A { x }`,
			errContain: `only one fragment named "F"`,
		},
		{
			name: "UniqueArgumentNames",
			schema: `
				type Query { user(id: ID!): User }
				type User { id: ID! }`,
			query:      `query { user(id: "1", id: "2") { id } }`,
			errContain: `only one argument named "id"`,
		},
		{
			name: "UniqueVariableNames",
			schema: `
				type Query { user(id: ID!): User }
				type User { id: ID! }`,
			query:      `query($id: ID!, $id: String) { user(id: $id) { id } }`,
			errContain: `only one variable named "$id"`,
		},
		{
			name: "NoFragmentCycles",
			schema: `
				type Query { a: A }
				type A { x: String }`,
			query: `
				query { a { ...A } }
				fragment A on A { ...B }
				fragment B on A { ...A }`,
			errContain: "Cannot spread fragment",
		},
		{
			name: "NoUndefinedVariables",
			schema: `
				type Query { user(id: ID!): User }
				type User { id: ID! }`,
			query:      `query { user(id: $undeclared) { id } }`,
			errContain: `Variable "$undeclared" is not defined`,
		},
		{
			name: "NoUnusedFragments",
			schema: `
				type Query { a: A }
				type A { x: String }`,
			query: `
				query { a { x } }
				fragment Unused on A { x }`,
			errContain: `Fragment "Unused" is never used`,
		},
		{
			name: "PossibleFragmentSpreads",
			schema: `
				type Query { user: User }
				type User { id: ID! }
				type Post { title: String }`,
			query:      `query { user { ... on Post { title } } }`,
			errContain: "can never be of type",
		},
		{
			name: "ProvidedRequiredArguments",
			schema: `
				type Query { user(id: ID!): User }
				type User { id: ID! }`,
			query:      `query { user { id } }`,
			errContain: `argument "id" of type "ID!"`,
		},
		{
			name: "ValuesOfCorrectType",
			schema: `
				type Query { user(id: Int!): User }
				type User { id: ID! }`,
			query:      `query { user(id: "not_an_int") { id } }`,
			errContain: "Int cannot represent",
		},
		{
			name: "VariablesAreInputTypes",
			schema: `
				type Query { user: User }
				type User { id: ID! }`,
			query:      `query($u: User) { user { id } }`,
			errContain: "cannot be non-input type",
		},
		{
			name: "VariablesInAllowedPosition",
			schema: `
				type Query { user(id: ID!): User }
				type User { id: ID! }`,
			query:      `query($id: String) { user(id: $id) { id } }`,
			errContain: "used in position expecting",
		},
		{
			name: "SingleFieldSubscriptions",
			schema: `
				type Query { a: String }
				type Subscription { a: String, b: String }`,
			query:      `subscription { a b }`,
			errContain: "must select only one",
		},
		{
			name: "OverlappingFields/Conflict",
			schema: `
				type Query { a: A }
				type A { x: String, y: Int }`,
			query:      `query { a { f: x f: y } }`,
			errContain: "conflict",
		},
		{
			name: "DirectiveAtWrongLocation",
			schema: `
				type Query { user: User }
				type User { id: ID! }
				directive @deprecated(reason: String) on FIELD_DEFINITION`,
			query:      `query @deprecated { user { id } }`,
			errContain: "may not be used on",
		},
		{
			name: "EnumValues/InvalidEnum",
			schema: `
				enum Status { ACTIVE INACTIVE }
				type Query { users(status: Status): [User] }
				type User { id: ID! }`,
			query:      `query { users(status: UNKNOWN) { id } }`,
			errContain: "does not exist",
		},
		{
			name: "InputObject/MissingRequiredField",
			schema: `
				input CreateInput { name: String!, email: String }
				type Query { create(input: CreateInput!): Boolean }`,
			query:      `query { create(input: {email: "test@test.com"}) }`,
			errContain: "required type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProvider(t, tt.schema)
			doc := parseTestQuery(t, tt.query)

			v := validator.New(rules.DefaultRules()...)
			errs := v.Validate(context.Background(), p, doc)
			require.NotEmpty(t, errs, "expected validation error")
			assert.Contains(t, errs[0].Message, tt.errContain)
		})
	}
}

func TestValidator_ValidQueries(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		query  string
	}{
		{
			name: "SimpleQuery",
			schema: `
				type Query { user(id: ID!): User }
				type User { id: ID!, name: String, posts: [Post] }
				type Post { id: ID!, title: String }`,
			query: `
				query GetUser($id: ID!) {
					user(id: $id) {
						id
						name
						posts { id title }
					}
				}`,
		},
		{
			name: "OverlappingFields/DifferentAliases",
			schema: `
				type Query { a: A }
				type A { x: String, y: Int }`,
			query: `query { a { x: x y: x } }`,
		},
		{
			name: "WithInterface",
			schema: `
				type Query { node: Node }
				interface Node { id: ID! }
				type User implements Node { id: ID!, name: String }
				type Post implements Node { id: ID!, title: String }`,
			query: `
				query {
					node {
						id
						... on User { name }
						... on Post { title }
					}
				}`,
		},
		{
			name: "EnumValues/ValidEnum",
			schema: `
				enum Status { ACTIVE INACTIVE }
				type Query { users(status: Status): [User] }
				type User { id: ID! }`,
			query: `query { users(status: ACTIVE) { id } }`,
		},
		{
			name: "InputObject/ValidInput",
			schema: `
				input CreateInput { name: String!, email: String }
				type Query { create(input: CreateInput!): Boolean }`,
			query: `query { create(input: {name: "test"}) }`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProvider(t, tt.schema)
			doc := parseTestQuery(t, tt.query)

			v := validator.New(rules.DefaultRules()...)
			errs := v.Validate(context.Background(), p, doc)
			assert.Empty(t, errs)
		})
	}
}

// Ensure testProvider satisfies TypeResolver at compile time.
var _ validator.TypeResolver = (*testProvider)(nil)

func Test_Complex(t *testing.T) {
	sdl := `
		enum Role { ADMIN EDITOR VIEWER }
		enum PostStatus { DRAFT PUBLISHED ARCHIVED }

		input AddressInput {
			street: String!
			city: String!
			geo: GeoInput
		}
		input GeoInput {
			lat: Float!
			lon: Float!
		}
		input PostFilter {
			statuses: [PostStatus!]
			tags: [String!]
			author: AuthorFilter
		}
		input AuthorFilter {
			nameContains: String
			roles: [Role!]
		}

		type Query {
			user(id: ID!): User
			search(filter: PostFilter, limit: Int = 10): [Post]
		}

		type Mutation {
			createUser(name: String!, address: AddressInput!): User
		}

		interface Node { id: ID! }

		type User implements Node {
			id: ID!
			name: String!
			role: Role!
			address: Address
			posts(status: PostStatus, limit: Int): [Post]
		}

		type Address {
			street: String!
			city: String!
			geo: GeoPoint
		}
		type GeoPoint {
			lat: Float!
			lon: Float!
			label: String
		}

		type Post implements Node {
			id: ID!
			title: String!
			status: PostStatus!
			tags: [Tag]
			comments(first: Int): [Comment]
		}
		type Tag { name: String!, posts: [Post] }

		type Comment implements Node {
			id: ID!
			body: String!
			author: User
		}
	`

	query := `
		query GetUserPosts(
			$userId: ID!,
			$postStatus: PostStatus,
			$commentLimit: Int,
			$searchFilter: PostFilter
		) {
			user(id: $userId) {
				...UserBasic
				posts(status: $postStatus, limit: 5) {
					...PostFields
					comments(first: $commentLimit) {
						id
						body
						author {
							...UserBasic
						}
					}
				}
			}
			search(filter: $searchFilter, limit: 3) {
				... on Node { id }
				... on Post {
					title
					status
					tags { name }
				}
			}
		}

		fragment UserBasic on User {
			id
			name
			role
			address {
				street
				city
				geo { lat lon label }
			}
		}

		fragment PostFields on Post {
			id
			title
			status
			tags { name }
		}
	`

	p := newTestProvider(t, sdl)
	doc := parseTestQuery(t, query)

	v := validator.New(rules.DefaultRules()...)
	errs := v.Validate(context.Background(), p, doc)
	require.Empty(t, errs)

	// Verify enrichment on deeply nested fields (4 levels: user → posts → comments → author)
	op := doc.Operations[0]
	userField := op.SelectionSet[0].(*ast.Field)
	assert.Equal(t, "user", userField.Name)
	assert.NotNil(t, userField.Definition)
	assert.Equal(t, "Query", userField.ObjectDefinition.Name)

	// user → posts (via fragment spread + direct selection)
	// SelectionSet: [FragmentSpread(UserBasic), Field(posts)]
	postsField := userField.SelectionSet[1].(*ast.Field)
	assert.Equal(t, "posts", postsField.Name)
	assert.Equal(t, "User", postsField.ObjectDefinition.Name)

	// posts → comments
	commentsField := postsField.SelectionSet[1].(*ast.Field)
	assert.Equal(t, "comments", commentsField.Name)
	assert.Equal(t, "Post", commentsField.ObjectDefinition.Name)
	require.Len(t, commentsField.Arguments, 1)
	assert.NotNil(t, commentsField.Arguments[0].Value.VariableDefinition, "variable $commentLimit should be linked")

	// comments → author (level 4)
	authorField := commentsField.SelectionSet[2].(*ast.Field)
	assert.Equal(t, "author", authorField.Name)
	assert.Equal(t, "Comment", authorField.ObjectDefinition.Name)

	// Verify fragment enrichment
	require.Len(t, doc.Fragments, 2)
	for _, frag := range doc.Fragments {
		assert.NotNil(t, frag.Definition, "fragment %s should have Definition set", frag.Name)
	}

	// Verify variable definitions are all marked Used
	for _, varDef := range op.VariableDefinitions {
		assert.True(t, varDef.Used, "variable $%s should be marked used", varDef.Variable)
		assert.NotNil(t, varDef.Definition, "variable $%s should have Definition set", varDef.Variable)
	}

	// Verify search field has inline fragments enriched
	searchField := op.SelectionSet[1].(*ast.Field)
	assert.Equal(t, "search", searchField.Name)
	// ... on Node { id }
	nodeInline := searchField.SelectionSet[0].(*ast.InlineFragment)
	assert.Equal(t, "Node", nodeInline.TypeCondition)
	assert.NotNil(t, nodeInline.ObjectDefinition)
	// ... on Post { title status tags { name } }
	postInline := searchField.SelectionSet[1].(*ast.InlineFragment)
	assert.Equal(t, "Post", postInline.TypeCondition)
	assert.NotNil(t, postInline.ObjectDefinition)

	// Verify search filter argument links to $searchFilter variable
	searchFilterArg := searchField.Arguments[0]
	assert.Equal(t, "filter", searchFilterArg.Name)
	assert.NotNil(t, searchFilterArg.Value.VariableDefinition)

	// --- Mutation with nested input types ---
	mutQuery := `
		mutation CreateUser($addr: AddressInput!) {
			createUser(name: "Alice", address: $addr) {
				id
				name
				address { street city geo { lat lon } }
			}
		}
	`
	mutDoc := parseTestQuery(t, mutQuery)
	errs = v.Validate(context.Background(), p, mutDoc)
	require.Empty(t, errs)

	mutOp := mutDoc.Operations[0]
	assert.Equal(t, ast.Mutation, mutOp.Operation)

	createField := mutOp.SelectionSet[0].(*ast.Field)
	assert.Equal(t, "createUser", createField.Name)
	assert.Equal(t, "Mutation", createField.ObjectDefinition.Name)
	require.Len(t, createField.Arguments, 2)

	// "name" arg should have ExpectedType = String!
	nameArg := createField.Arguments[0]
	assert.Equal(t, "name", nameArg.Name)
	assert.NotNil(t, nameArg.Value.ExpectedType)
	assert.Equal(t, "String", nameArg.Value.ExpectedType.Name())

	// "address" arg should link to $addr variable
	addrArg := createField.Arguments[1]
	assert.Equal(t, "address", addrArg.Name)
	assert.NotNil(t, addrArg.Value.VariableDefinition)
}
