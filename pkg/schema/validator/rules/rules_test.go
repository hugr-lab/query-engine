package rules_test

import (
	"context"
	"iter"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/hugr-lab/query-engine/pkg/schema/validator/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
)

// ---------------------------------------------------------------------------
// Inline rules
// ---------------------------------------------------------------------------

func TestFieldsOnCorrectType(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "valid field",
			schema:  `type Query { user: User } type User { id: ID!, name: String }`,
			query:   `query { user { id name } }`,
			wantErr: false,
		},
		{
			name:    "__typename always valid",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { __typename } }`,
			wantErr: false,
		},
		{
			name:       "unknown field",
			schema:     `type Query { user: User } type User { id: ID! }`,
			query:      `query { user { nonexistent } }`,
			wantErr:    true,
			errContain: `Cannot query field "nonexistent"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithInlineRule(&rules.FieldsOnCorrectType{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestScalarLeafs(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "scalar without selection",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { id } }`,
			wantErr: false,
		},
		{
			name:    "composite with selection",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { id } }`,
			wantErr: false,
		},
		{
			name:       "scalar with selection",
			schema:     `type Query { user: User } type User { id: ID! }`,
			query:      `query { user { id { sub } } }`,
			wantErr:    true,
			errContain: "must not have a selection",
		},
		{
			name:       "composite without selection",
			schema:     `type Query { user: User } type User { id: ID! }`,
			query:      `query { user }`,
			wantErr:    true,
			errContain: "must have a selection",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithInlineRule(&rules.ScalarLeafs{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestFragmentsOnCompositeTypes(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "inline fragment on object",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { ... on User { id } } }`,
			wantErr: false,
		},
		{
			name:    "fragment spread on object",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { ...F } } fragment F on User { id }`,
			wantErr: false,
		},
		{
			name:       "inline fragment on scalar",
			schema:     `type Query { user: User } type User { id: ID!, name: String }`,
			query:      `query { user { ... on String { x } } }`,
			wantErr:    true,
			errContain: "non composite type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithInlineRule(&rules.FragmentsOnCompositeTypes{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestKnownDirectives(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "known directive at correct location",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { id @skip(if: true) } }`,
			wantErr: false,
		},
		{
			name:       "unknown directive",
			schema:     `type Query { user: User } type User { id: ID! }`,
			query:      `query { user { id @unknown } }`,
			wantErr:    true,
			errContain: `Unknown directive "@unknown"`,
		},
		{
			name: "directive at wrong location",
			schema: `type Query { user: User } type User { id: ID! }
				directive @deprecated(reason: String) on FIELD_DEFINITION`,
			query:      `query @deprecated { user { id } }`,
			wantErr:    true,
			errContain: "may not be used on",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithInlineRule(&rules.KnownDirectives{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestKnownArgumentNames(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "known argument",
			schema:  `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:   `query { user(id: "1") { id } }`,
			wantErr: false,
		},
		{
			name:       "unknown argument",
			schema:     `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:      `query { user(unknown: "1") { id } }`,
			wantErr:    true,
			errContain: `Unknown argument "unknown"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithInlineRule(&rules.KnownArgumentNames{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestKnownTypeNames(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "known type in inline fragment",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { ... on User { id } } }`,
			wantErr: false,
		},
		{
			name:       "unknown type in inline fragment",
			schema:     `type Query { user: User } type User { id: ID! }`,
			query:      `query { user { ... on NonExistent { id } } }`,
			wantErr:    true,
			errContain: `Unknown type "NonExistent"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithInlineRule(&rules.KnownTypeNames{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

// ---------------------------------------------------------------------------
// Post rules
// ---------------------------------------------------------------------------

func TestLoneAnonymousOperation(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "single anonymous",
			schema:  `type Query { a: String }`,
			query:   `query { a }`,
			wantErr: false,
		},
		{
			name:    "single named",
			schema:  `type Query { a: String }`,
			query:   `query Foo { a }`,
			wantErr: false,
		},
		{
			name:    "multiple named",
			schema:  `type Query { a: String, b: String }`,
			query:   `query A { a } query B { b }`,
			wantErr: false,
		},
		{
			name:       "anonymous with named",
			schema:     `type Query { a: String, b: String }`,
			query:      `query { a } query Named { b }`,
			wantErr:    true,
			errContain: "anonymous operation must be the only",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.LoneAnonymousOperation{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestUniqueOperationNames(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "unique names",
			schema:  `type Query { a: String }`,
			query:   `query A { a } query B { a }`,
			wantErr: false,
		},
		{
			name:       "duplicate names",
			schema:     `type Query { a: String }`,
			query:      `query Dup { a } query Dup { a }`,
			wantErr:    true,
			errContain: `only one operation named "Dup"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.UniqueOperationNames{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestUniqueFragmentNames(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "unique fragments",
			schema:  `type Query { a: A } type A { x: String }`,
			query:   `query { a { ...F1 ...F2 } } fragment F1 on A { x } fragment F2 on A { x }`,
			wantErr: false,
		},
		{
			name:       "duplicate fragments",
			schema:     `type Query { a: A } type A { x: String }`,
			query:      `query { a { ...F } } fragment F on A { x } fragment F on A { x }`,
			wantErr:    true,
			errContain: `only one fragment named "F"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.UniqueFragmentNames{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestUniqueArgumentNames(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "unique arguments",
			schema:  `type Query { user(id: ID!, name: String): User } type User { id: ID! }`,
			query:   `query { user(id: "1", name: "a") { id } }`,
			wantErr: false,
		},
		{
			name:       "duplicate arguments",
			schema:     `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:      `query { user(id: "1", id: "2") { id } }`,
			wantErr:    true,
			errContain: `only one argument named "id"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.UniqueArgumentNames{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestUniqueInputFieldNames(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "unique input fields",
			schema:  `input I { a: String, b: String } type Query { f(i: I): Boolean }`,
			query:   `query { f(i: {a: "x", b: "y"}) }`,
			wantErr: false,
		},
		{
			name:       "duplicate input fields",
			schema:     `input I { a: String } type Query { f(i: I): Boolean }`,
			query:      `query { f(i: {a: "x", a: "y"}) }`,
			wantErr:    true,
			errContain: `only one input field named "a"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.UniqueInputFieldNames{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestUniqueVariableNames(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "unique variables",
			schema:  `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:   `query($id: ID!) { user(id: $id) { id } }`,
			wantErr: false,
		},
		{
			name:       "duplicate variables",
			schema:     `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:      `query($id: ID!, $id: String) { user(id: $id) { id } }`,
			wantErr:    true,
			errContain: `only one variable named "$id"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.UniqueVariableNames{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestUniqueDirectivesPerLocation(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "single directive",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { id @skip(if: true) } }`,
			wantErr: false,
		},
		{
			name:       "duplicate non-repeatable directive",
			schema:     `type Query { user: User } type User { id: ID! }`,
			query:      `query { user { id @skip(if: true) @skip(if: false) } }`,
			wantErr:    true,
			errContain: `can only be used once`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.UniqueDirectivesPerLocation{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestNoFragmentCycles(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "no cycle",
			schema:  `type Query { a: A } type A { x: String }`,
			query:   `query { a { ...F } } fragment F on A { x }`,
			wantErr: false,
		},
		{
			name:   "direct cycle",
			schema: `type Query { a: A } type A { x: String }`,
			query: `query { a { ...A } }
				fragment A on A { ...B }
				fragment B on A { ...A }`,
			wantErr:    true,
			errContain: "Cannot spread fragment",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.NoFragmentCycles{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestNoUndefinedVariables(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "defined variable",
			schema:  `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:   `query($id: ID!) { user(id: $id) { id } }`,
			wantErr: false,
		},
		{
			name:       "undefined variable",
			schema:     `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:      `query { user(id: $undeclared) { id } }`,
			wantErr:    true,
			errContain: `Variable "$undeclared" is not defined`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.NoUndefinedVariables{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestNoUnusedFragments(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "used fragment",
			schema:  `type Query { a: A } type A { x: String }`,
			query:   `query { a { ...F } } fragment F on A { x }`,
			wantErr: false,
		},
		{
			name:       "unused fragment",
			schema:     `type Query { a: A } type A { x: String }`,
			query:      `query { a { x } } fragment Unused on A { x }`,
			wantErr:    true,
			errContain: `Fragment "Unused" is never used`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.NoUnusedFragments{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestPossibleFragmentSpreads(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name: "valid spread on interface implementor",
			schema: `type Query { node: Node }
				interface Node { id: ID! }
				type User implements Node { id: ID!, name: String }`,
			query:   `query { node { ... on User { name } } }`,
			wantErr: false,
		},
		{
			name: "impossible spread",
			schema: `type Query { user: User }
				type User { id: ID! }
				type Post { title: String }`,
			query:      `query { user { ... on Post { title } } }`,
			wantErr:    true,
			errContain: "can never be of type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.PossibleFragmentSpreads{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestProvidedRequiredArguments(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "required arg provided",
			schema:  `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:   `query { user(id: "1") { id } }`,
			wantErr: false,
		},
		{
			name:    "optional arg omitted",
			schema:  `type Query { users(limit: Int = 10): [User] } type User { id: ID! }`,
			query:   `query { users { id } }`,
			wantErr: false,
		},
		{
			name:       "required arg missing",
			schema:     `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:      `query { user { id } }`,
			wantErr:    true,
			errContain: `argument "id" of type "ID!"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.ProvidedRequiredArguments{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestValuesOfCorrectType(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "correct string value",
			schema:  `type Query { user(name: String!): User } type User { id: ID! }`,
			query:   `query { user(name: "Alice") { id } }`,
			wantErr: false,
		},
		{
			name:    "correct int value",
			schema:  `type Query { user(id: Int!): User } type User { id: ID! }`,
			query:   `query { user(id: 42) { id } }`,
			wantErr: false,
		},
		{
			name:       "string where int expected",
			schema:     `type Query { user(id: Int!): User } type User { id: ID! }`,
			query:      `query { user(id: "not_an_int") { id } }`,
			wantErr:    true,
			errContain: "Int cannot represent",
		},
		{
			name:    "valid enum value",
			schema:  `enum Status { ACTIVE INACTIVE } type Query { users(s: Status): [User] } type User { id: ID! }`,
			query:   `query { users(s: ACTIVE) { id } }`,
			wantErr: false,
		},
		{
			name:       "invalid enum value",
			schema:     `enum Status { ACTIVE INACTIVE } type Query { users(s: Status): [User] } type User { id: ID! }`,
			query:      `query { users(s: UNKNOWN) { id } }`,
			wantErr:    true,
			errContain: "does not exist",
		},
		{
			name:       "missing required input field",
			schema:     `input I { name: String!, email: String } type Query { f(i: I!): Boolean }`,
			query:      `query { f(i: {email: "a@b.c"}) }`,
			wantErr:    true,
			errContain: "required type",
		},
		{
			name:    "valid input object",
			schema:  `input I { name: String!, email: String } type Query { f(i: I!): Boolean }`,
			query:   `query { f(i: {name: "test"}) }`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.ValuesOfCorrectType{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestVariablesAreInputTypes(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "input type variable",
			schema:  `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:   `query($id: ID!) { user(id: $id) { id } }`,
			wantErr: false,
		},
		{
			name:       "output type variable",
			schema:     `type Query { user: User } type User { id: ID! }`,
			query:      `query($u: User) { user { id } }`,
			wantErr:    true,
			errContain: "cannot be non-input type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.VariablesAreInputTypes{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestVariablesInAllowedPosition(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "compatible variable type",
			schema:  `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:   `query($id: ID!) { user(id: $id) { id } }`,
			wantErr: false,
		},
		{
			name:       "incompatible variable type",
			schema:     `type Query { user(id: ID!): User } type User { id: ID! }`,
			query:      `query($id: String) { user(id: $id) { id } }`,
			wantErr:    true,
			errContain: "used in position expecting",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.VariablesInAllowedPosition{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestOverlappingFieldsCanBeMerged(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "same field, same alias",
			schema:  `type Query { a: A } type A { x: String }`,
			query:   `query { a { x x } }`,
			wantErr: false,
		},
		{
			name:    "different aliases",
			schema:  `type Query { a: A } type A { x: String }`,
			query:   `query { a { x: x y: x } }`,
			wantErr: false,
		},
		{
			name:       "conflicting aliases different fields",
			schema:     `type Query { a: A } type A { x: String, y: Int }`,
			query:      `query { a { f: x f: y } }`,
			wantErr:    true,
			errContain: "conflict",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.OverlappingFieldsCanBeMerged{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestSingleFieldSubscriptions(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "single field",
			schema:  `type Query { a: String } type Subscription { a: String, b: String }`,
			query:   `subscription { a }`,
			wantErr: false,
		},
		{
			name:       "multiple fields",
			schema:     `type Query { a: String } type Subscription { a: String, b: String }`,
			query:      `subscription { a b }`,
			wantErr:    true,
			errContain: "must select only one",
		},
		{
			name:    "query with multiple fields is ok",
			schema:  `type Query { a: String, b: String }`,
			query:   `query { a b }`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.SingleFieldSubscriptions{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

func TestMaxIntrospectionDepth(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		query      string
		wantErr    bool
		errContain string
	}{
		{
			name:    "shallow query",
			schema:  `type Query { user: User } type User { id: ID! }`,
			query:   `query { user { id } }`,
			wantErr: false,
		},
		{
			name:    "depth 20 is ok",
			schema:  deepSchema(20),
			query:   deepQuery(20),
			wantErr: false,
		},
		{
			name:       "depth 21 is too deep",
			schema:     deepSchema(21),
			query:      deepQuery(21),
			wantErr:    true,
			errContain: "exceeds the limit",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(t, tt.schema, tt.query, validator.WithPostRule(&rules.MaxIntrospectionDepth{}))
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

// ---------------------------------------------------------------------------
// Permission rule (from perm package)
// ---------------------------------------------------------------------------

func TestPermissionFieldRule(t *testing.T) {
	schema := `type Query { user: User, post: Post }
		type User { id: ID!, name: String, secret: String }
		type Post { id: ID!, title: String }`

	tests := []struct {
		name       string
		query      string
		perms      *perm.RolePermissions
		wantErr    bool
		errContain string
	}{
		{
			name:    "no permissions in context",
			query:   `query { user { id name secret } }`,
			perms:   nil,
			wantErr: false,
		},
		{
			name:  "all allowed",
			query: `query { user { id name } }`,
			perms: &perm.RolePermissions{
				Name: "admin",
				Permissions: []perm.Permission{
					{Object: "*", Field: "*"},
				},
			},
			wantErr: false,
		},
		{
			name:  "field denied",
			query: `query { user { id secret } }`,
			perms: &perm.RolePermissions{
				Name: "reader",
				Permissions: []perm.Permission{
					{Object: "*", Field: "*"},
					{Object: "User", Field: "secret", Disabled: true},
				},
			},
			wantErr:    true,
			errContain: auth.ErrForbidden.Error(),
		},
		{
			name:  "disabled role",
			query: `query { user { id } }`,
			perms: &perm.RolePermissions{
				Name:     "disabled",
				Disabled: true,
			},
			wantErr:    true,
			errContain: auth.ErrForbidden.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProvider(t, schema)
			doc := parseTestQuery(t, tt.query)

			ctx := context.Background()
			if tt.perms != nil {
				ctx = perm.CtxWithPerm(ctx, tt.perms)
			}

			v := validator.New(validator.WithInlineRule(&perm.PermissionFieldRule{}))
			errs := v.Validate(ctx, p, doc)
			checkErrors(t, errs, tt.wantErr, tt.errContain)
		})
	}
}

// ---------------------------------------------------------------------------
// DefaultRules integration
// ---------------------------------------------------------------------------

func TestDefaultRules_AllRulesApplied(t *testing.T) {
	p := newTestProvider(t, `
		type Query { user(id: ID!): User }
		type User { id: ID!, name: String }
	`)

	t.Run("valid query passes", func(t *testing.T) {
		doc := parseTestQuery(t, `query { user(id: "1") { id name } }`)
		v := validator.New(rules.DefaultRules()...)
		errs := v.Validate(context.Background(), p, doc)
		assert.Empty(t, errs)
	})

	t.Run("unknown field fails", func(t *testing.T) {
		doc := parseTestQuery(t, `query { user(id: "1") { nonexistent } }`)
		v := validator.New(rules.DefaultRules()...)
		errs := v.Validate(context.Background(), p, doc)
		require.NotEmpty(t, errs)
		assert.Contains(t, errs[0].Message, "nonexistent")
	})
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func validate(t *testing.T, sdl, query string, opts ...validator.Option) gqlerror.List {
	t.Helper()
	p := newTestProvider(t, sdl)
	doc := parseTestQuery(t, query)
	v := validator.New(opts...)
	return v.Validate(context.Background(), p, doc)
}

func checkErrors(t *testing.T, errs gqlerror.List, wantErr bool, errContain string) {
	t.Helper()
	if wantErr {
		require.NotEmpty(t, errs, "expected validation error")
		if errContain != "" {
			assert.Contains(t, errs[0].Message, errContain)
		}
	} else {
		assert.Empty(t, errs)
	}
}

func parseTestQuery(t *testing.T, query string) *ast.QueryDocument {
	t.Helper()
	doc, err := parser.ParseQuery(&ast.Source{Input: query})
	require.NoError(t, err)
	return doc
}

type testProvider struct {
	schema *ast.Schema
}

func newTestProvider(t *testing.T, sdl string) *testProvider {
	t.Helper()
	src := &ast.Source{Input: sdl, BuiltIn: false}
	doc, parseErr := parser.ParseSchema(src)
	require.NoError(t, parseErr)

	builtinSrc := &ast.Source{Input: builtinSchema, BuiltIn: true}
	builtinDoc, err := parser.ParseSchema(builtinSrc)
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
	return &testProvider{schema: s}
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

func (p *testProvider) PossibleTypes(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.schema.Types[name]
	if def == nil {
		return nil
	}
	return func(yield func(*ast.Definition) bool) {
		for _, t := range p.schema.PossibleTypes[def.Name] {
			if !yield(t) {
				return
			}
		}
	}
}

func (p *testProvider) Implements(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.schema.Types[name]
	if def == nil {
		return nil
	}
	return func(yield func(*ast.Definition) bool) {
		for _, iface := range p.schema.Implements[def.Name] {
			if !yield(iface) {
				return
			}
		}
	}
}

var _ validator.TypeResolver = (*testProvider)(nil)

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

// deepSchema generates a schema with n levels of nesting: L0 { child: L1 }, L1 { child: L2 }, ...
func deepSchema(n int) string {
	var b []byte
	b = append(b, "type Query { root: L0 }\n"...)
	for i := 0; i < n-1; i++ {
		b = append(b, []byte("type L"+itoa(i)+" { child: L"+itoa(i+1)+" }\n")...)
	}
	b = append(b, []byte("type L"+itoa(n-1)+" { value: String }\n")...)
	return string(b)
}

// deepQuery generates a query nesting n levels: { root { child { child { ... { value } } } } }
func deepQuery(n int) string {
	var b []byte
	b = append(b, "query { root "...)
	for i := 0; i < n-1; i++ {
		b = append(b, "{ child "...)
	}
	b = append(b, "{ value }"...)
	for i := 0; i < n-1; i++ {
		b = append(b, " }"...)
	}
	b = append(b, " }"...)
	return string(b)
}

func itoa(i int) string {
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}
