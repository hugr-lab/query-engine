package static_test

import (
	"context"
	"slices"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func newDDL(t *testing.T, sdl string) *ast.SchemaDocument {
	t.Helper()
	doc, err := parser.ParseSchema(&ast.Source{Input: sdl})
	require.NoError(t, err)
	return doc
}

// --- Definition operations ---

func TestUpdate_AddDefinition(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type User { id: ID!, name: String! }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.Equal(t, "User", user.Name)
	assert.NotNil(t, user.Fields.ForName("id"))
	assert.NotNil(t, user.Fields.ForName("name"))
}

func TestUpdate_AddDefinition_AlreadyExists(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type Query { id: ID }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestUpdate_DropDefinition(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type User @drop { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	assert.Nil(t, p.ForName(ctx, "User"))
	assert.NotNil(t, p.ForName(ctx, "Query"))
}

func TestUpdate_DropDefinition_NotFound(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type Missing @drop { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.ErrorIs(t, err, base.ErrDefinitionNotFound)
}

func TestUpdate_DropDefinition_IfExists(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type Missing @drop(if_exists: true) { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
}

func TestUpdate_ReplaceDefinition(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type User @replace { id: ID!, name: String!, email: String }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.NotNil(t, user.Fields.ForName("name"))
	assert.NotNil(t, user.Fields.ForName("email"))
	// @replace should be stripped
	assert.Nil(t, user.Directives.ForName("replace"))
}

func TestUpdate_ReplaceDefinition_NotFound(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type Missing @replace { id: ID! }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.ErrorIs(t, err, base.ErrDefinitionNotFound)
}

func TestUpdate_IfNotExists_TypeExists(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type User @if_not_exists { id: ID!, name: String! }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	// should keep original - no "name" field
	assert.Nil(t, user.Fields.ForName("name"))
}

func TestUpdate_IfNotExists_TypeNew(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type User @if_not_exists { id: ID!, name: String! }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.NotNil(t, user.Fields.ForName("name"))
	// @if_not_exists should be stripped
	assert.Nil(t, user.Directives.ForName("if_not_exists"))
}

func TestUpdate_DropAndReaddSameName(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	// Drop User then add new User in same DDL
	ddl := newDDL(t, `
		type User @drop { _: Boolean }
	`)
	src := static.NewDocumentProvider(ddl)
	require.NoError(t, p.Update(ctx, src))
	assert.Nil(t, p.ForName(ctx, "User"))

	ddl2 := newDDL(t, `type User { id: ID!, name: String!, email: String }`)
	src2 := static.NewDocumentProvider(ddl2)
	require.NoError(t, p.Update(ctx, src2))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.NotNil(t, user.Fields.ForName("email"))
}

// --- Extension operations ---

func TestUpdate_ExtendAddField(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { name: String! }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.NotNil(t, user.Fields.ForName("name"))
	assert.NotNil(t, user.Fields.ForName("id"))
}

func TestUpdate_ExtendDropField(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID!, name: String!, email: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { email: String @drop }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.Nil(t, user.Fields.ForName("email"))
	assert.NotNil(t, user.Fields.ForName("name"))
}

func TestUpdate_ExtendDropField_NotFound(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { missing: String @drop }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field missing")
}

func TestUpdate_ExtendReplaceField(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID!, name: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { name: String! @replace }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	field := user.Fields.ForName("name")
	require.NotNil(t, field)
	assert.True(t, field.Type.NonNull, "field should now be non-null")
	assert.Nil(t, field.Directives.ForName("replace"))
}

func TestUpdate_ExtendUpdateDescription(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	// gqlparser doesn't support block strings on extend, so we set description
	// directly on the parsed extension definition
	ddl := newDDL(t, `extend type User { _: Boolean }`)
	ddl.Extensions[0].Description = "Updated description"
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.Equal(t, "Updated description", user.Description)
}

func TestUpdate_ExtendAddDirective(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on OBJECT
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User @cached(ttl: 60) { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	cached := user.Directives.ForName("cached")
	require.NotNil(t, cached)
	assert.Equal(t, "60", cached.Arguments.ForName("ttl").Value.Raw)
}

func TestUpdate_ExtendDropDirective_All(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @tag(name: String!) repeatable on OBJECT
		type User @tag(name: "a") @tag(name: "b") { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User @drop_directive(name: "tag") { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.Empty(t, user.Directives.ForNames("tag"))
}

func TestUpdate_ExtendDropDirective_Match(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @tag(name: String!) repeatable on OBJECT
		type User @tag(name: "keep") @tag(name: "remove") { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User @drop_directive(name: "tag", match: {name: "remove"}) { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	tags := user.Directives.ForNames("tag")
	require.Len(t, tags, 1)
	assert.Equal(t, "keep", tags[0].Arguments.ForName("name").Value.Raw)
}

func TestUpdate_ExtendMergeFieldDirectives(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @default(value: String) on FIELD_DEFINITION
		type User { id: ID!, name: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { name: String @default(value: "Anonymous") }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	field := user.Fields.ForName("name")
	require.NotNil(t, field)
	defaultDir := field.Directives.ForName("default")
	require.NotNil(t, defaultDir)
	assert.Equal(t, "Anonymous", defaultDir.Arguments.ForName("value").Value.Raw)
}

func TestUpdate_ExtendAddEnumValue(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		enum Status { ACTIVE INACTIVE }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend enum Status { ARCHIVED }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	status := p.ForName(ctx, "Status")
	require.NotNil(t, status)
	assert.NotNil(t, status.EnumValues.ForName("ARCHIVED"))
	assert.NotNil(t, status.EnumValues.ForName("ACTIVE"))
}

func TestUpdate_ExtendDropEnumValue(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		enum Status { ACTIVE INACTIVE ARCHIVED }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend enum Status { ARCHIVED @drop }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	status := p.ForName(ctx, "Status")
	require.NotNil(t, status)
	assert.Nil(t, status.EnumValues.ForName("ARCHIVED"))
	assert.NotNil(t, status.EnumValues.ForName("ACTIVE"))
}

func TestUpdate_ExtendNotFound(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type Missing { name: String }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.ErrorIs(t, err, base.ErrDefinitionNotFound)
}

// --- Relationship updates ---

func TestUpdate_RelationshipsOnAdd(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		interface Node { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `type User implements Node { id: ID!, name: String! }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))

	// PossibleTypes for Node should include User
	possibles := slices.Collect(p.PossibleTypes(ctx, "Node"))
	require.Len(t, possibles, 1)
	assert.Equal(t, "User", possibles[0].Name)

	// Implements for User should include Node
	impls := slices.Collect(p.Implements(ctx, "User"))
	require.Len(t, impls, 1)
	assert.Equal(t, "Node", impls[0].Name)
}

func TestUpdate_RelationshipsOnDrop(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		interface Node { id: ID! }
		type User implements Node { id: ID!, name: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	// Verify relationships exist before drop
	possibles := slices.Collect(p.PossibleTypes(ctx, "Node"))
	require.Len(t, possibles, 1)

	ddl := newDDL(t, `type User @drop { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))

	// PossibleTypes for Node should be empty
	possibles = slices.Collect(p.PossibleTypes(ctx, "Node"))
	assert.Empty(t, possibles)
}

func TestUpdate_ExtendAddInterface(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		interface Node { id: ID! }
		type User { id: ID!, name: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User implements Node { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))

	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.Contains(t, user.Interfaces, "Node")

	// Relationships should be updated
	possibles := slices.Collect(p.PossibleTypes(ctx, "Node"))
	require.Len(t, possibles, 1)
	assert.Equal(t, "User", possibles[0].Name)
}

// --- DefinitionsSource only (no extensions) ---

func TestUpdate_DefinitionsSourceOnly(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
	`)
	p := static.New(s)
	ctx := context.Background()

	// Use a source that only implements DefinitionsSource, not ExtensionsSource
	ddl := newDDL(t, `type User { id: ID!, name: String! }`)
	src := static.NewDocSourceOnly(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.NotNil(t, user.Fields.ForName("name"))
}

// --- Multiple operations in one DDL ---

func TestUpdate_MultipleDefinitions(t *testing.T) {
	s := newTestSchema(t, `type Query { id: ID }`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `
		type User { id: ID!, name: String! }
		type Post { id: ID!, title: String! }
	`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	assert.NotNil(t, p.ForName(ctx, "User"))
	assert.NotNil(t, p.ForName(ctx, "Post"))
}

func TestUpdate_MixedDefinitionsAndExtensions(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `
		type Post { id: ID!, title: String! }
		extend type User { name: String! }
	`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	assert.NotNil(t, p.ForName(ctx, "Post"))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.NotNil(t, user.Fields.ForName("name"))
}

// --- Field-level directive operations ---

func TestUpdate_ExtendFieldAddDirective(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @deprecated(reason: String) on FIELD_DEFINITION
		type User { id: ID!, name: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { name: String @deprecated(reason: "use full_name") }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	field := user.Fields.ForName("name")
	require.NotNil(t, field)
	dep := field.Directives.ForName("deprecated")
	require.NotNil(t, dep)
	assert.Equal(t, "use full_name", dep.Arguments.ForName("reason").Value.Raw)
}

func TestUpdate_ExtendFieldDropDirective(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @default(value: String) on FIELD_DEFINITION
		type User {
			id: ID!
			name: String @default(value: "Anonymous")
		}
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { name: String @drop_directive(name: "default") }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	field := user.Fields.ForName("name")
	require.NotNil(t, field)
	assert.Nil(t, field.Directives.ForName("default"))
}

func TestUpdate_ExtendFieldDropDirective_Match(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @tag(name: String!) repeatable on FIELD_DEFINITION
		type User {
			id: ID!
			name: String @tag(name: "keep") @tag(name: "remove")
		}
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User { name: String @drop_directive(name: "tag", match: {name: "remove"}) }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	field := user.Fields.ForName("name")
	require.NotNil(t, field)
	tags := field.Directives.ForNames("tag")
	require.Len(t, tags, 1)
	assert.Equal(t, "keep", tags[0].Arguments.ForName("name").Value.Raw)
}

func TestUpdate_ExtendFieldChangeDirective(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @default(value: String) on FIELD_DEFINITION
		type User {
			id: ID!
			name: String @default(value: "Old")
		}
	`)
	p := static.New(s)
	ctx := context.Background()

	// Drop existing @default, then add new @default in same extension
	ddl := newDDL(t, `extend type User { name: String @drop_directive(name: "default") @default(value: "New") }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	field := user.Fields.ForName("name")
	require.NotNil(t, field)
	defaults := field.Directives.ForNames("default")
	require.Len(t, defaults, 1)
	assert.Equal(t, "New", defaults[0].Arguments.ForName("value").Value.Raw)
}

// --- Directive definition validation ---

func TestUpdate_DirectiveValidation_UndefinedDirectiveOnType(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User @nonexistent(foo: "bar") { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directive @nonexistent")
	assert.Contains(t, err.Error(), "not defined")
}

func TestUpdate_DirectiveValidation_UndefinedDirectiveOnField(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	// New field with undefined directive
	ddl := newDDL(t, `extend type User { name: String @nonexistent }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directive @nonexistent")
	assert.Contains(t, err.Error(), "not defined")
}

func TestUpdate_DirectiveValidation_UndefinedDirectiveOnFieldMerge(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID!, name: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	// Merge directive on existing field — undefined
	ddl := newDDL(t, `extend type User { name: String @undefined_dir }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directive @undefined_dir")
	assert.Contains(t, err.Error(), "not defined")
}

func TestUpdate_DirectiveValidation_MissingRequiredArg(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!) on OBJECT
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	// @cached requires ttl argument, but we omit it
	ddl := newDDL(t, `extend type User @cached { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directive @cached")
	assert.Contains(t, err.Error(), "missing required argument")
	assert.Contains(t, err.Error(), "ttl")
}

func TestUpdate_DirectiveValidation_MissingRequiredArgOnField(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @constraint(min: Int!) on FIELD_DEFINITION
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	// New field with @constraint missing required min arg
	ddl := newDDL(t, `extend type User { age: Int @constraint }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directive @constraint")
	assert.Contains(t, err.Error(), "missing required argument")
}

func TestUpdate_DirectiveValidation_OptionalArgOmitted(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		directive @cached(ttl: Int!, tags: [String]) on OBJECT
		type User { id: ID! }
	`)
	p := static.New(s)
	ctx := context.Background()

	// @cached with required ttl provided, optional tags omitted — should pass
	ddl := newDDL(t, `extend type User @cached(ttl: 60) { _: Boolean }`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	assert.NotNil(t, user.Directives.ForName("cached"))
}

func TestUpdate_DirectiveValidation_ReplaceFieldUndefined(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User { id: ID!, name: String }
	`)
	p := static.New(s)
	ctx := context.Background()

	// Replace field with undefined directive
	ddl := newDDL(t, `extend type User { name: String! @replace @not_real }`)
	src := static.NewDocumentProvider(ddl)

	err := p.Update(ctx, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directive @not_real")
	assert.Contains(t, err.Error(), "not defined")
}

func TestUpdate_ExtendFieldUpdateDescription(t *testing.T) {
	s := newTestSchema(t, `
		type Query { id: ID }
		type User {
			id: ID!
			"old description"
			name: String
		}
	`)
	p := static.New(s)
	ctx := context.Background()

	ddl := newDDL(t, `extend type User {
		"new description"
		name: String
	}`)
	src := static.NewDocumentProvider(ddl)

	require.NoError(t, p.Update(ctx, src))
	user := p.ForName(ctx, "User")
	require.NotNil(t, user)
	field := user.Fields.ForName("name")
	require.NotNil(t, field)
	assert.Equal(t, "new description", field.Description)
}

// --- Real-world sequential test ---
// Simulates the full lifecycle with proper module hierarchy.
// The compiler generates module types for the module tree and places queries/mutations
// into those types. The root types (Query, Mutation) get fields pointing to module types.
//
// Module hierarchy example:
//   @module(name: "core") → compiler creates core_query, adds Query.core → core_query
//   @module(name: "core.meta") → compiler creates core_meta_query as sub-module,
//     adds core_query.meta → core_meta_query, queries go into core_meta_query
//
// Functions are handled similarly: core_function, core_mutation_function types
// are created and linked from root Function / MutationFunction.

func TestUpdate_RealWorldSequential(t *testing.T) {
	// Phase 1: Build base schema with directives and core types
	baseSDL := `
		directive @system on OBJECT | INTERFACE | UNION | INPUT_OBJECT | ENUM | FIELD_DEFINITION | SCALAR
		directive @module(name: String!) on OBJECT | FIELD_DEFINITION
		directive @module_root(name: String!, type: ModuleObjectType!) on OBJECT
		directive @table(name: String!, is_m2m: Boolean) on OBJECT
		directive @view(name: String!, sql: String) on OBJECT
		directive @function(name: String!, sql: String) on FIELD_DEFINITION
		directive @query(name: String, type: QueryType) on OBJECT | FIELD_DEFINITION
		directive @mutation(name: String, type: MutationType, data_input: String!) on OBJECT | FIELD_DEFINITION
		directive @pk on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
		directive @default(value: any, sequence: String) on FIELD_DEFINITION | ARGUMENT_DEFINITION
		directive @field_references(
			name: String
			references_name: String!
			field: String
			query: String
			references_query: String
		) repeatable on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
		directive @field_source(field: String!) on FIELD_DEFINITION | INPUT_FIELD_DEFINITION
		directive @unique(fields: [String]!, query_suffix: String) repeatable on OBJECT
		directive @args(name: String!, required: Boolean) on OBJECT
		directive @references(
			name: String
			references_name: String!
			source_fields: [String!]!
			references_fields: [String!]!
			query: String
			references_query: String
		) repeatable on OBJECT

		scalar BigInt
		scalar Timestamp
		scalar JSON
		scalar Geometry
		scalar H3Cell
		scalar any

		enum ModuleObjectType @system { QUERY MUTATION FUNCTION MUT_FUNCTION }
		enum QueryType @system { SELECT SELECT_ONE }
		enum MutationType @system { INSERT UPDATE DELETE }

		type Query @system {
			jq(query: String!): Query @system
		}

		type Mutation @system {
			_placeholder: Boolean @system
		}

		type Function @system {
			_placeholder: Boolean @system
		}

		type MutationFunction @system {
			_placeholder: Boolean @system
		}

		type OperationResult @system {
			success: Boolean
			affected_rows: BigInt
			message: String
		}
	`
	s := newTestSchema(t, baseSDL)
	p := static.New(s)
	ctx := context.Background()

	// Verify base state
	q := p.ForName(ctx, "Query")
	require.NotNil(t, q)
	assert.Len(t, q.Fields, 1, "Query should only have jq field")

	// -------------------------------------------------------------------
	// Phase 2: Apply core-db module
	// Compiler creates module type hierarchy + data types + query/mutation types
	// Module: "core" → core_query, core_mutation, core_function, core_mutation_function
	// -------------------------------------------------------------------
	coreDBDDL := newDDL(t, `
		# --- Module types created by compiler (only query + mutation for tables) ---
		type core_query @system @module_root(name: "core", type: QUERY) {
			_placeholder: Boolean @system
		}
		type core_mutation @system @module_root(name: "core", type: MUTATION) {
			_placeholder: Boolean @system
		}

		# --- Root types get module fields ---
		extend type Query { core: core_query @module(name: "core") }
		extend type Mutation { core: core_mutation @module(name: "core") }

		# --- Data types (tables) ---
		type catalog_sources @module(name: "core") @table(name: "catalog_sources") {
			name: String! @pk
			type: String!
			description: String
			path: String!
		}

		type data_sources @module(name: "core") @table(name: "data_sources") {
			name: String! @pk
			type: String!
			prefix: String!
			path: String!
			disabled: Boolean @default(value: "false")
		}

		type catalogs @module(name: "core") @table(name: "data_source_catalogs", is_m2m: true) {
			catalog_name: String! @pk @field_references(references_name: "catalog_sources", field: "name", references_query: "data_sources")
			data_source_name: String! @pk @field_references(references_name: "data_sources", field: "name", references_query: "catalogs")
		}

		type roles @module(name: "core") @table(name: "roles") {
			name: String! @pk
			description: String!
			disabled: Boolean @default(value: "false")
		}

		type role_permissions @module(name: "core") @table(name: "permissions") {
			role: String! @pk @field_references(references_name: "roles", field: "name", references_query: "permissions")
			type_name: String! @pk
			field_name: String! @pk
			hidden: Boolean @default(value: "false")
		}

		type api_keys @module(name: "core") @table(name: "api_keys") @unique(fields: ["key"], query_suffix: "by_key") {
			name: String! @pk
			description: String!
			key: String!
			default_role: String @field_references(references_name: "roles", field: "name", references_query: "api_keys")
			disabled: Boolean @default(value: "false")
		}

		# --- Queries go into module query type ---
		extend type core_query {
			catalog_sources: [catalog_sources] @query(name: "catalog_sources", type: SELECT)
			data_sources: [data_sources] @query(name: "data_sources", type: SELECT)
			catalogs: [catalogs] @query(name: "catalogs", type: SELECT)
			roles: [roles] @query(name: "roles", type: SELECT)
			role_permissions: [role_permissions] @query(name: "role_permissions", type: SELECT)
			api_keys: [api_keys] @query(name: "api_keys", type: SELECT)
		}

		# --- Mutations go into module mutation type ---
		extend type core_mutation {
			insert_catalog_sources(name: String!, type: String!, path: String!): OperationResult @mutation(name: "insert_catalog_sources", type: INSERT, data_input: "catalog_sources_input")
			delete_catalog_sources(name: String!): OperationResult @mutation(name: "delete_catalog_sources", type: DELETE, data_input: "catalog_sources_input")
			insert_data_sources(name: String!, type: String!, prefix: String!, path: String!): OperationResult @mutation(name: "insert_data_sources", type: INSERT, data_input: "data_sources_input")
			insert_roles(name: String!, description: String!): OperationResult @mutation(name: "insert_roles", type: INSERT, data_input: "roles_input")
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(coreDBDDL)))

	// Verify module type hierarchy
	coreQ := p.ForName(ctx, "core_query")
	require.NotNil(t, coreQ, "core_query module type should exist")
	assert.NotNil(t, coreQ.Directives.ForName("module_root"))

	coreMut := p.ForName(ctx, "core_mutation")
	require.NotNil(t, coreMut, "core_mutation module type should exist")

	// Verify Query.core → core_query
	q = p.ForName(ctx, "Query")
	coreField := q.Fields.ForName("core")
	require.NotNil(t, coreField, "Query should have core field")
	assert.Equal(t, "core_query", coreField.Type.NamedType)

	// Verify Mutation.core → core_mutation
	mut := p.ForName(ctx, "Mutation")
	assert.NotNil(t, mut.Fields.ForName("core"), "Mutation should have core field")

	// Verify queries in core_query
	assert.NotNil(t, coreQ.Fields.ForName("catalog_sources"), "core_query should have catalog_sources")
	assert.NotNil(t, coreQ.Fields.ForName("data_sources"))
	assert.NotNil(t, coreQ.Fields.ForName("roles"))
	assert.NotNil(t, coreQ.Fields.ForName("api_keys"))
	assert.Equal(t, 7, len(coreQ.Fields), "core_query: _placeholder + 6 queries")

	// Verify mutations in core_mutation
	assert.NotNil(t, coreMut.Fields.ForName("insert_catalog_sources"))
	assert.NotNil(t, coreMut.Fields.ForName("insert_roles"))
	assert.Equal(t, 5, len(coreMut.Fields), "core_mutation: _placeholder + 4 mutations")

	// Verify data types
	assert.NotNil(t, p.ForName(ctx, "catalog_sources"))
	assert.NotNil(t, p.ForName(ctx, "data_sources"))

	// Verify field directives
	dsField := p.ForName(ctx, "data_sources").Fields.ForName("disabled")
	require.NotNil(t, dsField)
	assert.NotNil(t, dsField.Directives.ForName("default"))

	// -------------------------------------------------------------------
	// Phase 3: Apply data-sources module
	// Functions → compiler places into core_function / core_mutation_function
	// -------------------------------------------------------------------
	dataSourcesDDL := newDDL(t, `
		# --- First module that needs functions creates the function module types ---
		type core_function @if_not_exists @system @module_root(name: "core", type: FUNCTION) {
			_placeholder: Boolean @system
		}
		type core_mutation_function @if_not_exists @system @module_root(name: "core", type: MUT_FUNCTION) {
			_placeholder: Boolean @system
		}

		# --- Link to root (only if just created) ---
		extend type Function { core: core_function @module(name: "core") }
		extend type MutationFunction { core: core_mutation_function @module(name: "core") }

		extend type core_mutation_function {
			load_data_source(name: String!): OperationResult @function(name: "load_data_source")
			unload_data_source(name: String!): OperationResult @function(name: "unload_data_source")
		}

		extend type core_function {
			data_source_status(name: String!): String @function(name: "data_source_status")
			describe_data_source_schema(name: String!): String @function(name: "describe_data_source_schema")
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(dataSourcesDDL)))

	// core_function was created by this DDL via @if_not_exists
	coreFn := p.ForName(ctx, "core_function")
	require.NotNil(t, coreFn, "core_function should be created by data-sources DDL")
	assert.NotNil(t, coreFn.Fields.ForName("data_source_status"))
	assert.NotNil(t, coreFn.Fields.ForName("describe_data_source_schema"))
	assert.Equal(t, 3, len(coreFn.Fields), "core_function: _placeholder + 2 functions")

	// Function root should now link to core_function
	fn := p.ForName(ctx, "Function")
	assert.NotNil(t, fn.Fields.ForName("core"), "Function should now have core field")

	coreMutFn := p.ForName(ctx, "core_mutation_function")
	require.NotNil(t, coreMutFn, "core_mutation_function should be created")
	assert.NotNil(t, coreMutFn.Fields.ForName("load_data_source"))
	assert.NotNil(t, coreMutFn.Fields.ForName("unload_data_source"))

	// -------------------------------------------------------------------
	// Phase 4: Apply gis module
	// gis functions go into core_function (same module root)
	// -------------------------------------------------------------------
	gisDDL := newDDL(t, `
		extend type core_function {
			geom_to_h3_cells(geom: Geometry!, resolution: Int!): [H3Cell!] @function(name: "h3_geom_to_cells")
			h3_cell_to_geom(cell: H3Cell!): Geometry! @function(name: "h3_cell_to_geom")
			h3_cells_to_multi_polygon(cells: [H3Cell!]!): Geometry! @function(name: "h3_cells_to_multi_polygon")
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(gisDDL)))

	coreFn = p.ForName(ctx, "core_function")
	assert.NotNil(t, coreFn.Fields.ForName("geom_to_h3_cells"))
	assert.NotNil(t, coreFn.Fields.ForName("h3_cell_to_geom"))
	assert.NotNil(t, coreFn.Fields.ForName("h3_cells_to_multi_polygon"))
	assert.Equal(t, 6, len(coreFn.Fields), "core_function: _placeholder + 2 data-sources + 3 gis")

	// -------------------------------------------------------------------
	// Phase 5: Apply storage module
	// New types + storage functions in core_mutation_function
	// Queries go into core_query
	// -------------------------------------------------------------------
	storageDDL := newDDL(t, `
		type registered_object_storages @view(name: "core_registered_object_storages") {
			name: String! @pk
			type: String!
			scope: [String]
			parameters: String
		}

		input req_ls {
			path: String!
		}

		type ls @view(name: "path_view") @args(name: "req_ls") {
			name: String! @pk
			content: String!
		}

		extend type core_mutation_function {
			register_object_storage(type: String!, name: String!, scope: String!, key: String!, secret: String!, endpoint: String!): OperationResult @function(name: "register_object_storage")
			unregister_storage(name: String!): OperationResult @function(name: "unregister_object_storage")
		}

		extend type core_query {
			registered_object_storages: [registered_object_storages] @query(name: "registered_object_storages", type: SELECT)
			ls: [ls] @query(name: "ls", type: SELECT)
		}

		extend type core_mutation {
			register_storage(type: String!, name: String!, scope: String!): OperationResult @mutation(name: "register_storage", type: INSERT, data_input: "storage_input")
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(storageDDL)))

	assert.NotNil(t, p.ForName(ctx, "registered_object_storages"))
	assert.NotNil(t, p.ForName(ctx, "ls"))
	assert.NotNil(t, p.ForName(ctx, "req_ls"))

	coreQ = p.ForName(ctx, "core_query")
	assert.NotNil(t, coreQ.Fields.ForName("registered_object_storages"))
	assert.NotNil(t, coreQ.Fields.ForName("ls"))
	assert.Equal(t, 9, len(coreQ.Fields), "core_query: _placeholder + 6 core-db + 2 storage")

	coreMut = p.ForName(ctx, "core_mutation")
	assert.Equal(t, 6, len(coreMut.Fields), "core_mutation: _placeholder + 4 core-db + 1 storage")

	coreMutFn = p.ForName(ctx, "core_mutation_function")
	assert.NotNil(t, coreMutFn.Fields.ForName("register_object_storage"))
	assert.Equal(t, 5, len(coreMutFn.Fields), "core_mutation_function: _placeholder + 2 data-sources + 2 storage")

	// -------------------------------------------------------------------
	// Phase 6: Apply meta-info module with sub-module
	// Module: "core.meta" → compiler creates core_meta_query sub-module type
	// core_query gets field: meta → core_meta_query
	// -------------------------------------------------------------------
	metaInfoDDL := newDDL(t, `
		# Sub-module type
		type core_meta_query @system @module_root(name: "core.meta", type: QUERY) {
			_placeholder: Boolean @system
		}
		type core_meta_function @system @module_root(name: "core.meta", type: FUNCTION) {
			_placeholder: Boolean @system
		}

		# Link sub-module into parent module
		extend type core_query { meta: core_meta_query @module(name: "core.meta") }
		extend type core_function { meta: core_meta_function @module(name: "core.meta") }

		# --- Data types ---
		type databases @view(name: "duckdb_databases") @unique(fields: ["name"], query_suffix: "by_name") {
			id: BigInt! @pk @field_source(field: "database_oid")
			name: String! @field_source(field: "database_name")
			type: String!
			comment: String
			readonly: Boolean
			internal: Boolean
		}

		type schemas @view(name: "duckdb_schemas") {
			id: BigInt! @pk @field_source(field: "oid")
			name: String! @pk @field_source(field: "schema_name")
			database_id: BigInt! @field_source(field: "database_oid")
				@field_references(references_name: "databases", field: "id", query: "database", references_query: "schemas")
			database_name: String!
		}

		type tables @view(name: "duckdb_tables") {
			id: BigInt! @pk @field_source(field: "table_oid")
			name: String! @field_source(field: "table_name")
			database_id: BigInt! @field_source(field: "database_oid")
				@field_references(references_name: "databases", field: "id", query: "database", references_query: "tables")
			schema_id: BigInt! @field_source(field: "schema_oid")
				@field_references(references_name: "schemas", field: "id", query: "schema", references_query: "tables")
			schema_name: String!
			estimated_size: BigInt
			column_count: Int
		}

		type settings @view(name: "duckdb_settings") {
			name: String! @pk
			value: String
			description: String
			input_type: String
			scope: String
		}

		type duckdb_memory @view(name: "duckdb_memory") {
			tag: String! @pk
			memory_usage: BigInt @field_source(field: "memory_usage_bytes")
			temporary_storage: BigInt @field_source(field: "temporary_storage_bytes")
		}

		# --- Queries into sub-module ---
		extend type core_meta_query {
			databases: [databases] @query(name: "databases", type: SELECT)
			schemas_list: [schemas] @query(name: "schemas_list", type: SELECT)
			tables_list: [tables] @query(name: "tables_list", type: SELECT)
			settings: [settings] @query(name: "settings", type: SELECT)
			duckdb_memory: [duckdb_memory] @query(name: "duckdb_memory", type: SELECT)
		}

		# --- Functions into sub-module ---
		extend type core_meta_function {
			duckdb_version: String @function(name: "version")
			schema_summary: JSON @function(name: "core_meta_schema_info")
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(metaInfoDDL)))

	// Verify sub-module types
	coreMetaQ := p.ForName(ctx, "core_meta_query")
	require.NotNil(t, coreMetaQ, "core_meta_query sub-module should exist")

	coreMetaFn := p.ForName(ctx, "core_meta_function")
	require.NotNil(t, coreMetaFn, "core_meta_function sub-module should exist")

	// Verify sub-module linked into parent module
	coreQ = p.ForName(ctx, "core_query")
	metaField := coreQ.Fields.ForName("meta")
	require.NotNil(t, metaField, "core_query should have meta field linking to sub-module")
	assert.Equal(t, "core_meta_query", metaField.Type.NamedType)

	// Verify queries in sub-module
	assert.NotNil(t, coreMetaQ.Fields.ForName("databases"))
	assert.NotNil(t, coreMetaQ.Fields.ForName("settings"))
	assert.NotNil(t, coreMetaQ.Fields.ForName("duckdb_memory"))
	assert.Equal(t, 6, len(coreMetaQ.Fields), "core_meta_query: _placeholder + 5 queries")

	// Verify functions in sub-module
	assert.NotNil(t, coreMetaFn.Fields.ForName("duckdb_version"))
	assert.NotNil(t, coreMetaFn.Fields.ForName("schema_summary"))
	assert.Equal(t, 3, len(coreMetaFn.Fields), "core_meta_function: _placeholder + 2 functions")

	// Verify data types
	assert.NotNil(t, p.ForName(ctx, "databases"))
	assert.NotNil(t, p.ForName(ctx, "schemas"))
	assert.NotNil(t, p.ForName(ctx, "tables"))

	// Verify cross-module references on tables type
	tablesType := p.ForName(ctx, "tables")
	dbIDField := tablesType.Fields.ForName("database_id")
	require.NotNil(t, dbIDField)
	refs := dbIDField.Directives.ForNames("field_references")
	assert.Len(t, refs, 1, "database_id should have @field_references")

	// -------------------------------------------------------------------
	// Comprehensive check: module hierarchy navigation
	// Query → core → core_query → meta → core_meta_query → databases
	// -------------------------------------------------------------------
	q = p.ForName(ctx, "Query")
	assert.Equal(t, 2, len(q.Fields), "Query: jq + core")

	mut = p.ForName(ctx, "Mutation")
	assert.Equal(t, 2, len(mut.Fields), "Mutation: _placeholder + core")

	fn = p.ForName(ctx, "Function")
	assert.Equal(t, 2, len(fn.Fields), "Function: _placeholder + core")

	mutFunc := p.ForName(ctx, "MutationFunction")
	assert.Equal(t, 2, len(mutFunc.Fields), "MutationFunction: _placeholder + core")

	// Module types field counts
	coreQ = p.ForName(ctx, "core_query")
	assert.Equal(t, 10, len(coreQ.Fields), "core_query: _placeholder + 6 core-db + 2 storage + 1 meta")

	coreFn = p.ForName(ctx, "core_function")
	assert.Equal(t, 7, len(coreFn.Fields), "core_function: _placeholder + 2 data-sources + 3 gis + 1 meta")

	// ===================================================================
	// Phase 7: Sequential deletion — reverse order
	// ===================================================================

	// --- Delete meta-info: drop sub-module types, remove links, drop data types ---
	deleteMetaInfo := newDDL(t, `
		type databases @drop { _: Boolean }
		type schemas @drop { _: Boolean }
		type tables @drop { _: Boolean }
		type settings @drop { _: Boolean }
		type duckdb_memory @drop { _: Boolean }
		type core_meta_query @drop { _: Boolean }
		type core_meta_function @drop { _: Boolean }

		extend type core_query {
			meta: core_meta_query @drop
		}

		extend type core_function {
			meta: core_meta_function @drop
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(deleteMetaInfo)))

	assert.Nil(t, p.ForName(ctx, "databases"), "databases should be deleted")
	assert.Nil(t, p.ForName(ctx, "schemas"), "schemas should be deleted")
	assert.Nil(t, p.ForName(ctx, "tables"), "tables should be deleted")
	assert.Nil(t, p.ForName(ctx, "settings"), "settings should be deleted")
	assert.Nil(t, p.ForName(ctx, "duckdb_memory"), "duckdb_memory should be deleted")
	assert.Nil(t, p.ForName(ctx, "core_meta_query"), "sub-module query type should be deleted")
	assert.Nil(t, p.ForName(ctx, "core_meta_function"), "sub-module function type should be deleted")

	coreQ = p.ForName(ctx, "core_query")
	assert.Nil(t, coreQ.Fields.ForName("meta"), "meta link should be removed from core_query")
	assert.Equal(t, 9, len(coreQ.Fields), "core_query: back to _placeholder + 6 core-db + 2 storage")

	coreFn = p.ForName(ctx, "core_function")
	assert.Nil(t, coreFn.Fields.ForName("meta"), "meta link should be removed from core_function")
	assert.Equal(t, 6, len(coreFn.Fields), "core_function: back to _placeholder + 2 data-sources + 3 gis")

	// --- Delete storage ---
	deleteStorage := newDDL(t, `
		type registered_object_storages @drop { _: Boolean }
		type ls @drop { _: Boolean }
		type req_ls @drop { _: Boolean }

		extend type core_mutation_function {
			register_object_storage: OperationResult @drop
			unregister_storage: OperationResult @drop
		}

		extend type core_query {
			registered_object_storages: [registered_object_storages] @drop
			ls: [ls] @drop
		}

		extend type core_mutation {
			register_storage: OperationResult @drop
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(deleteStorage)))

	assert.Nil(t, p.ForName(ctx, "registered_object_storages"))
	assert.Nil(t, p.ForName(ctx, "ls"))
	assert.Nil(t, p.ForName(ctx, "req_ls"))

	coreQ = p.ForName(ctx, "core_query")
	assert.Equal(t, 7, len(coreQ.Fields), "core_query: _placeholder + 6 core-db")

	coreMut = p.ForName(ctx, "core_mutation")
	assert.Equal(t, 5, len(coreMut.Fields), "core_mutation: _placeholder + 4 core-db")

	coreMutFn = p.ForName(ctx, "core_mutation_function")
	assert.Equal(t, 3, len(coreMutFn.Fields), "core_mutation_function: _placeholder + 2 data-sources")

	// --- Delete gis ---
	deleteGis := newDDL(t, `
		extend type core_function {
			geom_to_h3_cells: [H3Cell!] @drop
			h3_cell_to_geom: Geometry @drop
			h3_cells_to_multi_polygon: Geometry @drop
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(deleteGis)))

	coreFn = p.ForName(ctx, "core_function")
	assert.Nil(t, coreFn.Fields.ForName("geom_to_h3_cells"))
	assert.Equal(t, 3, len(coreFn.Fields), "core_function: _placeholder + 2 data-sources")

	// --- Delete data-sources ---
	deleteDataSources := newDDL(t, `
		extend type core_mutation_function {
			load_data_source: OperationResult @drop
			unload_data_source: OperationResult @drop
		}

		extend type core_function {
			data_source_status: String @drop
			describe_data_source_schema: String @drop
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(deleteDataSources)))

	coreFn = p.ForName(ctx, "core_function")
	assert.Equal(t, 1, len(coreFn.Fields), "core_function: _placeholder only")

	coreMutFn = p.ForName(ctx, "core_mutation_function")
	assert.Equal(t, 1, len(coreMutFn.Fields), "core_mutation_function: _placeholder only")

	// --- Delete core-db: drop data types + module types, unlink from root ---
	deleteCoreDB := newDDL(t, `
		type catalog_sources @drop { _: Boolean }
		type data_sources @drop { _: Boolean }
		type catalogs @drop { _: Boolean }
		type roles @drop { _: Boolean }
		type role_permissions @drop { _: Boolean }
		type api_keys @drop { _: Boolean }
		type core_query @drop { _: Boolean }
		type core_mutation @drop { _: Boolean }
		type core_function @drop { _: Boolean }
		type core_mutation_function @drop { _: Boolean }

		extend type Query {
			core: core_query @drop
		}

		extend type Mutation {
			core: core_mutation @drop
		}

		extend type Function {
			core: core_function @drop
		}

		extend type MutationFunction {
			core: core_mutation_function @drop
		}
	`)
	require.NoError(t, p.Update(ctx, static.NewDocumentProvider(deleteCoreDB)))

	// All core-db types should be gone
	assert.Nil(t, p.ForName(ctx, "catalog_sources"))
	assert.Nil(t, p.ForName(ctx, "data_sources"))
	assert.Nil(t, p.ForName(ctx, "core_query"))
	assert.Nil(t, p.ForName(ctx, "core_mutation"))
	assert.Nil(t, p.ForName(ctx, "core_function"))
	assert.Nil(t, p.ForName(ctx, "core_mutation_function"))

	// -------------------------------------------------------------------
	// Final verification: schema should be back to base state
	// -------------------------------------------------------------------
	q = p.ForName(ctx, "Query")
	assert.Equal(t, 1, len(q.Fields), "Query should be back to 1 field (jq)")
	assert.NotNil(t, q.Fields.ForName("jq"), "Query should still have jq field")

	mut = p.ForName(ctx, "Mutation")
	assert.Equal(t, 1, len(mut.Fields), "Mutation should be back to 1 field (_placeholder)")

	fn = p.ForName(ctx, "Function")
	assert.Equal(t, 1, len(fn.Fields), "Function should be back to 1 field (_placeholder)")

	mutFunc = p.ForName(ctx, "MutationFunction")
	assert.Equal(t, 1, len(mutFunc.Fields), "MutationFunction should be back to 1 field (_placeholder)")

	// System types still intact
	assert.NotNil(t, p.ForName(ctx, "OperationResult"))
}
