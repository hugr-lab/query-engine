//go:build duckdb_arrow

package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	dbprovider "github.com/hugr-lab/query-engine/pkg/catalog/db"
	ctypes "github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/embedding"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"
)

// ─── DuckDB tests (in-memory, no Docker) ────────────────────────────────────

func TestDuckDB_ProviderLifecycle(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache: dbprovider.DefaultCacheConfig(),
	}, nil)
	require.NoError(t, err)

	// Update with a catalog
	source := newTestSource([]*ast.Definition{
		objectType("User", "test_cat", "A user type", []*ast.FieldDefinition{
			field("id", "Int", true),
			fieldWithDesc("name", "String", false, "user name",
				&ast.ArgumentDefinitionList{
					{Name: "format", Type: ast.NamedType("String", nil), Description: "name format",
						DefaultValue: &ast.Value{Raw: "full", Kind: ast.StringValue}},
				},
			),
		}),
		objectType("Post", "test_cat", "A post", []*ast.FieldDefinition{
			field("id", "Int", true),
			field("title", "String", false),
		}),
	})

	require.NoError(t, p.Update(ctx, source))

	// AC-1: Persist and retrieve
	def := p.ForName(ctx, "User")
	require.NotNil(t, def)
	assert.Equal(t, "A user type", def.Description)
	assert.Equal(t, ast.Object, def.Kind)
	assert.Len(t, def.Fields, 2)

	// Verify fields and arguments
	nameField := def.Fields.ForName("name")
	require.NotNil(t, nameField)
	assert.Equal(t, "user name", nameField.Description)
	require.Len(t, nameField.Arguments, 1)
	formatArg := nameField.Arguments.ForName("format")
	require.NotNil(t, formatArg)
	assert.Equal(t, "name format", formatArg.Description)
	require.NotNil(t, formatArg.DefaultValue)
	assert.Equal(t, "full", formatArg.DefaultValue.Raw)

	// AC-7: Types() streams all
	count := 0
	for range p.Types(ctx) {
		count++
	}
	assert.GreaterOrEqual(t, count, 2, "should stream at least User and Post")

	// AC-2: Cache hit — second call returns same pointer
	def2 := p.ForName(ctx, "User")
	require.NotNil(t, def2)
	assert.Same(t, def, def2, "second ForName should return cached pointer")

	// AC-5: DropCatalog removes types and fields
	require.NoError(t, p.DropCatalog(ctx, "test_cat", false))
	assert.Nil(t, p.ForName(ctx, "User"), "User should be gone after DropCatalog")
	assert.Nil(t, p.ForName(ctx, "Post"), "Post should be gone after DropCatalog")
}

func TestDuckDB_TypesStreamAndEnums(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		objectType("TypeA", "cat1", "", nil),
		objectType("TypeB", "cat1", "", nil),
		{
			Kind: ast.Enum, Name: "StatusEnum",
			Directives: ast.DirectiveList{catalogDir("cat1")},
			EnumValues: ast.EnumValueList{
				{Name: "ACTIVE", Description: "active status"},
				{Name: "INACTIVE"},
			},
		},
	})
	require.NoError(t, p.Update(ctx, source))

	var names []string
	for name, def := range p.Types(ctx) {
		names = append(names, name)
		assert.NotNil(t, def)
	}
	// System types are seeded by initDuckDBSchema, so total count includes them.
	assert.Contains(t, names, "TypeA")
	assert.Contains(t, names, "TypeB")
	assert.Contains(t, names, "StatusEnum")
	assert.GreaterOrEqual(t, len(names), 3, "should have at least the 3 test types")

	// Check enum values
	enumDef := p.ForName(ctx, "StatusEnum")
	require.NotNil(t, enumDef)
	require.Len(t, enumDef.EnumValues, 2)
	active := enumDef.EnumValues.ForName("ACTIVE")
	require.NotNil(t, active)
	assert.Equal(t, "active status", active.Description)
}

func TestDuckDB_PossibleTypesInterface(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		{
			Kind: ast.Interface, Name: "Node", Description: "A node",
			Directives: ast.DirectiveList{catalogDir("iface_cat")},
			Fields:     ast.FieldList{field("name", "String", false)},
		},
		{
			Kind: ast.Object, Name: "Dog", Description: "A dog",
			Interfaces: []string{"Node"},
			Directives: ast.DirectiveList{catalogDir("iface_cat")},
			Fields:     ast.FieldList{field("name", "String", false), field("breed", "String", false)},
		},
		{
			Kind: ast.Object, Name: "Cat", Description: "A cat",
			Interfaces: []string{"Node"},
			Directives: ast.DirectiveList{catalogDir("iface_cat")},
			Fields:     ast.FieldList{field("name", "String", false), field("indoor", "Boolean", false)},
		},
		// Unrelated type — not an implementor
		objectType("Other", "iface_cat", "", []*ast.FieldDefinition{field("x", "String", false)}),
	})
	require.NoError(t, p.Update(ctx, source))

	// AC-8: PossibleTypes for interface
	names := collectPossibleTypes(p, ctx, "Node")
	assert.Len(t, names, 2)
	assert.Contains(t, names, "Dog")
	assert.Contains(t, names, "Cat")
}

func TestDuckDB_PossibleTypesUnion(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		objectType("CatAnimal", "cat1", "", []*ast.FieldDefinition{field("meow", "String", false)}),
		objectType("DogAnimal", "cat1", "", []*ast.FieldDefinition{field("bark", "String", false)}),
		{
			Kind: ast.Union, Name: "Animal",
			Directives: ast.DirectiveList{catalogDir("cat1")},
			Types:      []string{"CatAnimal", "DogAnimal"},
		},
	})
	require.NoError(t, p.Update(ctx, source))

	names := collectPossibleTypes(p, ctx, "Animal")
	assert.Len(t, names, 2)
	assert.Contains(t, names, "CatAnimal")
	assert.Contains(t, names, "DogAnimal")
}

func TestDuckDB_PossibleTypesCache(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Interface, Name: "CachedIface", Directives: ast.DirectiveList{catalogDir("cat1")},
			Fields: ast.FieldList{field("id", "Int", true)}},
		{Kind: ast.Object, Name: "CachedImpl", Interfaces: []string{"CachedIface"},
			Directives: ast.DirectiveList{catalogDir("cat1")},
			Fields:     ast.FieldList{field("id", "Int", true), field("name", "String", false)}},
	})
	require.NoError(t, p.Update(ctx, source))

	impls1 := collectPossibleTypes(p, ctx, "CachedIface")
	assert.Len(t, impls1, 1)
	assert.Contains(t, impls1, "CachedImpl")

	// Second call uses cache
	impls2 := collectPossibleTypes(p, ctx, "CachedIface")
	assert.Equal(t, impls1, impls2)

	// Invalidate and re-check
	p.InvalidateCatalog("cat1")
	impls3 := collectPossibleTypes(p, ctx, "CachedIface")
	assert.Equal(t, impls1, impls3)
}

func TestDuckDB_CacheSelectiveInvalidation(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Two catalogs
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("Type1", "cat1", "", []*ast.FieldDefinition{field("a", "String", false)}),
	})))
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("Type2", "cat2", "", []*ast.FieldDefinition{field("b", "String", false)}),
	})))

	// Warm cache
	ptr1 := p.ForName(ctx, "Type1")
	ptr2 := p.ForName(ctx, "Type2")
	require.NotNil(t, ptr1)
	require.NotNil(t, ptr2)

	// Invalidate only cat1
	p.InvalidateCatalog("cat1")

	newPtr1 := p.ForName(ctx, "Type1")
	newPtr2 := p.ForName(ctx, "Type2")
	require.NotNil(t, newPtr1)
	require.NotNil(t, newPtr2)

	// AC-4: Tag-based invalidation is selective
	assert.True(t, ptr1 != newPtr1, "Type1 should have been evicted and reloaded")
	assert.True(t, ptr2 == newPtr2, "Type2 should still be cached")
}

func TestDuckDB_DirectiveHandling(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Persist a type
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("TempType", "cat1", "v1", []*ast.FieldDefinition{field("x", "String", false)}),
	})))
	require.NotNil(t, p.ForName(ctx, "TempType"))

	// @drop removes the type
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "TempType", Directives: ast.DirectiveList{dropDir()}},
	})))
	assert.Nil(t, p.ForName(ctx, "TempType"))

	// Re-create for @replace test
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("MutableType", "cat1", "v1", []*ast.FieldDefinition{field("old_field", "String", false)}),
	})))
	def := p.ForName(ctx, "MutableType")
	require.NotNil(t, def)
	assert.Equal(t, "v1", def.Description)

	// @replace deletes old and inserts new
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "MutableType", Description: "v2",
			Directives: ast.DirectiveList{catalogDir("cat1"), replaceDir()},
			Fields:     ast.FieldList{field("new_field", "Int", false)}},
	})))
	def = p.ForName(ctx, "MutableType")
	require.NotNil(t, def)
	assert.Equal(t, "v2", def.Description)
	require.Len(t, def.Fields, 1)
	assert.Equal(t, "new_field", def.Fields[0].Name)

	// @if_not_exists skips if type already exists
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "MutableType", Description: "overwritten",
			Directives: ast.DirectiveList{catalogDir("cat1"), ifNotExistsDir()},
			Fields:     ast.FieldList{field("y", "Int", false)}},
	})))
	def = p.ForName(ctx, "MutableType")
	require.NotNil(t, def)
	assert.Equal(t, "v2", def.Description) // unchanged

	// @if_not_exists creates brand new types
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "BrandNew", Description: "created",
			Directives: ast.DirectiveList{catalogDir("cat1"), ifNotExistsDir()},
			Fields:     ast.FieldList{field("z", "String", false)}},
	})))
	assert.NotNil(t, p.ForName(ctx, "BrandNew"))
}

func TestDuckDB_ExtensionFields(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Persist base type
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("Base", "cat1", "", []*ast.FieldDefinition{
			field("id", "Int", true),
			field("remove_me", "String", false),
			fieldWithDesc("mutable", "String", false, "old desc", nil),
		}),
	})))

	// Add field via extension
	extSource := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "Base", Fields: ast.FieldList{
			{Name: "extra", Type: ast.NamedType("String", nil), Description: "added field",
				Directives: ast.DirectiveList{depDir("ext_catalog")}},
		}},
	})
	require.NoError(t, p.Update(ctx, extSource))

	def := p.ForName(ctx, "Base")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 4) // id + remove_me + mutable + extra
	extra := def.Fields.ForName("extra")
	require.NotNil(t, extra)
	assert.Equal(t, "added field", extra.Description)

	// Drop field via extension
	extSource2 := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "Base", Fields: ast.FieldList{
			{Name: "remove_me", Type: ast.NamedType("String", nil), Directives: ast.DirectiveList{dropDir()}},
		}},
	})
	require.NoError(t, p.Update(ctx, extSource2))
	def = p.ForName(ctx, "Base")
	require.NotNil(t, def)
	assert.Nil(t, def.Fields.ForName("remove_me"))

	// Replace field via extension
	extSource3 := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "Base", Fields: ast.FieldList{
			{Name: "mutable", Type: ast.NamedType("Int", nil), Description: "new desc",
				Directives: ast.DirectiveList{replaceDir()}},
		}},
	})
	require.NoError(t, p.Update(ctx, extSource3))
	def = p.ForName(ctx, "Base")
	require.NotNil(t, def)
	f := def.Fields.ForName("mutable")
	require.NotNil(t, f)
	assert.Equal(t, "new desc", f.Description)
	assert.Equal(t, "Int", f.Type.NamedType)
}

func TestDuckDB_DirectiveDefinitions(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSourceWithDirs(nil, map[string]*ast.DirectiveDefinition{
		"cache": {
			Name:         "cache",
			Description:  "Cache directive",
			IsRepeatable: false,
			Locations:    []ast.DirectiveLocation{ast.LocationField, ast.LocationObject},
			Arguments: ast.ArgumentDefinitionList{
				{Name: "ttl", Type: ast.NamedType("Int", nil), Description: "TTL in seconds"},
				{Name: "key", Type: ast.NamedType("String", nil), Description: "Cache key"},
				{Name: "tags", Type: ast.ListType(ast.NonNullNamedType("String", nil), nil), Description: "Cache tags"},
			},
		},
	})
	require.NoError(t, p.Update(ctx, source))

	dir := p.DirectiveForName(ctx, "cache")
	require.NotNil(t, dir)
	assert.Equal(t, "cache", dir.Name)
	assert.Equal(t, "Cache directive", dir.Description)
	assert.False(t, dir.IsRepeatable)
	assert.Len(t, dir.Locations, 2)
	assert.Contains(t, dir.Locations, ast.LocationField)
	assert.Contains(t, dir.Locations, ast.LocationObject)

	// Verify directive arguments are persisted and round-tripped.
	require.Len(t, dir.Arguments, 3)

	ttlArg := dir.Arguments.ForName("ttl")
	require.NotNil(t, ttlArg)
	assert.Equal(t, "Int", ttlArg.Type.NamedType)
	assert.Equal(t, "TTL in seconds", ttlArg.Description)

	keyArg := dir.Arguments.ForName("key")
	require.NotNil(t, keyArg)
	assert.Equal(t, "String", keyArg.Type.NamedType)
	assert.Equal(t, "Cache key", keyArg.Description)

	tagsArg := dir.Arguments.ForName("tags")
	require.NotNil(t, tagsArg)
	assert.NotNil(t, tagsArg.Type.Elem)
	assert.Equal(t, "String", tagsArg.Type.Elem.NamedType)
	assert.True(t, tagsArg.Type.Elem.NonNull)
	assert.Equal(t, "Cache tags", tagsArg.Description)
}

func TestDuckDB_QueryAndMutationType(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: base.QueryBaseName,
			Directives: ast.DirectiveList{catalogDir("core")},
			Fields:     ast.FieldList{field("health", "String", false)}},
		{Kind: ast.Object, Name: base.MutationBaseName,
			Directives: ast.DirectiveList{catalogDir("core")},
			Fields:     ast.FieldList{field("noop", "String", false)}},
	})
	require.NoError(t, p.Update(ctx, source))

	qt := p.QueryType(ctx)
	require.NotNil(t, qt)
	assert.Equal(t, base.QueryBaseName, qt.Name)

	mt := p.MutationType(ctx)
	require.NotNil(t, mt)
	assert.Equal(t, base.MutationBaseName, mt.Name)

	assert.Nil(t, p.SubscriptionType(ctx))
}

func TestDuckDB_TypeWithoutCatalog(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Scalar, Name: "JSON"},
	})
	require.NoError(t, p.Update(ctx, source))

	def := p.ForName(ctx, "JSON")
	require.NotNil(t, def)
	assert.Equal(t, ast.Scalar, def.Kind)
}

func TestDuckDB_ForNameNonExistent(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	p, err := dbprovider.New(t.Context(), pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)
	assert.Nil(t, p.ForName(context.Background(), "DoesNotExist"))
}

func TestDuckDB_ProviderWithEmbeddings(t *testing.T) {
	pool := newDuckDBPool(t)

	vecSize := 128
	initDuckDBSchema(t, pool, vecSize)

	ctx := t.Context()
	emb := &mockEmbedder{vec: makeVector(vecSize)}
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache:   dbprovider.DefaultCacheConfig(),
		VecSize: vecSize,
	}, emb)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		objectType("EmbType", "emb_cat", "Embeddable type", []*ast.FieldDefinition{
			fieldWithDesc("val", "String", false, "A field", nil),
		}),
	})
	require.NoError(t, p.Update(ctx, source))

	// AC-14: Embeddings computed for type and field
	assert.GreaterOrEqual(t, emb.callCount, 2, "embedder should have been called for type and field")

	// AC-17: SetDefinitionDescription recomputes embedding
	oldCount := emb.callCount
	err = p.SetDefinitionDescription(ctx, "EmbType", "new desc", "long desc")
	require.NoError(t, err)
	assert.Greater(t, emb.callCount, oldCount, "embedding should be recomputed on SetDefinitionDescription")

	// AC-16: Embedding uses long_description when available
	lastInput := emb.inputs[len(emb.inputs)-1]
	assert.Equal(t, "long desc", lastInput, "embedding should use long_description")

	// Verify description updated
	def := p.ForName(ctx, "EmbType")
	require.NotNil(t, def)
	assert.Equal(t, "new desc", def.Description)
}

func TestDuckDB_ProviderWithoutEmbeddings(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache: dbprovider.DefaultCacheConfig(),
	}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		objectType("NoEmbType", "no_emb_cat", "Type without embeddings", []*ast.FieldDefinition{
			field("id", "Int", true),
		}),
	})
	require.NoError(t, p.Update(ctx, source))

	// AC-15: NULL vec when embedder not provided
	def := p.ForName(ctx, "NoEmbType")
	require.NotNil(t, def)
	assert.Equal(t, "Type without embeddings", def.Description)
}

func TestDuckDB_SummarizedDescriptionReset(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache: dbprovider.DefaultCacheConfig(),
	}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		objectType("Summarized", "sum_cat", "original", []*ast.FieldDefinition{
			fieldWithDesc("x", "Int", true, "field initial", nil),
		}),
	})
	require.NoError(t, p.Update(ctx, source))

	// Manually mark as summarized with enriched descriptions
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `UPDATE _schema_types SET is_summarized = true, description = 'enriched' WHERE name = 'Summarized'`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `UPDATE _schema_fields SET is_summarized = true, description = 'enriched field' WHERE type_name = 'Summarized' AND name = 'x'`)
	require.NoError(t, err)
	conn.Close()

	// Re-update same catalog — should reset is_summarized and overwrite descriptions.
	// In production, version matching prevents re-compilation entirely, so summarized
	// descriptions are only lost when the catalog source actually changes.
	p.InvalidateAll()
	require.NoError(t, p.Update(ctx, source))

	def := p.ForName(ctx, "Summarized")
	require.NotNil(t, def)
	assert.Equal(t, "original", def.Description, "re-Update should reset description to source value")
	f := def.Fields.ForName("x")
	require.NotNil(t, f)
	assert.Equal(t, "field initial", f.Description, "re-Update should reset field description to source value")
}

func TestDuckDB_DisabledCatalog(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache: dbprovider.DefaultCacheConfig(),
	}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		objectType("DisType", "dis_cat", "Disabled type", []*ast.FieldDefinition{
			field("id", "Int", true),
		}),
	})
	require.NoError(t, p.Update(ctx, source))
	require.NotNil(t, p.ForName(ctx, "DisType"))

	// AC-9: Disabled catalog hides types
	require.NoError(t, p.SetCatalogDisabled(ctx, "dis_cat", true))
	assert.Nil(t, p.ForName(ctx, "DisType"), "disabled catalog should hide types")
}

func TestDuckDB_DisabledCatalogFieldFiltering(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Persist base type
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("Base", "base_cat", "", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// Create ext_cat and add extension field
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO _schema_catalogs (name) VALUES ('ext_cat') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	conn.Close()

	extSource := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "Base", Fields: ast.FieldList{
			{Name: "ext_field", Type: ast.NamedType("String", nil), Description: "from ext",
				Directives: ast.DirectiveList{depDir("ext_cat")}},
		}},
	})
	require.NoError(t, p.Update(ctx, extSource))

	// Both fields visible
	p.InvalidateCatalog("base_cat")
	def := p.ForName(ctx, "Base")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 2)

	// Disable ext_cat
	conn, err = pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `UPDATE _schema_catalogs SET disabled = true WHERE name = 'ext_cat'`)
	require.NoError(t, err)
	conn.Close()
	p.InvalidateCatalog("base_cat")

	// AC-10: Base type visible but ext_field hidden
	def = p.ForName(ctx, "Base")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 1)
	assert.Equal(t, "id", def.Fields[0].Name)
}

func TestDuckDB_SetDefinitionDescription(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 128)

	ctx := t.Context()
	emb := &mockEmbedder{vec: makeVector(128)}
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache: dbprovider.DefaultCacheConfig(), VecSize: 128,
	}, emb)
	require.NoError(t, err)

	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("Updatable", "cat1", "original", []*ast.FieldDefinition{field("x", "String", false)}),
	})))

	callsBefore := emb.callCount
	require.NoError(t, p.SetDefinitionDescription(ctx, "Updatable", "new enriched description", ""))
	assert.Greater(t, emb.callCount, callsBefore)

	p.InvalidateAll()
	def := p.ForName(ctx, "Updatable")
	require.NotNil(t, def)
	assert.Equal(t, "new enriched description", def.Description)

	// Re-persist should overwrite — is_summarized is always reset on Update.
	// In production, version matching prevents re-compilation when source hasn't changed.
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("Updatable", "cat1", "overwrite attempt", []*ast.FieldDefinition{field("x", "String", false)}),
	})))
	p.InvalidateAll()
	def = p.ForName(ctx, "Updatable")
	require.NotNil(t, def)
	assert.Equal(t, "overwrite attempt", def.Description) // new source description
}

func TestDuckDB_SetFieldDescription(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 128)

	ctx := t.Context()
	emb := &mockEmbedder{vec: makeVector(128)}
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache: dbprovider.DefaultCacheConfig(), VecSize: 128,
	}, emb)
	require.NoError(t, err)

	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("MyType", "cat1", "a type", []*ast.FieldDefinition{
			fieldWithDesc("myfield", "String", false, "original field desc", nil),
		}),
	})))

	callsBefore := emb.callCount
	require.NoError(t, p.SetFieldDescription(ctx, "MyType", "myfield", "new field desc", "long field description"))
	assert.Greater(t, emb.callCount, callsBefore)

	p.InvalidateAll()
	def := p.ForName(ctx, "MyType")
	require.NotNil(t, def)
	f := def.Fields.ForName("myfield")
	require.NotNil(t, f)
	assert.Equal(t, "new field desc", f.Description)

	// Re-persist should overwrite — is_summarized is always reset on Update.
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("MyType", "cat1", "a type", []*ast.FieldDefinition{
			fieldWithDesc("myfield", "String", false, "overwrite attempt", nil),
		}),
	})))
	p.InvalidateAll()
	def = p.ForName(ctx, "MyType")
	require.NotNil(t, def)
	f = def.Fields.ForName("myfield")
	require.NotNil(t, f)
	assert.Equal(t, "overwrite attempt", f.Description) // new source description
}

func TestDuckDB_SetCatalogDescription(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 128)

	ctx := t.Context()
	emb := &mockEmbedder{vec: makeVector(128)}
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache: dbprovider.DefaultCacheConfig(), VecSize: 128,
	}, emb)
	require.NoError(t, err)

	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("X", "desc_cat", "", []*ast.FieldDefinition{field("x", "String", false)}),
	})))

	callsBefore := emb.callCount
	require.NoError(t, p.SetCatalogDescription(ctx, "desc_cat", "catalog desc", "long catalog desc"))
	assert.Greater(t, emb.callCount, callsBefore)

	// Verify description persisted
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	var desc, longDesc string
	err = conn.QueryRow(ctx, `SELECT description, long_description FROM _schema_catalogs WHERE name = 'desc_cat'`).Scan(&desc, &longDesc)
	require.NoError(t, err)
	assert.Equal(t, "catalog desc", desc)
	assert.Equal(t, "long catalog desc", longDesc)
	conn.Close()
}

func TestDuckDB_SetModuleDescription(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Insert a module record
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO _schema_modules (name) VALUES ('testmod')`)
	require.NoError(t, err)
	conn.Close()

	require.NoError(t, p.SetModuleDescription(ctx, "testmod", "module desc", "long module desc"))

	conn, err = pool.Conn(ctx)
	require.NoError(t, err)
	var desc, longDesc string
	err = conn.QueryRow(ctx, `SELECT description, long_description FROM _schema_modules WHERE name = 'testmod'`).Scan(&desc, &longDesc)
	require.NoError(t, err)
	assert.Equal(t, "module desc", desc)
	assert.Equal(t, "long module desc", longDesc)
	conn.Close()
}

func TestDuckDB_DropCatalogDetailedCleanup(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "DroppedType", Description: "to be dropped",
			Directives: ast.DirectiveList{catalogDir("drop_cat")},
			Fields: ast.FieldList{
				field("f1", "String", false),
				fieldWithDesc("f2", "Int", false, "", &ast.ArgumentDefinitionList{
					{Name: "arg1", Type: ast.NamedType("String", nil)},
				}),
			}},
		{Kind: ast.Enum, Name: "DroppedEnum", Directives: ast.DirectiveList{catalogDir("drop_cat")},
			EnumValues: ast.EnumValueList{{Name: "A"}, {Name: "B"}}},
	})
	require.NoError(t, p.Update(ctx, source))

	require.NotNil(t, p.ForName(ctx, "DroppedType"))
	require.NotNil(t, p.ForName(ctx, "DroppedEnum"))

	// AC-5: DropCatalog removes types and fields
	require.NoError(t, p.DropCatalog(ctx, "drop_cat", false))
	p.InvalidateAll()
	assert.Nil(t, p.ForName(ctx, "DroppedType"))
	assert.Nil(t, p.ForName(ctx, "DroppedEnum"))

	// Verify DB is clean
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var count int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM _schema_types WHERE catalog = 'drop_cat'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "types should be deleted")

	err = conn.QueryRow(ctx, `SELECT count(*) FROM _schema_fields WHERE type_name = 'DroppedType'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "fields should be deleted")

	err = conn.QueryRow(ctx, `SELECT count(*) FROM _schema_arguments WHERE type_name = 'DroppedType'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "arguments should be deleted")

	err = conn.QueryRow(ctx, `SELECT count(*) FROM _schema_enum_values WHERE type_name = 'DroppedEnum'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "enum values should be deleted")
}

func TestDuckDB_DropCatalogCascade(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Create base and dependent catalogs
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("BaseType", "base_cat", "Base type", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("DepType", "dep_cat", "Dependent type", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// Create dependency
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO _schema_catalog_dependencies (catalog_name, depends_on) VALUES ('dep_cat', 'base_cat')`)
	require.NoError(t, err)
	conn.Close()

	// Drop base catalog with cascade — should suspend dep_cat
	require.NoError(t, p.DropCatalog(ctx, "base_cat", true))
	assert.Nil(t, p.ForName(ctx, "BaseType"), "BaseType should be gone")

	cat, err := p.GetCatalog(ctx, "dep_cat")
	require.NoError(t, err)
	require.NotNil(t, cat)
	assert.True(t, cat.Suspended, "dependent catalog should be suspended")
}

func TestDuckDB_DropCatalogCleansExtensionFields(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Persist base type
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("Host", "host_cat", "", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// Create dep_cat and add extension field to Host
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO _schema_catalogs (name) VALUES ('dep_cat') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	conn.Close()

	extSource := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "Host", Fields: ast.FieldList{
			{Name: "dep_field", Type: ast.NamedType("String", nil),
				Directives: ast.DirectiveList{depDir("dep_cat")}},
		}},
	})
	require.NoError(t, p.Update(ctx, extSource))

	p.InvalidateAll()
	def := p.ForName(ctx, "Host")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 2)

	// Drop dep_cat — extension field should be removed
	require.NoError(t, p.DropCatalog(ctx, "dep_cat", false))
	p.InvalidateAll()
	def = p.ForName(ctx, "Host")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 1)
	assert.Equal(t, "id", def.Fields[0].Name)
}

func TestDuckDB_ReconcileModules(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// AC-11: Module-catalog link table populated
	source := newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "geo_Query",
			Directives: ast.DirectiveList{
				catalogDir("geo_cat"),
				{Name: base.ModuleRootDirectiveName},
				{Name: base.ModuleDirectiveName, Arguments: ast.ArgumentList{
					{Name: base.ArgName, Value: &ast.Value{Raw: "geo", Kind: ast.StringValue}},
				}},
			},
			Fields: ast.FieldList{field("x", "String", false)},
		},
	})
	require.NoError(t, p.Update(ctx, source))

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var count int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM _schema_modules WHERE name = 'geo'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	err = conn.QueryRow(ctx, `SELECT count(*) FROM _schema_module_type_catalogs WHERE type_name = 'geo_Query' AND catalog_name = 'geo_cat'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestDuckDB_CatalogGetters(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("GetterType", "getter_cat", "test", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// GetCatalog
	cat, err := p.GetCatalog(ctx, "getter_cat")
	require.NoError(t, err)
	require.NotNil(t, cat)
	assert.Equal(t, "getter_cat", cat.Name)
	assert.False(t, cat.Disabled)
	assert.False(t, cat.Suspended)

	// SetCatalogVersion
	require.NoError(t, p.SetCatalogVersion(ctx, "getter_cat", "2.0"))
	cat, err = p.GetCatalog(ctx, "getter_cat")
	require.NoError(t, err)
	assert.Equal(t, "2.0", cat.Version)

	// SetCatalogDisabled
	require.NoError(t, p.SetCatalogDisabled(ctx, "getter_cat", true))
	cat, err = p.GetCatalog(ctx, "getter_cat")
	require.NoError(t, err)
	assert.True(t, cat.Disabled)

	// SetCatalogSuspended
	require.NoError(t, p.SetCatalogSuspended(ctx, "getter_cat", true))
	cat, err = p.GetCatalog(ctx, "getter_cat")
	require.NoError(t, err)
	assert.True(t, cat.Suspended)

	// ListCatalogs
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("GetterType2", "another_cat", "", []*ast.FieldDefinition{field("y", "String", false)}),
	})))
	catalogs, err := p.ListCatalogs(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(catalogs), 2)

	names := make([]string, len(catalogs))
	for i, c := range catalogs {
		names[i] = c.Name
	}
	assert.Contains(t, names, "getter_cat")
	assert.Contains(t, names, "another_cat")

	// Non-existent catalog
	cat, err = p.GetCatalog(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, cat)
}

func TestDuckDB_VectorSizeMigration(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 128)

	ctx := t.Context()
	p1, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache:   dbprovider.DefaultCacheConfig(),
		VecSize: 128,
	}, nil)
	require.NoError(t, err)

	require.NoError(t, p1.Update(ctx, newTestSource([]*ast.Definition{
		objectType("VecType", "vec_cat", "vec test", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// AC-20: Create new provider with different vec_size — should migrate
	p2, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache:   dbprovider.DefaultCacheConfig(),
		VecSize: 256,
	}, nil)
	require.NoError(t, err)

	def := p2.ForName(ctx, "VecType")
	require.NotNil(t, def)
	assert.Equal(t, "vec test", def.Description)
}

func TestDuckDB_VectorSizeZero(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache:   dbprovider.DefaultCacheConfig(),
		VecSize: 0,
	}, nil)
	require.NoError(t, err)

	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("ZeroVecType", "zv_cat", "no vec", []*ast.FieldDefinition{field("x", "Int", true)}),
	})))
	def := p.ForName(ctx, "ZeroVecType")
	require.NotNil(t, def)
	assert.Equal(t, "no vec", def.Description)
}

func TestDuckDB_AttachedMode(t *testing.T) {
	pool := newDuckDBPool(t)
	ctx := t.Context()

	// Attach an in-memory DB as "core"
	_, err := pool.Exec(ctx, "ATTACH ':memory:' AS core;")
	require.NoError(t, err)

	// Initialize schema with core. prefix
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBAttachedDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 128,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	// Seed system types in the attached "core" schema.
	seedSystemTypesWithPrefix(t, pool, "core.", 128)

	// AC-18: SQL prefix works
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache:       dbprovider.DefaultCacheConfig(),
		TablePrefix: "core.",
		VecSize:     128,
	}, nil)
	require.NoError(t, err)

	source := newTestSource([]*ast.Definition{
		objectType("AttachedType", "attached_cat", "Attached test", []*ast.FieldDefinition{
			field("id", "Int", true),
		}),
	})
	require.NoError(t, p.Update(ctx, source))

	def := p.ForName(ctx, "AttachedType")
	require.NotNil(t, def)
	assert.Equal(t, "Attached test", def.Description)

	// Verify data is in core schema
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var count int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM core._schema_types WHERE name = 'AttachedType'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "type should be in core schema")

	// DropCatalog should also work with prefix
	require.NoError(t, p.DropCatalog(ctx, "attached_cat", false))
	assert.Nil(t, p.ForName(ctx, "AttachedType"))
}

func TestDuckDB_ExecWriteOperations(t *testing.T) {
	pool := newDuckDBPool(t)
	initDuckDBSchema(t, pool, 0)

	ctx := t.Context()
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{Cache: dbprovider.DefaultCacheConfig()}, nil)
	require.NoError(t, err)

	// Test INSERT/UPDATE/DELETE through the provider's public API
	require.NoError(t, p.Update(ctx, newTestSource([]*ast.Definition{
		objectType("WriteTest", "write_cat", "v1", []*ast.FieldDefinition{field("x", "String", false)}),
	})))

	def := p.ForName(ctx, "WriteTest")
	require.NotNil(t, def)
	assert.Equal(t, "v1", def.Description)

	// Update via SetDefinitionDescription
	require.NoError(t, p.SetDefinitionDescription(ctx, "WriteTest", "v2", ""))
	p.InvalidateAll()
	def = p.ForName(ctx, "WriteTest")
	require.NotNil(t, def)
	assert.Equal(t, "v2", def.Description)

	// Delete via DropCatalog
	require.NoError(t, p.DropCatalog(ctx, "write_cat", false))
	assert.Nil(t, p.ForName(ctx, "WriteTest"))
}

// ─── PostgreSQL tests (real PG via DuckDB attach) ───────────────────────────

// pgEnv holds the PostgreSQL test environment: DuckDB pool with attached PG,
// direct PG connection for verification, and the provider.
type pgEnv struct {
	pool   *db.Pool
	pgConn *sql.DB
	p      *dbprovider.Provider
	ctx    context.Context
}

// newPGEnv sets up PG test environment with optional embedder.
func newPGEnv(t *testing.T, vecSize int, emb dbprovider.Embedder) *pgEnv {
	t.Helper()
	pgConn := openPG(t)
	t.Cleanup(func() {
		cleanPG(t, pgConn)
		pgConn.Close()
	})
	cleanPG(t, pgConn)
	initPGSchema(t, pgConn)

	pool := newDuckDBPool(t)
	ctx := t.Context()
	attachPG(t, pool, ctx)

	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache:       dbprovider.DefaultCacheConfig(),
		TablePrefix: "core.",
		IsPostgres:  true,
		VecSize:     vecSize,
	}, emb)
	require.NoError(t, err)

	return &pgEnv{pool: pool, pgConn: pgConn, p: p, ctx: ctx}
}

// pgCount returns the row count for a query on the PG connection.
func (e *pgEnv) pgCount(t *testing.T, query string, args ...any) int {
	t.Helper()
	var count int
	err := e.pgConn.QueryRow(query, args...).Scan(&count)
	require.NoError(t, err)
	return count
}

func TestPostgres_ProviderLifecycle(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		objectType("PgUser", "pg_cat", "A PG user type", []*ast.FieldDefinition{
			field("id", "Int", true),
			fieldWithDesc("name", "String", false, "user name",
				&ast.ArgumentDefinitionList{
					{Name: "format", Type: ast.NamedType("String", nil), Description: "name format",
						DefaultValue: &ast.Value{Raw: "full", Kind: ast.StringValue}},
				},
			),
		}),
		objectType("PgPost", "pg_cat", "A PG post", []*ast.FieldDefinition{
			field("id", "Int", true),
			field("body", "String", false),
		}),
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	// Retrieve — reads go through DuckDB's postgres_query
	def := env.p.ForName(env.ctx, "PgUser")
	require.NotNil(t, def, "PgUser should be retrievable from PG")
	assert.Equal(t, "A PG user type", def.Description)
	assert.Equal(t, ast.Object, def.Kind)
	assert.Len(t, def.Fields, 2)

	// Verify fields and arguments
	nameField := def.Fields.ForName("name")
	require.NotNil(t, nameField)
	assert.Equal(t, "user name", nameField.Description)
	require.Len(t, nameField.Arguments, 1)
	formatArg := nameField.Arguments.ForName("format")
	require.NotNil(t, formatArg)
	assert.Equal(t, "name format", formatArg.Description)
	require.NotNil(t, formatArg.DefaultValue)
	assert.Equal(t, "full", formatArg.DefaultValue.Raw)

	def2 := env.p.ForName(env.ctx, "PgPost")
	require.NotNil(t, def2)
	assert.Equal(t, "A PG post", def2.Description)

	// Types() streams all
	count := 0
	for range env.p.Types(env.ctx) {
		count++
	}
	assert.GreaterOrEqual(t, count, 2, "should stream at least PgUser and PgPost")

	// Cache hit
	def3 := env.p.ForName(env.ctx, "PgUser")
	require.NotNil(t, def3)
	assert.Same(t, def, def3, "second ForName should return cached pointer")

	// Verify data is actually in PostgreSQL
	assert.GreaterOrEqual(t, env.pgCount(t, `SELECT count(*) FROM _schema_types`), 2)

	// DropCatalog — deletes through postgres_execute
	require.NoError(t, env.p.DropCatalog(env.ctx, "pg_cat", false))
	assert.Nil(t, env.p.ForName(env.ctx, "PgUser"))
	assert.Nil(t, env.p.ForName(env.ctx, "PgPost"))

	// Verify deleted from PG
	assert.Equal(t, 0, env.pgCount(t, `SELECT count(*) FROM _schema_types WHERE catalog = 'pg_cat'`))
}

func TestPostgres_TypesStreamAndEnums(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		objectType("PgTypeA", "cat1", "", nil),
		objectType("PgTypeB", "cat1", "", nil),
		{
			Kind: ast.Enum, Name: "PgStatusEnum",
			Directives: ast.DirectiveList{catalogDir("cat1")},
			EnumValues: ast.EnumValueList{
				{Name: "ACTIVE", Description: "active status"},
				{Name: "INACTIVE"},
			},
		},
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	var names []string
	for name, def := range env.p.Types(env.ctx) {
		names = append(names, name)
		assert.NotNil(t, def)
	}
	assert.Len(t, names, 3)
	assert.Contains(t, names, "PgTypeA")
	assert.Contains(t, names, "PgTypeB")
	assert.Contains(t, names, "PgStatusEnum")

	// Check enum values
	enumDef := env.p.ForName(env.ctx, "PgStatusEnum")
	require.NotNil(t, enumDef)
	require.Len(t, enumDef.EnumValues, 2)
	active := enumDef.EnumValues.ForName("ACTIVE")
	require.NotNil(t, active)
	assert.Equal(t, "active status", active.Description)
}

func TestPostgres_PossibleTypesInterface(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		{
			Kind: ast.Interface, Name: "PgNode", Description: "A node",
			Directives: ast.DirectiveList{catalogDir("iface_cat")},
			Fields:     ast.FieldList{field("name", "String", false)},
		},
		{
			Kind: ast.Object, Name: "PgDog", Description: "A dog",
			Interfaces: []string{"PgNode"},
			Directives: ast.DirectiveList{catalogDir("iface_cat")},
			Fields:     ast.FieldList{field("name", "String", false), field("breed", "String", false)},
		},
		{
			Kind: ast.Object, Name: "PgCat", Description: "A cat",
			Interfaces: []string{"PgNode"},
			Directives: ast.DirectiveList{catalogDir("iface_cat")},
			Fields:     ast.FieldList{field("name", "String", false), field("indoor", "Boolean", false)},
		},
		objectType("PgOther", "iface_cat", "", []*ast.FieldDefinition{field("x", "String", false)}),
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	names := collectPossibleTypes(env.p, env.ctx, "PgNode")
	assert.Len(t, names, 2)
	assert.Contains(t, names, "PgDog")
	assert.Contains(t, names, "PgCat")
}

func TestPostgres_PossibleTypesUnion(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		objectType("PgCatAnimal", "cat1", "", []*ast.FieldDefinition{field("meow", "String", false)}),
		objectType("PgDogAnimal", "cat1", "", []*ast.FieldDefinition{field("bark", "String", false)}),
		{
			Kind: ast.Union, Name: "PgAnimal",
			Directives: ast.DirectiveList{catalogDir("cat1")},
			Types:      []string{"PgCatAnimal", "PgDogAnimal"},
		},
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	names := collectPossibleTypes(env.p, env.ctx, "PgAnimal")
	assert.Len(t, names, 2)
	assert.Contains(t, names, "PgCatAnimal")
	assert.Contains(t, names, "PgDogAnimal")
}

func TestPostgres_DirectiveHandling(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	// Persist a type
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgTempType", "cat1", "v1", []*ast.FieldDefinition{field("x", "String", false)}),
	})))
	require.NotNil(t, env.p.ForName(env.ctx, "PgTempType"))

	// @drop removes the type
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "PgTempType", Directives: ast.DirectiveList{dropDir()}},
	})))
	assert.Nil(t, env.p.ForName(env.ctx, "PgTempType"))

	// @replace
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgMutableType", "cat1", "v1", []*ast.FieldDefinition{field("old_field", "String", false)}),
	})))
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "PgMutableType", Description: "v2",
			Directives: ast.DirectiveList{catalogDir("cat1"), replaceDir()},
			Fields:     ast.FieldList{field("new_field", "Int", false)}},
	})))
	def := env.p.ForName(env.ctx, "PgMutableType")
	require.NotNil(t, def)
	assert.Equal(t, "v2", def.Description)
	require.Len(t, def.Fields, 1)
	assert.Equal(t, "new_field", def.Fields[0].Name)

	// @if_not_exists skips existing
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "PgMutableType", Description: "overwritten",
			Directives: ast.DirectiveList{catalogDir("cat1"), ifNotExistsDir()},
			Fields:     ast.FieldList{field("y", "Int", false)}},
	})))
	def = env.p.ForName(env.ctx, "PgMutableType")
	require.NotNil(t, def)
	assert.Equal(t, "v2", def.Description) // unchanged

	// @if_not_exists creates brand new
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "PgBrandNew", Description: "created",
			Directives: ast.DirectiveList{catalogDir("cat1"), ifNotExistsDir()},
			Fields:     ast.FieldList{field("z", "String", false)}},
	})))
	assert.NotNil(t, env.p.ForName(env.ctx, "PgBrandNew"))
}

func TestPostgres_ExtensionFields(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	// Persist base type
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgBase", "cat1", "", []*ast.FieldDefinition{
			field("id", "Int", true),
			field("remove_me", "String", false),
			fieldWithDesc("mutable", "String", false, "old desc", nil),
		}),
	})))

	// Add field via extension
	extSource := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "PgBase", Fields: ast.FieldList{
			{Name: "extra", Type: ast.NamedType("String", nil), Description: "added field",
				Directives: ast.DirectiveList{depDir("ext_catalog")}},
		}},
	})
	require.NoError(t, env.p.Update(env.ctx, extSource))

	def := env.p.ForName(env.ctx, "PgBase")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 4)
	extra := def.Fields.ForName("extra")
	require.NotNil(t, extra)
	assert.Equal(t, "added field", extra.Description)

	// Drop field via extension
	extSource2 := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "PgBase", Fields: ast.FieldList{
			{Name: "remove_me", Type: ast.NamedType("String", nil), Directives: ast.DirectiveList{dropDir()}},
		}},
	})
	require.NoError(t, env.p.Update(env.ctx, extSource2))
	def = env.p.ForName(env.ctx, "PgBase")
	require.NotNil(t, def)
	assert.Nil(t, def.Fields.ForName("remove_me"))

	// Replace field via extension
	extSource3 := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "PgBase", Fields: ast.FieldList{
			{Name: "mutable", Type: ast.NamedType("Int", nil), Description: "new desc",
				Directives: ast.DirectiveList{replaceDir()}},
		}},
	})
	require.NoError(t, env.p.Update(env.ctx, extSource3))
	def = env.p.ForName(env.ctx, "PgBase")
	require.NotNil(t, def)
	f := def.Fields.ForName("mutable")
	require.NotNil(t, f)
	assert.Equal(t, "new desc", f.Description)
	assert.Equal(t, "Int", f.Type.NamedType)
}

func TestPostgres_DirectiveDefinitions(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSourceWithDirs(nil, map[string]*ast.DirectiveDefinition{
		"cache": {
			Name:         "cache",
			Description:  "Cache directive",
			IsRepeatable: false,
			Locations:    []ast.DirectiveLocation{ast.LocationField, ast.LocationObject},
		},
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	dir := env.p.DirectiveForName(env.ctx, "cache")
	require.NotNil(t, dir)
	assert.Equal(t, "cache", dir.Name)
	assert.Equal(t, "Cache directive", dir.Description)
	assert.False(t, dir.IsRepeatable)
	assert.Len(t, dir.Locations, 2)
}

func TestPostgres_QueryAndMutationType(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: base.QueryBaseName,
			Directives: ast.DirectiveList{catalogDir("core")},
			Fields:     ast.FieldList{field("health", "String", false)}},
		{Kind: ast.Object, Name: base.MutationBaseName,
			Directives: ast.DirectiveList{catalogDir("core")},
			Fields:     ast.FieldList{field("noop", "String", false)}},
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	qt := env.p.QueryType(env.ctx)
	require.NotNil(t, qt)
	assert.Equal(t, base.QueryBaseName, qt.Name)

	mt := env.p.MutationType(env.ctx)
	require.NotNil(t, mt)
	assert.Equal(t, base.MutationBaseName, mt.Name)

	assert.Nil(t, env.p.SubscriptionType(env.ctx))
}

func TestPostgres_TypeWithoutCatalog(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Scalar, Name: "PgJSON"},
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	def := env.p.ForName(env.ctx, "PgJSON")
	require.NotNil(t, def)
	assert.Equal(t, ast.Scalar, def.Kind)
}

func TestPostgres_ProviderWithEmbeddings(t *testing.T) {
	vecSize := 128
	emb := &mockEmbedder{vec: makeVector(vecSize)}
	env := newPGEnv(t, vecSize, emb)

	source := newTestSource([]*ast.Definition{
		objectType("PgEmbType", "emb_cat", "Embeddable type", []*ast.FieldDefinition{
			fieldWithDesc("val", "String", false, "A field", nil),
		}),
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	// Embeddings computed for type and field
	assert.GreaterOrEqual(t, emb.callCount, 2)

	// SetDefinitionDescription recomputes embedding
	oldCount := emb.callCount
	require.NoError(t, env.p.SetDefinitionDescription(env.ctx, "PgEmbType", "new desc", "long desc"))
	assert.Greater(t, emb.callCount, oldCount)

	// Embedding uses long_description when available
	lastInput := emb.inputs[len(emb.inputs)-1]
	assert.Equal(t, "long desc", lastInput)

	def := env.p.ForName(env.ctx, "PgEmbType")
	require.NotNil(t, def)
	assert.Equal(t, "new desc", def.Description)
}

func TestPostgres_ProviderWithoutEmbeddings(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		objectType("PgNoEmbType", "no_emb_cat", "Type without embeddings", []*ast.FieldDefinition{
			field("id", "Int", true),
		}),
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	def := env.p.ForName(env.ctx, "PgNoEmbType")
	require.NotNil(t, def)
	assert.Equal(t, "Type without embeddings", def.Description)
}

func TestPostgres_SummarizedDescriptionPreserved(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		objectType("PgSummarized", "sum_cat", "original", []*ast.FieldDefinition{
			fieldWithDesc("x", "Int", true, "field initial", nil),
		}),
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	// Manually mark as summarized with enriched descriptions in PG directly
	_, err := env.pgConn.Exec(`UPDATE _schema_types SET is_summarized = true, description = 'enriched' WHERE name = 'PgSummarized'`)
	require.NoError(t, err)
	_, err = env.pgConn.Exec(`UPDATE _schema_fields SET is_summarized = true, description = 'enriched field' WHERE type_name = 'PgSummarized' AND name = 'x'`)
	require.NoError(t, err)

	// Re-update same catalog
	env.p.InvalidateAll()
	require.NoError(t, env.p.Update(env.ctx, source))

	def := env.p.ForName(env.ctx, "PgSummarized")
	require.NotNil(t, def)
	assert.Equal(t, "enriched", def.Description, "summarized type description should be preserved")
	f := def.Fields.ForName("x")
	require.NotNil(t, f)
	assert.Equal(t, "enriched field", f.Description, "summarized field description should be preserved")
}

func TestPostgres_DisabledCatalog(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		objectType("PgDisType", "dis_cat", "Disabled type", []*ast.FieldDefinition{
			field("id", "Int", true),
		}),
	})
	require.NoError(t, env.p.Update(env.ctx, source))
	require.NotNil(t, env.p.ForName(env.ctx, "PgDisType"))

	require.NoError(t, env.p.SetCatalogDisabled(env.ctx, "dis_cat", true))
	assert.Nil(t, env.p.ForName(env.ctx, "PgDisType"), "disabled catalog should hide types")
}

func TestPostgres_DisabledCatalogFieldFiltering(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	// Persist base type
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgFilterBase", "base_cat", "", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// Create ext_cat in PG and add extension field
	_, err := env.pgConn.Exec(`INSERT INTO _schema_catalogs (name) VALUES ('ext_cat') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)

	extSource := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "PgFilterBase", Fields: ast.FieldList{
			{Name: "ext_field", Type: ast.NamedType("String", nil), Description: "from ext",
				Directives: ast.DirectiveList{depDir("ext_cat")}},
		}},
	})
	require.NoError(t, env.p.Update(env.ctx, extSource))

	// Both fields visible
	env.p.InvalidateCatalog("base_cat")
	def := env.p.ForName(env.ctx, "PgFilterBase")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 2)

	// Disable ext_cat in PG directly
	_, err = env.pgConn.Exec(`UPDATE _schema_catalogs SET disabled = true WHERE name = 'ext_cat'`)
	require.NoError(t, err)
	env.p.InvalidateCatalog("base_cat")

	def = env.p.ForName(env.ctx, "PgFilterBase")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 1)
	assert.Equal(t, "id", def.Fields[0].Name)
}

func TestPostgres_SetDefinitionDescription(t *testing.T) {
	vecSize := 128
	emb := &mockEmbedder{vec: makeVector(vecSize)}
	env := newPGEnv(t, vecSize, emb)

	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgUpdatable", "cat1", "original", []*ast.FieldDefinition{field("x", "String", false)}),
	})))

	callsBefore := emb.callCount
	require.NoError(t, env.p.SetDefinitionDescription(env.ctx, "PgUpdatable", "new enriched description", ""))
	assert.Greater(t, emb.callCount, callsBefore)

	env.p.InvalidateAll()
	def := env.p.ForName(env.ctx, "PgUpdatable")
	require.NotNil(t, def)
	assert.Equal(t, "new enriched description", def.Description)

	// Verify in PG directly
	var desc string
	err := env.pgConn.QueryRow(`SELECT description FROM _schema_types WHERE name = 'PgUpdatable'`).Scan(&desc)
	require.NoError(t, err)
	assert.Equal(t, "new enriched description", desc)

	// Re-persist should NOT overwrite (is_summarized=true)
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgUpdatable", "cat1", "overwrite attempt", []*ast.FieldDefinition{field("x", "String", false)}),
	})))
	env.p.InvalidateAll()
	def = env.p.ForName(env.ctx, "PgUpdatable")
	require.NotNil(t, def)
	assert.Equal(t, "new enriched description", def.Description) // preserved
}

func TestPostgres_SetFieldDescription(t *testing.T) {
	vecSize := 128
	emb := &mockEmbedder{vec: makeVector(vecSize)}
	env := newPGEnv(t, vecSize, emb)

	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgMyType", "cat1", "a type", []*ast.FieldDefinition{
			fieldWithDesc("myfield", "String", false, "original field desc", nil),
		}),
	})))

	callsBefore := emb.callCount
	require.NoError(t, env.p.SetFieldDescription(env.ctx, "PgMyType", "myfield", "new field desc", "long field description"))
	assert.Greater(t, emb.callCount, callsBefore)

	env.p.InvalidateAll()
	def := env.p.ForName(env.ctx, "PgMyType")
	require.NotNil(t, def)
	f := def.Fields.ForName("myfield")
	require.NotNil(t, f)
	assert.Equal(t, "new field desc", f.Description)

	// Verify in PG directly
	var desc string
	err := env.pgConn.QueryRow(`SELECT description FROM _schema_fields WHERE type_name = 'PgMyType' AND name = 'myfield'`).Scan(&desc)
	require.NoError(t, err)
	assert.Equal(t, "new field desc", desc)

	// Re-persist should NOT overwrite (is_summarized=true)
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgMyType", "cat1", "a type", []*ast.FieldDefinition{
			fieldWithDesc("myfield", "String", false, "overwrite attempt", nil),
		}),
	})))
	env.p.InvalidateAll()
	def = env.p.ForName(env.ctx, "PgMyType")
	require.NotNil(t, def)
	f = def.Fields.ForName("myfield")
	require.NotNil(t, f)
	assert.Equal(t, "new field desc", f.Description) // preserved
}

func TestPostgres_SetCatalogDescription(t *testing.T) {
	vecSize := 128
	emb := &mockEmbedder{vec: makeVector(vecSize)}
	env := newPGEnv(t, vecSize, emb)

	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgX", "desc_cat", "", []*ast.FieldDefinition{field("x", "String", false)}),
	})))

	callsBefore := emb.callCount
	require.NoError(t, env.p.SetCatalogDescription(env.ctx, "desc_cat", "catalog desc", "long catalog desc"))
	assert.Greater(t, emb.callCount, callsBefore)

	// Verify in PG directly
	var desc, longDesc string
	err := env.pgConn.QueryRow(`SELECT description, long_description FROM _schema_catalogs WHERE name = 'desc_cat'`).Scan(&desc, &longDesc)
	require.NoError(t, err)
	assert.Equal(t, "catalog desc", desc)
	assert.Equal(t, "long catalog desc", longDesc)
}

func TestPostgres_SetModuleDescription(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	// Insert a module record in PG
	_, err := env.pgConn.Exec(`INSERT INTO _schema_modules (name) VALUES ('pgtestmod')`)
	require.NoError(t, err)

	require.NoError(t, env.p.SetModuleDescription(env.ctx, "pgtestmod", "module desc", "long module desc"))

	var desc, longDesc string
	err = env.pgConn.QueryRow(`SELECT description, long_description FROM _schema_modules WHERE name = 'pgtestmod'`).Scan(&desc, &longDesc)
	require.NoError(t, err)
	assert.Equal(t, "module desc", desc)
	assert.Equal(t, "long module desc", longDesc)
}

func TestPostgres_DropCatalogDetailedCleanup(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "PgDroppedType", Description: "to be dropped",
			Directives: ast.DirectiveList{catalogDir("drop_cat")},
			Fields: ast.FieldList{
				field("f1", "String", false),
				fieldWithDesc("f2", "Int", false, "", &ast.ArgumentDefinitionList{
					{Name: "arg1", Type: ast.NamedType("String", nil)},
				}),
			}},
		{Kind: ast.Enum, Name: "PgDroppedEnum", Directives: ast.DirectiveList{catalogDir("drop_cat")},
			EnumValues: ast.EnumValueList{{Name: "A"}, {Name: "B"}}},
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	require.NotNil(t, env.p.ForName(env.ctx, "PgDroppedType"))
	require.NotNil(t, env.p.ForName(env.ctx, "PgDroppedEnum"))

	require.NoError(t, env.p.DropCatalog(env.ctx, "drop_cat", false))
	env.p.InvalidateAll()
	assert.Nil(t, env.p.ForName(env.ctx, "PgDroppedType"))
	assert.Nil(t, env.p.ForName(env.ctx, "PgDroppedEnum"))

	// Verify DB is clean
	assert.Equal(t, 0, env.pgCount(t, `SELECT count(*) FROM _schema_types WHERE catalog = 'drop_cat'`))
	assert.Equal(t, 0, env.pgCount(t, `SELECT count(*) FROM _schema_fields WHERE type_name = 'PgDroppedType'`))
	assert.Equal(t, 0, env.pgCount(t, `SELECT count(*) FROM _schema_arguments WHERE type_name = 'PgDroppedType'`))
	assert.Equal(t, 0, env.pgCount(t, `SELECT count(*) FROM _schema_enum_values WHERE type_name = 'PgDroppedEnum'`))
}

func TestPostgres_DropCatalogCascade(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgBaseType", "base_cat", "Base type", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgDepType", "dep_cat", "Dependent type", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// Create dependency in PG
	_, err := env.pgConn.Exec(`INSERT INTO _schema_catalog_dependencies (catalog_name, depends_on) VALUES ('dep_cat', 'base_cat')`)
	require.NoError(t, err)

	// Drop base catalog with cascade — should suspend dep_cat
	require.NoError(t, env.p.DropCatalog(env.ctx, "base_cat", true))
	assert.Nil(t, env.p.ForName(env.ctx, "PgBaseType"), "PgBaseType should be gone")

	cat, err := env.p.GetCatalog(env.ctx, "dep_cat")
	require.NoError(t, err)
	require.NotNil(t, cat)
	assert.True(t, cat.Suspended, "dependent catalog should be suspended")
}

func TestPostgres_DropCatalogCleansExtensionFields(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	// Persist base type
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgHost", "host_cat", "", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// Create dep_cat in PG and add extension field to PgHost
	_, err := env.pgConn.Exec(`INSERT INTO _schema_catalogs (name) VALUES ('dep_cat') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)

	extSource := newTestExtSource(nil, []*ast.Definition{
		{Kind: ast.Object, Name: "PgHost", Fields: ast.FieldList{
			{Name: "dep_field", Type: ast.NamedType("String", nil),
				Directives: ast.DirectiveList{depDir("dep_cat")}},
		}},
	})
	require.NoError(t, env.p.Update(env.ctx, extSource))

	env.p.InvalidateAll()
	def := env.p.ForName(env.ctx, "PgHost")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 2)

	// Drop dep_cat — extension field should be removed
	require.NoError(t, env.p.DropCatalog(env.ctx, "dep_cat", false))
	env.p.InvalidateAll()
	def = env.p.ForName(env.ctx, "PgHost")
	require.NotNil(t, def)
	require.Len(t, def.Fields, 1)
	assert.Equal(t, "id", def.Fields[0].Name)
}

func TestPostgres_ReconcileModules(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	source := newTestSource([]*ast.Definition{
		{Kind: ast.Object, Name: "pg_geo_Query",
			Directives: ast.DirectiveList{
				catalogDir("geo_cat"),
				{Name: base.ModuleRootDirectiveName},
				{Name: base.ModuleDirectiveName, Arguments: ast.ArgumentList{
					{Name: base.ArgName, Value: &ast.Value{Raw: "pg_geo", Kind: ast.StringValue}},
				}},
			},
			Fields: ast.FieldList{field("x", "String", false)},
		},
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	assert.Equal(t, 1, env.pgCount(t, `SELECT count(*) FROM _schema_modules WHERE name = 'pg_geo'`))
	assert.Equal(t, 1, env.pgCount(t, `SELECT count(*) FROM _schema_module_type_catalogs WHERE module_name = 'pg_geo' AND catalog_name = 'geo_cat'`))
}

func TestPostgres_CatalogGetters(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgGetterType", "pg_getter_cat", "test", []*ast.FieldDefinition{field("id", "Int", true)}),
	})))

	// GetCatalog
	cat, err := env.p.GetCatalog(env.ctx, "pg_getter_cat")
	require.NoError(t, err)
	require.NotNil(t, cat)
	assert.Equal(t, "pg_getter_cat", cat.Name)
	assert.False(t, cat.Disabled)
	assert.False(t, cat.Suspended)

	// SetCatalogVersion
	require.NoError(t, env.p.SetCatalogVersion(env.ctx, "pg_getter_cat", "3.0"))
	cat, err = env.p.GetCatalog(env.ctx, "pg_getter_cat")
	require.NoError(t, err)
	assert.Equal(t, "3.0", cat.Version)

	// Verify in PG
	var version string
	err = env.pgConn.QueryRow(`SELECT version FROM _schema_catalogs WHERE name = 'pg_getter_cat'`).Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, "3.0", version)

	// SetCatalogDisabled
	require.NoError(t, env.p.SetCatalogDisabled(env.ctx, "pg_getter_cat", true))
	cat, err = env.p.GetCatalog(env.ctx, "pg_getter_cat")
	require.NoError(t, err)
	assert.True(t, cat.Disabled)
	assert.Nil(t, env.p.ForName(env.ctx, "PgGetterType"), "disabled catalog should hide types")

	// SetCatalogSuspended
	require.NoError(t, env.p.SetCatalogSuspended(env.ctx, "pg_getter_cat", true))
	cat, err = env.p.GetCatalog(env.ctx, "pg_getter_cat")
	require.NoError(t, err)
	assert.True(t, cat.Suspended)

	// ListCatalogs
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgGetterType2", "another_cat", "", []*ast.FieldDefinition{field("y", "String", false)}),
	})))
	catalogs, err := env.p.ListCatalogs(env.ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(catalogs), 2)

	names := make([]string, len(catalogs))
	for i, c := range catalogs {
		names[i] = c.Name
	}
	assert.Contains(t, names, "pg_getter_cat")
	assert.Contains(t, names, "another_cat")

	// Non-existent catalog
	cat, err = env.p.GetCatalog(env.ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, cat)
}

func TestPostgres_SpecialCharacters(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	// Test with special characters in descriptions that could break SQL inlining
	source := newTestSource([]*ast.Definition{
		objectType("PgSpecialType", "special_cat", "It's a \"special\" type with 'quotes'", []*ast.FieldDefinition{
			field("id", "Int", true),
		}),
	})
	require.NoError(t, env.p.Update(env.ctx, source))

	def := env.p.ForName(env.ctx, "PgSpecialType")
	require.NotNil(t, def)
	assert.Equal(t, "It's a \"special\" type with 'quotes'", def.Description)

	// SetDefinitionDescription with special chars
	require.NoError(t, env.p.SetDefinitionDescription(env.ctx, "PgSpecialType", "O'Brien's \"new\" desc", ""))
	def = env.p.ForName(env.ctx, "PgSpecialType")
	require.NotNil(t, def)
	assert.Equal(t, "O'Brien's \"new\" desc", def.Description)
}

func TestPostgres_ExecWriteOperations(t *testing.T) {
	env := newPGEnv(t, 0, nil)

	// Test INSERT/UPDATE/DELETE through the provider's public API
	require.NoError(t, env.p.Update(env.ctx, newTestSource([]*ast.Definition{
		objectType("PgWriteTest", "write_cat", "v1", []*ast.FieldDefinition{field("x", "String", false)}),
	})))

	def := env.p.ForName(env.ctx, "PgWriteTest")
	require.NotNil(t, def)
	assert.Equal(t, "v1", def.Description)

	// Update via SetDefinitionDescription
	require.NoError(t, env.p.SetDefinitionDescription(env.ctx, "PgWriteTest", "v2", ""))
	env.p.InvalidateAll()
	def = env.p.ForName(env.ctx, "PgWriteTest")
	require.NotNil(t, def)
	assert.Equal(t, "v2", def.Description)

	// Delete via DropCatalog
	require.NoError(t, env.p.DropCatalog(env.ctx, "write_cat", false))
	assert.Nil(t, env.p.ForName(env.ctx, "PgWriteTest"))
}

// ─── Infrastructure helpers ─────────────────────────────────────────────────

func newDuckDBPool(t *testing.T) *db.Pool {
	t.Helper()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	return pool
}

func initDuckDBSchema(t *testing.T, pool *db.Pool, vecSize int) {
	t.Helper()
	if vecSize == 0 {
		vecSize = 128 // use small default for tests
	}
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: vecSize,
	})
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), sqlStr)
	require.NoError(t, err)

	// Seed basic system scalar types so Update() validation passes
	// when test definitions reference Int, String, Float, Boolean, etc.
	seedSystemTypes(t, pool, vecSize)
}

// seedSystemTypesWithPrefix seeds system types into tables with a given prefix.
func seedSystemTypesWithPrefix(t *testing.T, pool *db.Pool, prefix string, vecSize int) {
	t.Helper()
	ctx := context.Background()
	scalars := []string{
		"Int", "Float", "String", "Boolean", "ID",
		"BigInt", "Timestamp", "Date", "Time",
		"JSON", "Geometry", "Vector", "Blob",
	}
	for _, s := range scalars {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s_schema_types (name, kind, hugr_type, description, long_description, module, catalog, interfaces, union_types, directives, vec)
			 VALUES ('%s', 'SCALAR', 'Scalar', '%s scalar', '', '', '_system', '', '', '[]', NULL)
			 ON CONFLICT (name) DO NOTHING`, prefix, s, s,
		))
		require.NoError(t, err)
	}
	for _, root := range []string{"Query", "Mutation"} {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s_schema_types (name, kind, hugr_type, description, long_description, module, catalog, interfaces, union_types, directives, vec)
			 VALUES ('%s', 'OBJECT', 'Object', '%s root type', '', '', '_system', '', '', '[]', NULL)
			 ON CONFLICT (name) DO NOTHING`, prefix, root, root,
		))
		require.NoError(t, err)
	}
}

// seedSystemTypes inserts basic scalar types into _schema_types so that
// validation in Update() can resolve references like "Int", "String", etc.
func seedSystemTypes(t *testing.T, pool *db.Pool, vecSize int) {
	t.Helper()
	ctx := context.Background()
	scalars := []string{
		"Int", "Float", "String", "Boolean", "ID",
		"BigInt", "Timestamp", "Date", "Time",
		"JSON", "Geometry", "Vector", "Blob",
	}
	vecPlaceholder := "NULL"
	if vecSize > 0 {
		vecPlaceholder = "NULL"
	}
	for _, s := range scalars {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO _schema_types (name, kind, hugr_type, description, long_description, module, catalog, interfaces, union_types, directives, vec)
			 VALUES ('%s', 'SCALAR', 'Scalar', '%s scalar', '', '', '_system', '', '', '[]', %s)
			 ON CONFLICT (name) DO NOTHING`, s, s, vecPlaceholder,
		))
		require.NoError(t, err)
	}

	// Also seed Query and Mutation root types.
	for _, root := range []string{"Query", "Mutation"} {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO _schema_types (name, kind, hugr_type, description, long_description, module, catalog, interfaces, union_types, directives, vec)
			 VALUES ('%s', 'OBJECT', 'Object', '%s root type', '', '', '_system', '', '', '[]', NULL)
			 ON CONFLICT (name) DO NOTHING`, root, root,
		))
		require.NoError(t, err)
	}
}

func pgDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DBPROVIDER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("DBPROVIDER_TEST_PG_DSN not set, skipping PostgreSQL tests")
	}
	return dsn
}

func openPG(t *testing.T) *sql.DB {
	t.Helper()
	dsn := pgDSN(t)
	conn, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	err = conn.Ping()
	require.NoError(t, err, "cannot connect to PostgreSQL at %s", dsn)
	return conn
}

func cleanPG(t *testing.T, conn *sql.DB) {
	t.Helper()
	tables := []string{
		"_schema_data_object_queries",
		"_schema_data_objects",
		"_schema_module_type_catalogs",
		"_schema_modules",
		"_schema_directives",
		"_schema_enum_values",
		"_schema_arguments",
		"_schema_fields",
		"_schema_types",
		"_schema_catalog_dependencies",
		"_schema_catalogs",
		"_schema_settings",
		"permissions",
		"api_keys",
		"data_source_catalogs",
		"data_sources",
		"catalog_sources",
		"roles",
		"version",
	}
	for _, table := range tables {
		_, _ = conn.Exec(`DROP TABLE IF EXISTS "` + table + `" CASCADE;`)
	}
}

func initPGSchema(t *testing.T, conn *sql.DB) {
	t.Helper()
	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS vector;")
	require.NoError(t, err)

	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBPostgres, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 128,
	})
	require.NoError(t, err)

	_, err = conn.Exec(sqlStr)
	require.NoError(t, err)
}

func attachPG(t *testing.T, pool *db.Pool, ctx context.Context) {
	t.Helper()
	dsn := pgDSN(t)
	_, err := pool.Exec(ctx, fmt.Sprintf(
		`ATTACH '%s' AS core (TYPE postgres);`, dsn,
	))
	require.NoError(t, err, "failed to attach PostgreSQL to DuckDB")
}

// ─── Test type constructors ─────────────────────────────────────────────────

func objectType(name, catalog, desc string, fields []*ast.FieldDefinition) *ast.Definition {
	fl := ast.FieldList(fields)
	return &ast.Definition{
		Kind:        ast.Object,
		Name:        name,
		Description: desc,
		Directives:  ast.DirectiveList{catalogDir(catalog)},
		Fields:      fl,
	}
}

func field(name, typeName string, nonNull bool) *ast.FieldDefinition {
	t := &ast.Type{NamedType: typeName}
	if nonNull {
		t.NonNull = true
	}
	return &ast.FieldDefinition{Name: name, Type: t}
}

func fieldWithDesc(name, typeName string, nonNull bool, desc string, args *ast.ArgumentDefinitionList) *ast.FieldDefinition {
	f := field(name, typeName, nonNull)
	f.Description = desc
	if args != nil {
		f.Arguments = *args
	}
	return f
}

// ─── Directive constructors ─────────────────────────────────────────────────

func catalogDir(name string) *ast.Directive {
	return &ast.Directive{
		Name: base.CatalogDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: name}},
		},
	}
}

func dropDir() *ast.Directive {
	return &ast.Directive{Name: base.DropDirectiveName}
}

func replaceDir() *ast.Directive {
	return &ast.Directive{Name: base.ReplaceDirectiveName}
}

func ifNotExistsDir() *ast.Directive {
	return &ast.Directive{Name: base.IfNotExistsDirectiveName}
}

func depDir(name string) *ast.Directive {
	return &ast.Directive{
		Name: base.DependencyDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: name}},
		},
	}
}

// ─── Test source types ──────────────────────────────────────────────────────

// testSource implements base.DefinitionsSource.
type testSource struct {
	defs []*ast.Definition
	dirs map[string]*ast.DirectiveDefinition
}

func newTestSource(defs []*ast.Definition) *testSource {
	return &testSource{defs: defs}
}

func newTestSourceWithDirs(defs []*ast.Definition, dirs map[string]*ast.DirectiveDefinition) *testSource {
	return &testSource{defs: defs, dirs: dirs}
}

func (s *testSource) ForName(_ context.Context, name string) *ast.Definition {
	for _, d := range s.defs {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (s *testSource) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	if s.dirs == nil {
		return nil
	}
	return s.dirs[name]
}

func (s *testSource) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.defs {
			if !yield(d) {
				return
			}
		}
	}
}

func (s *testSource) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {
		for name, dir := range s.dirs {
			if !yield(name, dir) {
				return
			}
		}
	}
}

// testExtSource implements both base.DefinitionsSource and base.ExtensionsSource.
type testExtSource struct {
	testSource
	exts []*ast.Definition
}

func newTestExtSource(defs []*ast.Definition, exts []*ast.Definition) *testExtSource {
	return &testExtSource{
		testSource: testSource{defs: defs},
		exts:       exts,
	}
}

func (s *testExtSource) DefinitionExtensions(_ context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, ext := range s.exts {
			if ext.Name == name {
				if !yield(ext) {
					return
				}
			}
		}
	}
}

func (s *testExtSource) Extensions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, ext := range s.exts {
			if !yield(ext) {
				return
			}
		}
	}
}

// ─── Mock embedder ──────────────────────────────────────────────────────────

type mockEmbedder struct {
	vec       ctypes.Vector
	inputs    []string
	callCount int
}

func (m *mockEmbedder) CreateEmbedding(_ context.Context, input string) (ctypes.Vector, error) {
	m.inputs = append(m.inputs, input)
	m.callCount++
	return m.vec, nil
}

func (m *mockEmbedder) CreateEmbeddings(_ context.Context, inputs []string) ([]ctypes.Vector, error) {
	vecs := make([]ctypes.Vector, len(inputs))
	for i, input := range inputs {
		m.inputs = append(m.inputs, input)
		m.callCount++
		vecs[i] = m.vec
	}
	return vecs, nil
}

func makeVector(size int) ctypes.Vector {
	v := make(ctypes.Vector, size)
	for i := range v {
		v[i] = float64(i) * 0.01
	}
	return v
}

// ─── Real Embedder Tests (requires running embedding server) ─────────────────

func TestDuckDB_RealEmbedder(t *testing.T) {
	embedderURL := os.Getenv("EMBEDDER_URL")
	if embedderURL == "" {
		t.Skip("EMBEDDER_URL not set, skipping real embedder test")
	}

	pool := newDuckDBPool(t)
	vecSize := 768
	initDuckDBSchema(t, pool, vecSize)

	ctx := t.Context()

	// Create embedding source from URL (same path as engine.go Init step 3b).
	src, err := embedding.New(types.DataSource{
		Name: "_system_embedder",
		Type: sources.Embedding,
		Path: embedderURL,
	}, false)
	require.NoError(t, err)

	err = src.Attach(ctx, pool)
	require.NoError(t, err)

	// Verify embedding source works directly.
	vec, err := src.CreateEmbedding(ctx, "hello world")
	require.NoError(t, err)
	assert.Equal(t, vecSize, len(vec), "expected %d-dim vector", vecSize)

	// Create DB provider with real embedder.
	p, err := dbprovider.New(ctx, pool, dbprovider.Config{
		Cache:   dbprovider.DefaultCacheConfig(),
		VecSize: vecSize,
	}, src)
	require.NoError(t, err)

	// Persist a type — should compute embeddings via real server.
	source := newTestSource([]*ast.Definition{
		objectType("RealEmbType", "real_emb", "A type with real embeddings", []*ast.FieldDefinition{
			fieldWithDesc("name", "String", false, "The name field", nil),
		}),
	})
	require.NoError(t, p.Update(ctx, source))

	// Verify the type is stored and readable.
	def := p.ForName(ctx, "RealEmbType")
	require.NotNil(t, def)
	assert.Equal(t, "A type with real embeddings", def.Description)

	// Update description — should recompute embedding via real server.
	err = p.SetDefinitionDescription(ctx, "RealEmbType", "updated desc", "long updated description")
	require.NoError(t, err)

	def = p.ForName(ctx, "RealEmbType")
	require.NotNil(t, def)
	assert.Equal(t, "updated desc", def.Description)
}

// ─── Helpers ────────────────────────────────────────────────────────────────

func collectPossibleTypes(p *dbprovider.Provider, ctx context.Context, name string) []string {
	var names []string
	for d := range p.PossibleTypes(ctx, name) {
		names = append(names, d.Name)
	}
	return names
}
