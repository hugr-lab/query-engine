package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestTypeStringRoundTrip(t *testing.T) {
	cases := []string{
		"String",
		"String!",
		"[String]",
		"[String!]",
		"[String!]!",
		"[String]!",
		"Int",
		"Boolean!",
		"[Float!]!",
		"MyCustomType",
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			typ, err := UnmarshalType(s)
			require.NoError(t, err)
			assert.Equal(t, s, MarshalType(typ))
		})
	}
}

func TestUnmarshalType_NamedNonNull(t *testing.T) {
	typ, err := UnmarshalType("String!")
	require.NoError(t, err)
	assert.Equal(t, "String", typ.NamedType)
	assert.True(t, typ.NonNull)
	assert.Nil(t, typ.Elem)
}

func TestUnmarshalType_ListNonNull(t *testing.T) {
	typ, err := UnmarshalType("[String!]!")
	require.NoError(t, err)
	assert.Empty(t, typ.NamedType)
	assert.True(t, typ.NonNull)
	require.NotNil(t, typ.Elem)
	assert.Equal(t, "String", typ.Elem.NamedType)
	assert.True(t, typ.Elem.NonNull)
}

func TestUnmarshalType_ListNullableInner(t *testing.T) {
	typ, err := UnmarshalType("[String]")
	require.NoError(t, err)
	assert.False(t, typ.NonNull)
	require.NotNil(t, typ.Elem)
	assert.Equal(t, "String", typ.Elem.NamedType)
	assert.False(t, typ.Elem.NonNull)
}

func TestUnmarshalType_Errors(t *testing.T) {
	cases := []struct {
		input string
		desc  string
	}{
		{"", "empty string"},
		{"[String", "missing closing bracket"},
		{"[String!!", "double bang inside list"},
		{"[]", "empty list type"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := UnmarshalType(tc.input)
			assert.Error(t, err)
		})
	}
}

func TestMarshalType_Nil(t *testing.T) {
	assert.Equal(t, "", MarshalType(nil))
}

func TestMarshalType_Simple(t *testing.T) {
	typ := &ast.Type{NamedType: "Int", NonNull: true}
	assert.Equal(t, "Int!", MarshalType(typ))
}
