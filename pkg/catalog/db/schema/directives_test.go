package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestMarshalDirectives_NoArgs(t *testing.T) {
	dirs := ast.DirectiveList{{Name: "pk"}}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"name":"pk"}]`, string(data))
}

func TestMarshalDirectives_StringArg(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "table", Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: "users", Kind: ast.StringValue}},
		}},
	}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"name":"table","args":{"name":"users"}}]`, string(data))
}

func TestMarshalDirectives_MixedArgTypes(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "test", Arguments: ast.ArgumentList{
			{Name: "s", Value: &ast.Value{Raw: "hello", Kind: ast.StringValue}},
			{Name: "i", Value: &ast.Value{Raw: "42", Kind: ast.IntValue}},
			{Name: "f", Value: &ast.Value{Raw: "3.14", Kind: ast.FloatValue}},
			{Name: "b", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue}},
			{Name: "e", Value: &ast.Value{Raw: "ACTIVE", Kind: ast.EnumValue}},
		}},
	}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "test", got[0].Name)

	// Check all argument types round-trip correctly.
	argByName := func(name string) *ast.Value {
		for _, a := range got[0].Arguments {
			if a.Name == name {
				return a.Value
			}
		}
		return nil
	}
	assert.Equal(t, ast.StringValue, argByName("s").Kind)
	assert.Equal(t, "hello", argByName("s").Raw)
	assert.Equal(t, ast.IntValue, argByName("i").Kind)
	assert.Equal(t, "42", argByName("i").Raw)
	assert.Equal(t, ast.FloatValue, argByName("f").Kind)
	assert.Equal(t, "3.14", argByName("f").Raw)
	assert.Equal(t, ast.BooleanValue, argByName("b").Kind)
	assert.Equal(t, "true", argByName("b").Raw)
	assert.Equal(t, ast.EnumValue, argByName("e").Kind)
	assert.Equal(t, "ACTIVE", argByName("e").Raw)
}

func TestMarshalDirectives_ListArg(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "references", Arguments: ast.ArgumentList{
			{Name: "source_fields", Value: &ast.Value{
				Kind: ast.ListValue,
				Children: ast.ChildValueList{
					{Value: &ast.Value{Raw: "a", Kind: ast.StringValue}},
					{Value: &ast.Value{Raw: "b", Kind: ast.StringValue}},
				},
			}},
		}},
	}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Arguments, 1)

	val := got[0].Arguments[0].Value
	assert.Equal(t, ast.ListValue, val.Kind)
	require.Len(t, val.Children, 2)
	assert.Equal(t, "a", val.Children[0].Value.Raw)
	assert.Equal(t, "b", val.Children[1].Value.Raw)
}

func TestMarshalDirectives_ObjectArg(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "config", Arguments: ast.ArgumentList{
			{Name: "opts", Value: &ast.Value{
				Kind: ast.ObjectValue,
				Children: ast.ChildValueList{
					{Name: "key", Value: &ast.Value{Raw: "val", Kind: ast.StringValue}},
					{Name: "num", Value: &ast.Value{Raw: "10", Kind: ast.IntValue}},
				},
			}},
		}},
	}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	require.Len(t, got, 1)

	val := got[0].Arguments.ForName("opts").Value
	assert.Equal(t, ast.ObjectValue, val.Kind)
	require.Len(t, val.Children, 2)
}

func TestMarshalDirectives_NullValue(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "default", Arguments: ast.ArgumentList{
			{Name: "value", Value: &ast.Value{Kind: ast.NullValue, Raw: "null"}},
		}},
	}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, ast.NullValue, got[0].Arguments[0].Value.Kind)
}

func TestMarshalDirectives_Multiple(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "table", Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: "users", Kind: ast.StringValue}},
		}},
		{Name: "pk"},
	}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "table", got[0].Name)
	assert.Equal(t, "pk", got[1].Name)
}

func TestMarshalDirectives_Deterministic(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "field_references", Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: "ref1", Kind: ast.StringValue}},
			{Name: "field", Value: &ast.Value{Raw: "id", Kind: ast.StringValue}},
			{Name: "references_name", Value: &ast.Value{Raw: "other", Kind: ast.StringValue}},
		}},
	}
	data1, err := MarshalDirectives(dirs)
	require.NoError(t, err)
	data2, err := MarshalDirectives(dirs)
	require.NoError(t, err)
	assert.Equal(t, data1, data2, "output should be byte-identical on repeated serialization")
}

func TestDirectiveRoundTrip_Comprehensive(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "table", Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: "users", Kind: ast.StringValue}},
		}},
		{Name: "pk"},
		{Name: "field_references", Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: "users_dept_id", Kind: ast.StringValue}},
			{Name: "field", Value: &ast.Value{Raw: "id", Kind: ast.StringValue}},
			{Name: "references_name", Value: &ast.Value{Raw: "departments", Kind: ast.StringValue}},
		}},
	}

	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	require.Equal(t, len(dirs), len(got))
	for i := range dirs {
		assert.Equal(t, dirs[i].Name, got[i].Name)
		assert.Equal(t, len(dirs[i].Arguments), len(got[i].Arguments))
	}
}

func TestMarshalDirectives_Empty(t *testing.T) {
	data, err := MarshalDirectives(nil)
	require.NoError(t, err)
	assert.Equal(t, "[]", string(data))

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	assert.Len(t, got, 0)
}

func TestMarshalDirectives_NilArgValue(t *testing.T) {
	dirs := ast.DirectiveList{
		{Name: "test", Arguments: ast.ArgumentList{
			{Name: "val", Value: nil},
		}},
	}
	data, err := MarshalDirectives(dirs)
	require.NoError(t, err)

	got, err := UnmarshalDirectives(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, ast.NullValue, got[0].Arguments.ForName("val").Value.Kind)
}
