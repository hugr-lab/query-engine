package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hugr-lab/query-engine/types"
)

func TestSqlLiteral(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
		wantErr  bool
	}{
		{"nil", nil, "NULL", false},
		{"string", "hello", "'hello'", false},
		{"string with quotes", "it's a test", "'it''s a test'", false},
		{"empty string", "", "''", false},
		{"bool true", true, "true", false},
		{"bool false", false, "false", false},
		{"int", 42, "42", false},
		{"int64", int64(1234567890), "1234567890", false},
		{"float32", float32(3.14), "3.14", false},
		{"float64", float64(2.718281828), "2.718281828", false},
		{"nil *string", (*string)(nil), "NULL", false},
		{"*string", strPtr("val"), "'val'", false},
		{"types.Vector", types.Vector{0.1, 0.2, 0.3}, "'[0.1,0.2,0.3]'", false},
		{"types.Vector nil", types.Vector(nil), "NULL", false},
		{"types.Vector empty", types.Vector{}, "'[]'", false},
		{"[]float32 vector", []float32{0.1, 0.2, 0.3}, "'[0.1,0.2,0.3]'", false},
		{"[]float32 empty", []float32{}, "'[]'", false},
		{"unsupported type", struct{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqlLiteral(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func strPtr(s string) *string { return &s }

func TestInlineParams(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []any
		expected string
		wantErr  bool
	}{
		{
			"single param",
			"SELECT * FROM t WHERE name = $1",
			[]any{"test"},
			"SELECT * FROM t WHERE name = 'test'",
			false,
		},
		{
			"multiple params",
			"INSERT INTO t (a, b, c) VALUES ($1, $2, $3)",
			[]any{"hello", 42, true},
			"INSERT INTO t (a, b, c) VALUES ('hello', 42, true)",
			false,
		},
		{
			"params with quotes",
			"UPDATE t SET desc = $1 WHERE name = $2",
			[]any{"it's a test", "O'Brien"},
			"UPDATE t SET desc = 'it''s a test' WHERE name = 'O''Brien'",
			false,
		},
		{
			"param with NULL",
			"INSERT INTO t (a, b) VALUES ($1, $2)",
			[]any{"name", nil},
			"INSERT INTO t (a, b) VALUES ('name', NULL)",
			false,
		},
		{
			"high-index params don't collide",
			"INSERT INTO t VALUES ($1, $10, $11)",
			[]any{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
			"INSERT INTO t VALUES ('a', 'j', 'k')",
			false,
		},
		{
			"[]float32 vector param",
			"UPDATE t SET vec = $1 WHERE id = $2",
			[]any{[]float32{0.1, 0.2}, "test"},
			"UPDATE t SET vec = '[0.1,0.2]' WHERE id = 'test'",
			false,
		},
		{
			"types.Vector param",
			"UPDATE t SET vec = $1 WHERE id = $2",
			[]any{types.Vector{0.1, 0.2}, "test"},
			"UPDATE t SET vec = '[0.1,0.2]' WHERE id = 'test'",
			false,
		},
		{
			"nil types.Vector",
			"UPDATE t SET vec = $1 WHERE id = $2",
			[]any{types.Vector(nil), "test"},
			"UPDATE t SET vec = NULL WHERE id = 'test'",
			false,
		},
		{
			"no params",
			"SELECT 1",
			nil,
			"SELECT 1",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := inlineParams(tt.query, tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPgDatabaseName(t *testing.T) {
	tests := []struct {
		prefix   string
		expected string
	}{
		{"core.", "core"},
		{"mydb.", "mydb"},
		{"", ""},
	}
	for _, tt := range tests {
		p := &Provider{prefix: tt.prefix}
		assert.Equal(t, tt.expected, p.pgDatabaseName())
	}
}

func TestSQLPrefixWorks(t *testing.T) {
	p := &Provider{}
	assert.Equal(t, "_schema_types", p.table("_schema_types"))

	p.prefix = "core."
	assert.Equal(t, "core._schema_types", p.table("_schema_types"))
}

func TestSqlLiteralVector(t *testing.T) {
	// nil Vector → NULL
	lit, err := sqlLiteral(types.Vector(nil))
	require.NoError(t, err)
	assert.Equal(t, "NULL", lit)

	// non-nil Vector → float32 precision literal
	vec := types.Vector{0.0, 0.01, 0.02}
	lit, err = sqlLiteral(vec)
	require.NoError(t, err)
	assert.Contains(t, lit, "[")
	assert.Contains(t, lit, "]")
}
