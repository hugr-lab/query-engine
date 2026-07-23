//go:build duckdb_arrow

package store

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPropertyBagJSON pins the marshaled JSON of the property bags to the
// STRUCT member names in core-db/hugr_catalog.sql. The writer stores exactly
// this text (plain VARCHAR → STRUCT/JSONB cast), so the shapes must match the
// probeCatalogStatements forms byte-for-byte. Absent directives store no key
// (omitempty); an empty bag marshals to "{}" and the writer stores SQL NULL.
func TestPropertyBagJSON(t *testing.T) {
	marshal := func(v any) string {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		return string(b)
	}

	// data_objects.properties — nested struct, list of struct, list of varchar
	// (mirrors probe A1's rich bag).
	do := &dataObjectProperties{
		Name:       "orders",
		SoftDelete: true,
		Unique: []uniqueConstraint{
			{Fields: []string{"name"}, QuerySuffix: "by_name"},
			{Fields: []string{"id", "name"}, SkipQuery: true},
		},
		Cache: &cacheSettings{TTL: "5m", Tags: []string{"a", "b"}},
		At:    &atPin{Version: 42},
	}
	assert.Equal(t,
		`{"name":"orders","soft_delete":true,`+
			`"unique":[{"fields":["name"],"query_suffix":"by_name"},{"fields":["id","name"],"skip_query":true}],`+
			`"cache":{"ttl":"5m","tags":["a","b"]},"at":{"version":42}}`,
		marshal(do))

	// fields.properties — JSON-typed members inside the struct (default.value,
	// function_call.args), mirrors probe A2.
	fp := &fieldProperties{
		Default: &defaultValue{Value: map[string]any{"a": float64(1)}},
		FunctionCall: &functionCallBinding{
			Function:     functionRef{Module: "m", Name: "f"},
			Args:         map[string]any{"id": "$id"},
			SourceFields: []string{"id"},
		},
	}
	assert.Equal(t,
		`{"default":{"value":{"a":1}},`+
			`"function_call":{"function":{"module":"m","name":"f"},"args":{"id":"$id"},"source_fields":["id"]}}`,
		marshal(fp))

	// functions.properties + args.
	fn := &functionProperties{Function: &functionBinding{Name: "f", SkipNullArg: true}}
	assert.Equal(t, `{"function":{"name":"f","skip_null_arg":true}}`, marshal(fn))
	args := []functionArgument{{Name: "id", Type: "Int!", Description: "identifier"}, {Name: "mode", Type: "String"}}
	assert.Equal(t,
		`[{"name":"id","type":"Int!","description":"identifier"},{"name":"mode","type":"String"}]`,
		marshal(args))

	// Step-1 gap members: soft-delete raw expressions, @dim, @unique_rule and
	// argument @deprecated — all pinned to the STRUCT member names.
	assert.Equal(t,
		`{"soft_delete":true,"soft_delete_cond":"deleted_at IS NULL","soft_delete_set":"deleted_at = now()"}`,
		marshal(&dataObjectProperties{SoftDelete: true, SoftDeleteCond: "deleted_at IS NULL", SoftDeleteSet: "deleted_at = now()"}))
	assert.Equal(t, `{"dim":3,"unique_rule":"INTERSECTS"}`,
		marshal(&fieldProperties{Dim: 3, UniqueRule: "INTERSECTS"}))
	assert.Equal(t, `[{"name":"id","type":"Int","deprecation_reason":"use uid"}]`,
		marshal([]functionArgument{{Name: "id", Type: "Int", DeprecationReason: "use uid"}}))

	// Empty bags carry no keys — the writer collapses "{}" to SQL NULL.
	assert.Equal(t, `{}`, marshal(&dataObjectProperties{}))
	assert.Equal(t, `{}`, marshal(&fieldProperties{}))
	assert.Equal(t, `{}`, marshal(&functionProperties{}))
}

// TestPropertyBagSQLMembers pins the typed SQL members (stored AND exposed for
// UI): @view / @function / @sql(exp:) / @join / @function_call /
// @table_function_call_join / @default(insert_exp:/update_exp:).
func TestPropertyBagSQLMembers(t *testing.T) {
	marshal := func(v any) string {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		return string(b)
	}

	// @view(sql:) body on the data object.
	assert.Equal(t, `{"name":"v_orders","sql":"SELECT 1"}`,
		marshal(&dataObjectProperties{Name: "v_orders", SQL: "SELECT 1"}))

	// @function(sql:) body.
	assert.Equal(t, `{"function":{"name":"f","sql":"SELECT 2"}}`,
		marshal(&functionProperties{Function: &functionBinding{Name: "f", SQL: "SELECT 2"}}))

	// @sql(exp:) computed field, @join(sql:), @function_call(sql:),
	// @default(insert_exp:/update_exp:) — all field-level.
	assert.Equal(t, `{"computed":true,"sql":"a + b"}`,
		marshal(&fieldProperties{Computed: true, SQL: "a + b"}))
	assert.Equal(t, `{"join":{"references_name":"r","sql":"s.id = d.id"}}`,
		marshal(&fieldProperties{Join: &joinBinding{ReferencesName: "r", SQL: "s.id = d.id"}}))
	assert.Equal(t, `{"function_call":{"function":{"module":"m","name":"f"},"sql":"t.id = f.id"}}`,
		marshal(&fieldProperties{FunctionCall: &functionCallBinding{Function: functionRef{Module: "m", Name: "f"}, SQL: "t.id = f.id"}}))
	assert.Equal(t, `{"default":{"insert_exp":"now()","update_exp":"now()"}}`,
		marshal(&fieldProperties{Default: &defaultValue{InsertExp: "now()", UpdateExp: "now()"}}))
}
