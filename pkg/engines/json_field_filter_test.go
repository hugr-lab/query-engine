package engines

import (
	"strings"
	"testing"
	"time"

	"github.com/paulmach/orb"
)

func normalizeSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// TestJSONFieldFilterSQL_DuckDB pins the SQL each typed JSONFieldFilter sub-
// filter produces for DuckDB after the refactor onto ExtractNestedTypedValue2.
// int/float/string/bool/timestamp/dateTime delegate to ExtractNestedTypedValue
// (subscript-style + try_cast). bigInt/date/time/interval/geometry use the
// json_extract / json_value / json_extract_string patterns the json-ops5
// reference implementation validated. Time-like params are coerced into
// deterministic strings and the freshly-bound `$N` is wrapped with CAST.
func TestJSONFieldFilterSQL_DuckDB(t *testing.T) {
	e := NewDuckDB()
	tests := []struct {
		name       string
		fv         map[string]any
		wantSQL    string
		wantParams []any
		wantErr    bool
	}{
		{
			name:       "int gte — delegates to ExtractNestedTypedValue(number)",
			fv:         map[string]any{"path": "user.age", "int": map[string]any{"gte": 18}},
			wantSQL:    "(try_cast(meta['user']['age'] AS FLOAT) >= $1)",
			wantParams: []any{18},
		},
		{
			name:       "bigInt eq — delegates to ExtractNestedTypedValue default branch (try_cast … AS BIGINT)",
			fv:         map[string]any{"path": "user.id", "bigInt": map[string]any{"eq": int64(9223372036854775000)}},
			wantSQL:    "(try_cast(meta['user']['id'] AS BIGINT) = $1)",
			wantParams: []any{int64(9223372036854775000)},
		},
		{
			name:       "float lt — delegates to ExtractNestedTypedValue(number)",
			fv:         map[string]any{"path": "metrics.score", "float": map[string]any{"lt": 0.5}},
			wantSQL:    "(try_cast(meta['metrics']['score'] AS FLOAT) < $1)",
			wantParams: []any{0.5},
		},
		{
			name:       "string ilike — inline json_extract_string (subscript-cast keeps JSON quotes; can't compare)",
			fv:         map[string]any{"path": "owner.email", "string": map[string]any{"ilike": "%@x.com"}},
			wantSQL:    "(json_extract_string(meta::JSON,'$.owner.email') ILIKE $1)",
			wantParams: []any{"%@x.com"},
		},
		{
			name:       "bool eq — delegates to ExtractNestedTypedValue(bool)",
			fv:         map[string]any{"path": "flags.active", "bool": map[string]any{"eq": true}},
			wantSQL:    "(try_cast(meta['flags']['active'] AS BOOLEAN) = $1)",
			wantParams: []any{true},
		},
		{
			name:       "date eq — delegates (try_cast … AS DATE) + CAST($1 AS DATE) on coerced string",
			fv:         map[string]any{"path": "signup.day", "date": map[string]any{"eq": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}},
			wantSQL:    "(try_cast(meta['signup']['day'] AS DATE) = CAST($1 AS DATE))",
			wantParams: []any{"2024-01-15"},
		},
		{
			name:       "time eq — delegates (try_cast … AS TIME) + CAST($1 AS TIME)",
			fv:         map[string]any{"path": "lunch.at_time", "time": map[string]any{"eq": time.Date(1, 1, 1, 12, 30, 0, 0, time.UTC)}},
			wantSQL:    "(try_cast(meta['lunch']['at_time'] AS TIME) = CAST($1 AS TIME))",
			wantParams: []any{"12:30:00"},
		},
		{
			name:       "dateTime eq — delegates to ExtractNestedTypedValue(datetime); coerced string + CAST($1 AS TIMESTAMP)",
			fv:         map[string]any{"path": "event.local_dt", "dateTime": map[string]any{"eq": time.Date(2024, 6, 11, 10, 0, 0, 0, time.UTC)}},
			wantSQL:    "(try_cast(meta['event']['local_dt'] AS TIMESTAMP) = CAST($1 AS TIMESTAMP))",
			wantParams: []any{"2024-06-11 10:00:00"},
		},
		{
			name:       "timestamp gte — delegates to ExtractNestedTypedValue(timestamp); time.Time binds natively",
			fv:         map[string]any{"path": "event.at", "timestamp": map[string]any{"gte": time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)}},
			wantSQL:    "(try_cast(meta['event']['at'] AS TIMESTAMPTZ) >= $1)",
			wantParams: []any{time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:       "interval eq — delegates (try_cast … AS INTERVAL) + CAST($1 AS INTERVAL)",
			fv:         map[string]any{"path": "subscription.duration", "interval": map[string]any{"eq": 90 * time.Minute}},
			wantSQL:    "(try_cast(meta['subscription']['duration'] AS INTERVAL) = CAST($1 AS INTERVAL))",
			wantParams: []any{"5400 seconds"},
		},
		{
			name:       "geometry intersects — new inline (json_extract → ST_GeomFromGeoJSON)",
			fv:         map[string]any{"path": "shape", "geometry": map[string]any{"intersects": orb.Point{1, 2}}},
			wantSQL:    "(ST_Intersects(ST_GeomFromGeoJSON(NULLIF(json_extract(meta::JSON,'$.shape')::VARCHAR, 'null')),$1))",
			wantParams: []any{orb.Point{1, 2}},
		},
		{
			name:    "isNull true alone",
			fv:      map[string]any{"path": "a.b", "isNull": true},
			wantSQL: "json_type(meta,'$.a.b') = 'NULL'",
		},
		{
			name:    "isNull false alone",
			fv:      map[string]any{"path": "a.b", "isNull": false},
			wantSQL: "json_type(meta,'$.a.b') <> 'NULL'",
		},
		{
			name:       "coalesce + int — placeholder branch uses inline try_cast (no delegation)",
			fv:         map[string]any{"path": "user.age", "coalesce": 0, "int": map[string]any{"gte": 18}},
			wantSQL:    "(COALESCE(try_cast(meta['user']['age'] AS FLOAT), try_cast($1 AS FLOAT)) >= $2)",
			wantParams: []any{0, 18},
		},
		{
			name:       "isNull false + coalesce + int",
			fv:         map[string]any{"path": "user.age", "isNull": false, "coalesce": 0, "int": map[string]any{"gte": 18}},
			wantSQL:    "json_type(meta,'$.user.age') <> 'NULL' AND (COALESCE(try_cast(meta['user']['age'] AS FLOAT), try_cast($1 AS FLOAT)) >= $2)",
			wantParams: []any{0, 18},
		},
		{
			name:    "two typed sub-filters rejected",
			fv:      map[string]any{"path": "x", "int": map[string]any{"eq": 1}, "string": map[string]any{"eq": "a"}},
			wantErr: true,
		},
		{
			name:    "missing path",
			fv:      map[string]any{"int": map[string]any{"eq": 1}},
			wantErr: true,
		},
		{
			name:    "no isNull and no sub-filter",
			fv:      map[string]any{"path": "x"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotParams, err := e.FilterOperationSQLValue("meta", "", "field", tt.fv, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err: want %v got %v", tt.wantErr, err)
			}
			if tt.wantErr {
				return
			}
			if normalizeSQL(gotSQL) != normalizeSQL(tt.wantSQL) {
				t.Errorf("sql:\n got %q\nwant %q", gotSQL, tt.wantSQL)
			}
			if len(gotParams) != len(tt.wantParams) {
				t.Errorf("params len: got %v want %v", gotParams, tt.wantParams)
				return
			}
			for i := range gotParams {
				if gotParams[i] != tt.wantParams[i] {
					t.Errorf("params[%d]: got %v (%T) want %v (%T)", i, gotParams[i], gotParams[i], tt.wantParams[i], tt.wantParams[i])
				}
			}
		})
	}
}

// TestJSONFieldFilterSQL_Postgres pins SQL after the refactor onto
// ExtractNestedTypedValue2 — delegation for the 5 covered tokens (which uses
// the engine's jsonb_typeof CASE wrapper for number/bool/string at empty path
// and `->>` shortcut for string at non-empty path), new inline `(extracted)::T`
// for bigInt/date/time/interval/geometry. pgx native bindings handle most
// time params; TIME alone coerces to "HH:MM:SS" and CASTs the placeholder.
func TestJSONFieldFilterSQL_Postgres(t *testing.T) {
	e := &Postgres{}
	cases := []struct {
		name       string
		fv         map[string]any
		wantSQL    string
		wantParams []any
		wantErr    bool
	}{
		{
			name:       "int gte — direct ->>::FLOAT cast",
			fv:         map[string]any{"path": "user.age", "int": map[string]any{"gte": 18}},
			wantSQL:    "((meta->'user'->>'age')::FLOAT >= $1)",
			wantParams: []any{18},
		},
		{
			name:       "bigInt eq — ->>::BIGINT",
			fv:         map[string]any{"path": "user.id", "bigInt": map[string]any{"eq": int64(9223372036854775000)}},
			wantSQL:    "((meta->'user'->>'id')::BIGINT = $1)",
			wantParams: []any{int64(9223372036854775000)},
		},
		{
			name:       "float lt — direct ->>::FLOAT",
			fv:         map[string]any{"path": "metrics.score", "float": map[string]any{"lt": 0.5}},
			wantSQL:    "((meta->'metrics'->>'score')::FLOAT < $1)",
			wantParams: []any{0.5},
		},
		{
			name:       "string ilike — ->> shortcut returns text directly",
			fv:         map[string]any{"path": "owner.email", "string": map[string]any{"ilike": "%@x.com"}},
			wantSQL:    "(meta->'owner'->>'email' ILIKE $1)",
			wantParams: []any{"%@x.com"},
		},
		{
			name:       "bool eq — direct ->>::BOOL",
			fv:         map[string]any{"path": "flags.active", "bool": map[string]any{"eq": true}},
			wantSQL:    "((meta->'flags'->>'active')::BOOL = $1)",
			wantParams: []any{true},
		},
		{
			name:       "date eq — new inline (->>::DATE)",
			fv:         map[string]any{"path": "signup.day", "date": map[string]any{"eq": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}},
			wantSQL:    "((meta->'signup'->>'day')::DATE = $1)",
			wantParams: []any{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:       "time eq — new inline (->>::TIME) + coerced HH:MM:SS + ::TIME on $1",
			fv:         map[string]any{"path": "lunch.at_time", "time": map[string]any{"eq": time.Date(1, 1, 1, 12, 30, 0, 0, time.UTC)}},
			wantSQL:    "((meta->'lunch'->>'at_time')::TIME = CAST($1 AS TIME))",
			wantParams: []any{"12:30:00"},
		},
		{
			name:       "dateTime eq — inline ->>::TIMESTAMP (jsonb_path_query_first delegation gave non-comparable text)",
			fv:         map[string]any{"path": "event.local_dt", "dateTime": map[string]any{"eq": time.Date(2024, 6, 11, 10, 0, 0, 0, time.UTC)}},
			wantSQL:    "((meta->'event'->>'local_dt')::TIMESTAMP = $1)",
			wantParams: []any{time.Date(2024, 6, 11, 10, 0, 0, 0, time.UTC)},
		},
		{
			name:       "timestamp gte — inline ->>::TIMESTAMPTZ",
			fv:         map[string]any{"path": "event.at", "timestamp": map[string]any{"gte": time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)}},
			wantSQL:    "((meta->'event'->>'at')::TIMESTAMPTZ >= $1)",
			wantParams: []any{time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:       "interval eq — new inline + defensive ::INTERVAL cast on $1",
			fv:         map[string]any{"path": "subscription.duration", "interval": map[string]any{"eq": 90 * time.Minute}},
			wantSQL:    "((meta->'subscription'->>'duration')::INTERVAL = CAST($1 AS INTERVAL))",
			wantParams: []any{90 * time.Minute},
		},
		{
			name:       "geometry intersects — new inline ST_GeomFromGeoJSON",
			fv:         map[string]any{"path": "shape", "geometry": map[string]any{"intersects": orb.Point{1, 2}}},
			wantSQL:    "(ST_Intersects(ST_GeomFromGeoJSON((meta->>'shape')::text),$1))",
			wantParams: []any{orb.Point{1, 2}},
		},
		{
			name:    "isNull true alone",
			fv:      map[string]any{"path": "a.b", "isNull": true},
			wantSQL: "jsonb_typeof((meta)->'a'->'b') = 'null'",
		},
		{
			name:       "coalesce + int — uniform ::FLOAT cast on both column and placeholder",
			fv:         map[string]any{"path": "user.age", "coalesce": 0, "int": map[string]any{"gte": 18}},
			wantSQL:    "(COALESCE((meta->'user'->>'age')::FLOAT, ($1)::FLOAT) >= $2)",
			wantParams: []any{0, 18},
		},
		{
			name:    "two typed sub-filters rejected",
			fv:      map[string]any{"path": "x", "int": map[string]any{"eq": 1}, "string": map[string]any{"eq": "a"}},
			wantErr: true,
		},
		{
			name:    "missing path",
			fv:      map[string]any{"int": map[string]any{"eq": 1}},
			wantErr: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotParams, err := e.FilterOperationSQLValue("meta", "", "field", tt.fv, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err: want %v got %v", tt.wantErr, err)
			}
			if tt.wantErr {
				return
			}
			if normalizeSQL(gotSQL) != normalizeSQL(tt.wantSQL) {
				t.Errorf("sql:\n got %q\nwant %q", gotSQL, tt.wantSQL)
			}
			if len(gotParams) != len(tt.wantParams) {
				t.Errorf("params len: got %v want %v", gotParams, tt.wantParams)
				return
			}
			for i := range gotParams {
				if gotParams[i] != tt.wantParams[i] {
					t.Errorf("params[%d]: got %v (%T) want %v (%T)", i, gotParams[i], gotParams[i], tt.wantParams[i], tt.wantParams[i])
				}
			}
		})
	}
}
