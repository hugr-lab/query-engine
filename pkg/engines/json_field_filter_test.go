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

// TestJSONFieldFilterSQL_DuckDB pins the SQL each typed sub-filter produces
// for DuckDB, including the dialect-specific coercion that this engine owns
// (string-format for DATE/TIME/TIMESTAMP/INTERVAL, with an explicit CAST on
// the parameter side).
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
			name: "int gte",
			fv: map[string]any{
				"path": "user.age",
				"int":  map[string]any{"gte": 18},
			},
			wantSQL:    "(try_cast(json_value(meta::JSON,'$.user.age') AS INTEGER) >= $1)",
			wantParams: []any{18},
		},
		{
			name: "string ilike (delegates to FilterOperationSQLValue)",
			fv: map[string]any{
				"path":   "owner.email",
				"string": map[string]any{"ilike": "%@x.com"},
			},
			wantSQL:    "(json_extract_string(meta::JSON,'$.owner.email') ILIKE $1)",
			wantParams: []any{"%@x.com"},
		},
		{
			name: "int gte and lt (sorted op order)",
			fv: map[string]any{
				"path": "user.age",
				"int":  map[string]any{"gte": 18, "lt": 65},
			},
			wantSQL:    "(try_cast(json_value(meta::JSON,'$.user.age') AS INTEGER) >= $1) AND (try_cast(json_value(meta::JSON,'$.user.age') AS INTEGER) < $2)",
			wantParams: []any{18, 65},
		},
		{
			name: "isNull true alone",
			fv: map[string]any{
				"path":   "a.b",
				"isNull": true,
			},
			wantSQL: "json_type(meta,'$.a.b') = 'NULL'",
		},
		{
			name: "isNull false alone",
			fv: map[string]any{
				"path":   "a.b",
				"isNull": false,
			},
			wantSQL: "json_type(meta,'$.a.b') <> 'NULL'",
		},
		{
			name: "coalesce + int",
			fv: map[string]any{
				"path":     "user.age",
				"coalesce": 0,
				"int":      map[string]any{"gte": 18},
			},
			wantSQL:    "(COALESCE(try_cast(json_value(meta::JSON,'$.user.age') AS INTEGER), try_cast($1 AS INTEGER)) >= $2)",
			wantParams: []any{0, 18},
		},
		{
			name: "isNull false + coalesce + int (distinguish defaulted from real)",
			fv: map[string]any{
				"path":     "user.age",
				"isNull":   false,
				"coalesce": 0,
				"int":      map[string]any{"gte": 18},
			},
			wantSQL:    "json_type(meta,'$.user.age') <> 'NULL' AND (COALESCE(try_cast(json_value(meta::JSON,'$.user.age') AS INTEGER), try_cast($1 AS INTEGER)) >= $2)",
			wantParams: []any{0, 18},
		},
		{
			name: "two typed sub-filters rejected",
			fv: map[string]any{
				"path":   "x",
				"int":    map[string]any{"eq": 1},
				"string": map[string]any{"eq": "a"},
			},
			wantErr: true,
		},
		{
			name: "missing path",
			fv: map[string]any{
				"int": map[string]any{"eq": 1},
			},
			wantErr: true,
		},
		{
			name: "no isNull and no sub-filter",
			fv: map[string]any{
				"path": "x",
			},
			wantErr: true,
		},
		{
			name: "geometry intersects delegates to FilterOperationSQLValue",
			fv: map[string]any{
				"path":     "shape",
				"geometry": map[string]any{"intersects": orb.Point{1, 2}},
			},
			wantSQL:    "(ST_Intersects(ST_GeomFromGeoJSON(NULLIF(json_extract(meta::JSON,'$.shape')::VARCHAR, 'null')),$1))",
			wantParams: []any{orb.Point{1, 2}},
		},
		{
			name: "date eq — time.Time coerced to YYYY-MM-DD, CAST($1 AS DATE)",
			fv: map[string]any{
				"path": "signup.day",
				"date": map[string]any{"eq": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
			},
			wantSQL:    "(try_cast(json_extract_string(meta::JSON,'$.signup.day') AS DATE) = CAST($1 AS DATE))",
			wantParams: []any{"2024-01-15"},
		},
		{
			name: "time eq — coerced to HH:MM:SS, avoids unimplemented TIMESTAMPTZ→TIME cast",
			fv: map[string]any{
				"path": "lunch.at_time",
				"time": map[string]any{"eq": time.Date(1, 1, 1, 12, 30, 0, 0, time.UTC)},
			},
			wantSQL:    "(try_cast(json_extract_string(meta::JSON,'$.lunch.at_time') AS TIME) = CAST($1 AS TIME))",
			wantParams: []any{"12:30:00"},
		},
		{
			name: "datetime eq — coerced to YYYY-MM-DD HH:MM:SS, CAST AS TIMESTAMP",
			fv: map[string]any{
				"path":     "event.local_dt",
				"dateTime": map[string]any{"eq": time.Date(2024, 6, 11, 10, 0, 0, 0, time.UTC)},
			},
			wantSQL:    "(try_cast(json_extract_string(meta::JSON,'$.event.local_dt') AS TIMESTAMP) = CAST($1 AS TIMESTAMP))",
			wantParams: []any{"2024-06-11 10:00:00"},
		},
		{
			name: "timestamp gte — time.Time binds natively (TIMESTAMPTZ), no coercion",
			fv: map[string]any{
				"path":      "event.at",
				"timestamp": map[string]any{"gte": time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)},
			},
			wantSQL:    "(try_cast(json_extract_string(meta::JSON,'$.event.at') AS TIMESTAMPTZ) >= $1)",
			wantParams: []any{time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)},
		},
		{
			name: "interval eq — time.Duration coerced to '<seconds> seconds', CAST AS INTERVAL",
			fv: map[string]any{
				"path":     "subscription.duration",
				"interval": map[string]any{"eq": 90 * time.Minute},
			},
			wantSQL:    "(try_cast(json_extract_string(meta::JSON,'$.subscription.duration') AS INTERVAL) = CAST($1 AS INTERVAL))",
			wantParams: []any{"5400 seconds"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotParams, err := CompileJSONFieldFilterSQL(e, "meta", "", tt.fv, nil)
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

// TestJSONFieldFilterSQL_Postgres pins the SQL each typed sub-filter produces
// for Postgres. Most types let pgx bind time.Time / time.Duration directly
// (PG's implicit coercion handles DATE / TIMESTAMP / TIMESTAMPTZ / INTERVAL).
// TIME is the lone outlier: pgx would bind time.Time as TIMESTAMPTZ with year
// 0001 (PG rejects the date) and PG cannot implicitly cast TIMESTAMPTZ→TIME
// either, so this engine coerces to "HH:MM:SS" + ::TIME.
func TestJSONFieldFilterSQL_Postgres(t *testing.T) {
	e := &Postgres{}
	cases := []struct {
		name       string
		fv         map[string]any
		wantSQL    string
		wantParams []any
	}{
		{
			name: "int gte",
			fv: map[string]any{
				"path": "user.age",
				"int":  map[string]any{"gte": 18},
			},
			wantSQL:    "((meta->'user'->>'age')::INTEGER >= $1)",
			wantParams: []any{18},
		},
		{
			name: "float lt uses DOUBLE PRECISION",
			fv: map[string]any{
				"path":  "metrics.score",
				"float": map[string]any{"lt": 0.5},
			},
			wantSQL:    "((meta->'metrics'->>'score')::DOUBLE PRECISION < $1)",
			wantParams: []any{0.5},
		},
		{
			name: "date eq — time.Time binds as TIMESTAMPTZ, PG implicitly coerces to DATE",
			fv: map[string]any{
				"path": "signup.day",
				"date": map[string]any{"eq": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
			},
			wantSQL:    "((meta->'signup'->>'day')::DATE = $1)",
			wantParams: []any{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		},
		{
			name: "time eq — coerced to HH:MM:SS + ::TIME (PG cannot cast TIMESTAMPTZ→TIME)",
			fv: map[string]any{
				"path": "lunch.at_time",
				"time": map[string]any{"eq": time.Date(1, 1, 1, 12, 30, 0, 0, time.UTC)},
			},
			wantSQL:    "((meta->'lunch'->>'at_time')::TIME = CAST($1 AS TIME))",
			wantParams: []any{"12:30:00"},
		},
		{
			name: "interval eq — explicit ::INTERVAL cast catches both pgx-INTERVAL bind and varchar fallback",
			fv: map[string]any{
				"path":     "subscription.duration",
				"interval": map[string]any{"eq": 90 * time.Minute},
			},
			wantSQL:    "((meta->'subscription'->>'duration')::INTERVAL = CAST($1 AS INTERVAL))",
			wantParams: []any{90 * time.Minute},
		},
		{
			name: "geometry intersects delegates to FilterOperationSQLValue",
			fv: map[string]any{
				"path":     "shape",
				"geometry": map[string]any{"intersects": orb.Point{1, 2}},
			},
			wantSQL:    "(ST_Intersects(ST_GeomFromGeoJSON((meta->>'shape')::text),$1))",
			wantParams: []any{orb.Point{1, 2}},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotParams, err := CompileJSONFieldFilterSQL(e, "meta", "", tt.fv, nil)
			if err != nil {
				t.Fatal(err)
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
