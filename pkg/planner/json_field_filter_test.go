package planner

import (
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/engines"
)

func TestJsonFieldFilterSQL_DuckDB(t *testing.T) {
	e := engines.NewDuckDB()
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
			name: "string ilike",
			fv: map[string]any{
				"path":   "owner.email",
				"string": map[string]any{"ilike": "%@x.com"},
			},
			wantSQL:    "(try_cast(json_value(meta::JSON,'$.owner.email') AS VARCHAR) ILIKE $1)",
			wantParams: []any{"%@x.com"},
		},
		{
			name: "isNull true alone",
			fv: map[string]any{
				"path":   "a.b",
				"isNull": true,
			},
			wantSQL: "(json_value(meta::JSON,'$.a.b')) IS NULL",
		},
		{
			name: "isNull false alone",
			fv: map[string]any{
				"path":   "a.b",
				"isNull": false,
			},
			wantSQL: "(json_value(meta::JSON,'$.a.b')) IS NOT NULL",
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
			wantSQL:    "(json_value(meta::JSON,'$.user.age')) IS NOT NULL AND (COALESCE(try_cast(json_value(meta::JSON,'$.user.age') AS INTEGER), try_cast($1 AS INTEGER)) >= $2)",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotParams, err := jsonFieldFilterSQL(e, "meta", "", tt.fv, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err: want %v got %v", tt.wantErr, err)
			}
			if tt.wantErr {
				return
			}
			// For two-clauses output the clauses come from a non-deterministic map iteration
			// only inside subValue, but we put isNull first explicitly.  All cases above are
			// either single-key sub-filters or just isNull, so order is deterministic.
			if normalize(gotSQL) != normalize(tt.wantSQL) {
				t.Errorf("sql:\n got %q\nwant %q", gotSQL, tt.wantSQL)
			}
			if len(gotParams) != len(tt.wantParams) {
				t.Errorf("params len: got %v want %v", gotParams, tt.wantParams)
				return
			}
			for i := range gotParams {
				if gotParams[i] != tt.wantParams[i] {
					t.Errorf("params[%d]: got %v want %v", i, gotParams[i], tt.wantParams[i])
				}
			}
		})
	}
}

func TestJsonFieldFilterSQL_Postgres(t *testing.T) {
	e := &engines.Postgres{}
	gotSQL, params, err := jsonFieldFilterSQL(e, "meta", "", map[string]any{
		"path": "user.age",
		"int":  map[string]any{"gte": 18},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "((meta->'user'->>'age')::INTEGER >= $1)"
	if gotSQL != want {
		t.Errorf("sql:\n got %q\nwant %q", gotSQL, want)
	}
	if len(params) != 1 || params[0] != 18 {
		t.Errorf("params: %v", params)
	}
}

func normalize(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
