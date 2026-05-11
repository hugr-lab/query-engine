package engines

import (
	"strings"
	"testing"
	"time"
)

func normalizeSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// TestJSONFieldFilterSQL_DuckDB pins the SQL each typed sub-filter produces
// for DuckDB. The supported types are: int, float, string, bool, timestamp —
// they all flow through DuckDB.ExtractNestedTypedValue and then recurse into
// FilterOperationSQLValue.
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
			wantSQL:    "try_cast(meta['user']['age'] AS FLOAT) >= $1",
			wantParams: []any{18},
		},
		{
			name: "float lt",
			fv: map[string]any{
				"path":  "metrics.score",
				"float": map[string]any{"lt": 0.5},
			},
			wantSQL:    "try_cast(meta['metrics']['score'] AS FLOAT) < $1",
			wantParams: []any{0.5},
		},
		{
			name: "string ilike (delegates to FilterOperationSQLValue)",
			fv: map[string]any{
				"path":   "owner.email",
				"string": map[string]any{"ilike": "%@x.com"},
			},
			wantSQL:    "try_cast(meta['owner']['email'] AS VARCHAR) ILIKE $1",
			wantParams: []any{"%@x.com"},
		},
		{
			name: "bool eq",
			fv: map[string]any{
				"path": "flags.active",
				"bool": map[string]any{"eq": true},
			},
			wantSQL:    "try_cast(meta['flags']['active'] AS BOOLEAN) = $1",
			wantParams: []any{true},
		},
		{
			name: "timestamp gte",
			fv: map[string]any{
				"path":      "event.at",
				"timestamp": map[string]any{"gte": time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)},
			},
			wantSQL:    "try_cast(meta['event']['at'] AS TIMESTAMPTZ) >= $1",
			wantParams: []any{time.Date(2024, 6, 9, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:    "more than 2 keys rejected",
			fv:      map[string]any{"path": "x", "int": map[string]any{"eq": 1}, "string": map[string]any{"eq": "a"}},
			wantErr: true,
		},
		{
			name:    "missing typed sub-filter (only path)",
			fv:      map[string]any{"path": "x"},
			wantErr: true,
		},
		{
			name:    "missing path",
			fv:      map[string]any{"int": map[string]any{"eq": 1}},
			wantErr: true,
		},
		{
			name:    "empty path",
			fv:      map[string]any{"path": "", "int": map[string]any{"eq": 1}},
			wantErr: true,
		},
		{
			name:    "unsupported type",
			fv:      map[string]any{"path": "x", "date": map[string]any{"eq": "2024-01-01"}},
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

// TestJSONFieldFilterSQL_DuckDB_TwoOps verifies AND-folding when the typed
// sub-filter carries multiple operators. Go's map iteration order is not
// deterministic, so the test asserts that both fragments appear and that the
// fragments are correctly joined by " AND ".
func TestJSONFieldFilterSQL_DuckDB_TwoOps(t *testing.T) {
	e := NewDuckDB()
	fv := map[string]any{
		"path": "user.age",
		"int":  map[string]any{"gte": 18, "lt": 65},
	}
	got, gotParams, err := e.FilterOperationSQLValue("meta", "", "field", fv, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, " AND ") {
		t.Errorf("expected AND-fold, got %q", got)
	}
	if !strings.Contains(got, "try_cast(meta['user']['age'] AS FLOAT) >= $") {
		t.Errorf("missing gte fragment in %q", got)
	}
	if !strings.Contains(got, "try_cast(meta['user']['age'] AS FLOAT) < $") {
		t.Errorf("missing lt fragment in %q", got)
	}
	if len(gotParams) != 2 {
		t.Errorf("expected 2 params, got %v", gotParams)
	}
}

// TestJSONFieldFilterSQL_Postgres pins the SQL each typed sub-filter produces
// for Postgres. Same 5 types as DuckDB; SQL shape follows Postgres'
// ExtractNestedTypedValue (jsonb_typeof guard around the cast).
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
			name: "int gte",
			fv: map[string]any{
				"path": "user.age",
				"int":  map[string]any{"gte": 18},
			},
			wantSQL:    "((CASE WHEN jsonb_typeof(meta->'user'->'age') = 'number' THEN (meta->'user'->'age')::float ELSE NULL END))::FLOAT >= $1",
			wantParams: []any{18},
		},
		{
			name: "float lt",
			fv: map[string]any{
				"path":  "metrics.score",
				"float": map[string]any{"lt": 0.5},
			},
			wantSQL:    "((CASE WHEN jsonb_typeof(meta->'metrics'->'score') = 'number' THEN (meta->'metrics'->'score')::float ELSE NULL END))::FLOAT < $1",
			wantParams: []any{0.5},
		},
		{
			name:    "more than 2 keys rejected",
			fv:      map[string]any{"path": "x", "int": map[string]any{"eq": 1}, "string": map[string]any{"eq": "a"}},
			wantErr: true,
		},
		{
			name:    "unsupported type",
			fv:      map[string]any{"path": "x", "date": map[string]any{"eq": "2024-01-01"}},
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
