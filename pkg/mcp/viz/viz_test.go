package viz

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalRowsUnwrapsAndExplains(t *testing.T) {
	// The usual GraphQL shape needs no jq at all.
	rows, err := CanonicalRows(mustJSON(t, `{"sales":{"orders":[{"month":"2026-01","total":10}]}}`))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "2026-01", rows[0]["month"])

	// An object with two keys is NOT a wrapper — unwrapping it would guess.
	_, err = CanonicalRows(mustJSON(t, `{"a":[],"b":[]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "got object")

	// Errors must name the offending field: the model's next call depends on
	// knowing which value to flatten.
	_, err = CanonicalRows(mustJSON(t, `[{"month":"2026-01","customer":{"name":"acme"}}]`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `field "customer"`)
	assert.Contains(t, err.Error(), "nested object")

	_, err = CanonicalRows(mustJSON(t, `[1,2,3]`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row 0 is a number")
}

// The KPI-card contract is validated with the same discipline as canonical
// rows: precise errors naming the card and the field, because a vague error
// costs the caller a whole round trip.
func TestCanonicalKPIs(t *testing.T) {
	full := mustJSON(t, `[{"label":"Revenue","value":12.5,"unit":"$","format":"number",
		"delta":1.2,"delta_pct":3.4,"direction":"up_good","trend":[1,2,3],"subtitle":"vs July"}]`)
	kpis, err := CanonicalKPIs(full)
	require.NoError(t, err)
	require.Len(t, kpis, 1)
	assert.Equal(t, "Revenue", kpis[0].Label)
	assert.Equal(t, 12.5, kpis[0].Value)
	assert.Equal(t, []float64{1, 2, 3}, kpis[0].Trend)
	assert.Equal(t, 3.4, *kpis[0].DeltaPct)

	// The usual single-key GraphQL wrapping unwraps, same as rows.
	wrapped := mustJSON(t, `{"data":{"m":[{"label":"a","value":1}]}}`)
	kpis, err = CanonicalKPIs(wrapped)
	require.NoError(t, err)
	assert.Len(t, kpis, 1)

	for name, tc := range map[string]struct{ in, wantErr string }{
		"not an array":  {`{"a":1,"b":2}`, "expected an array of KPI cards"},
		"empty":         {`[]`, "at least one"},
		"no label":      {`[{"value":1}]`, "label is required"},
		"no value":      {`[{"label":"x"}]`, "value is required"},
		"nested value":  {`[{"label":"x","value":{"a":1}}]`, "nested object"},
		"typoed field":  {`[{"label":"x","value":1,"delta_percent":5}]`, `unknown field "delta_percent"`},
		"bad direction": {`[{"label":"x","value":1,"direction":"up"}]`, "up_good|down_good|neutral"},
		"bad trend":     {`[{"label":"x","value":1,"trend":[1,"a"]}]`, "trend[1]"},
		"bad delta":     {`[{"label":"x","value":1,"delta":"big"}]`, "delta must be a number"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := CanonicalKPIs(mustJSON(t, tc.in))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	// The cap: a panel is a glance surface.
	big := make([]any, KPICap+1)
	for i := range big {
		big[i] = map[string]any{"label": "x", "value": float64(i)}
	}
	_, err = CanonicalKPIs(any(big))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at 32 at most")
}

// Volume is stated by the caller, never papered over here: hugr tables run to
// hundreds of millions of rows, and a silently truncated chart is a wrong
// chart. The guard fires only for a query that set no bound of its own.
func TestQueryHasLimit(t *testing.T) {
	for query, want := range map[string]bool{
		`query { m { obj { a } } }`:                             false,
		`query($limit: Int) { m { obj(limit: $limit) { a } } }`: true,
		`query { m { obj(limit: 100) { a } } }`:                 true,
		`query { m { obj_bucket_aggregation { key { a } } } }`:  false,
	} {
		assert.Equalf(t, want, QueryHasLimit(query), "limit detection for %s", query)
	}
}

func TestQueryLimit(t *testing.T) {
	cases := []struct {
		query string
		vars  map[string]any
		want  int
		known bool
	}{
		{`{ m { obj(limit: 500) { a } } }`, nil, 500, true},
		{`{ m { obj(limit:2000, offset: 0) { a } } }`, nil, 2000, true},
		{`query($limit: Int) { m { obj(limit: $limit) { a } } }`, map[string]any{"limit": float64(300)}, 300, true},
		{`{ m { obj { a } } }`, nil, 0, false},
		// The innermost bound is the one that shapes the rows we receive.
		{`{ m { obj(limit: 100) { rel(limit: 5) { a } } } }`, nil, 5, true},
	}
	for _, c := range cases {
		got, known := QueryLimit(c.query, c.vars)
		assert.Equalf(t, c.known, known, "known for %s", c.query)
		if c.known {
			assert.Equalf(t, c.want, got, "limit for %s", c.query)
		}
	}
}

func mustJSON(t *testing.T, s string) any {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal([]byte(s), &v))
	return v
}
