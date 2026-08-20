package reports

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/hugr-lab/query-engine/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubQuerier answers by recognizing a fragment of the query text and
// records every call — enough to drive the whole pipeline offline.
type stubQuerier struct {
	mu    sync.Mutex
	calls []stubCall
}

type stubCall struct {
	query string
	vars  map[string]any
}

func (s *stubQuerier) Query(_ context.Context, query string, vars map[string]any) (*types.Response, error) {
	s.mu.Lock()
	snapshot := make(map[string]any, len(vars))
	for k, v := range vars {
		snapshot[k] = v
	}
	s.calls = append(s.calls, stubCall{query: query, vars: snapshot})
	s.mu.Unlock()

	switch {
	case strings.Contains(query, "payments_aggregation"):
		return respData(map[string]any{"op": map[string]any{
			"total": map[string]any{"amount": map[string]any{"sum": 12345.0}},
		}}), nil
	case strings.Contains(query, "by_month"):
		return respData(map[string]any{"op": map[string]any{"by_month": []any{
			map[string]any{"key": map[string]any{"m": "2023-01"}, "aggregations": map[string]any{"s": map[string]any{"sum": 10.0}}},
			map[string]any{"key": map[string]any{"m": "2023-02"}, "aggregations": map[string]any{"s": map[string]any{"sum": 20.0}}},
		}}}), nil
	case strings.Contains(query, "providers"):
		rows := make([]any, 50)
		for i := range rows {
			rows[i] = map[string]any{"name": "p", "total": float64(i)}
		}
		return respData(map[string]any{"op": map[string]any{"providers": rows}}), nil
	case strings.Contains(query, "states_agg"):
		return respData(map[string]any{"op": map[string]any{"states_agg": []any{
			map[string]any{"key": map[string]any{"st": "CA"}},
			map[string]any{"key": map[string]any{"st": "NY"}},
		}}}), nil
	case strings.Contains(query, "cities"):
		return respData(map[string]any{"op": map[string]any{"cities": []any{
			map[string]any{"name": "Los Angeles"},
		}}}), nil
	case strings.Contains(query, "boom"):
		return &types.Response{Errors: types.WarpGraphQLError(assert.AnError)}, nil
	case strings.Contains(query, "wide_open"):
		rows := make([]any, 2500)
		for i := range rows {
			rows[i] = map[string]any{"i": float64(i)}
		}
		return respData(map[string]any{"m": map[string]any{"obj": rows}}), nil
	}
	return respData(map[string]any{}), nil
}

func respData(data map[string]any) *types.Response {
	return &types.Response{Data: data}
}

func (s *stubQuerier) callFor(fragment string) (stubCall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if strings.Contains(c.query, fragment) {
			return c, true
		}
	}
	return stubCall{}, false
}

func TestRunExecutesEverySectionKind(t *testing.T) {
	q := &stubQuerier{}
	data, err := Run(t.Context(), q, validSpec(), map[string]any{"states": []any{"CA"}}, RunOptions{})
	require.NoError(t, err)

	require.Len(t, data.Sections, 4)
	kpi, chart, table, text := data.Sections[0], data.Sections[1], data.Sections[2], data.Sections[3]

	require.Empty(t, kpi.Error)
	require.Len(t, kpi.Kpis, 1)
	assert.Equal(t, "Total", kpi.Kpis[0].Label)
	assert.Equal(t, 12345.0, kpi.Kpis[0].Value)

	require.Empty(t, chart.Error)
	assert.Equal(t, 2, chart.RowCount)
	assert.Equal(t, "2023-01", chart.Rows[0]["month"])

	require.Empty(t, table.Error)
	assert.Equal(t, 50, table.RowCount)
	assert.True(t, table.AtLimit, "exactly the query's own limit came back")

	assert.Equal(t, "text", text.Kind)
	assert.Empty(t, text.Rows)

	// Defaults filled the dates; the submitted list rode through.
	assert.Equal(t, "2023-01-01", data.Variables["date_from"])
	assert.Equal(t, []any{"CA"}, data.Variables["states"])
}

func TestRunResolvesOptionsInDependencyOrder(t *testing.T) {
	q := &stubQuerier{}
	data, err := Run(t.Context(), q, validSpec(), map[string]any{"states": []any{"CA"}}, RunOptions{OptionsOnly: true})
	require.NoError(t, err)

	assert.Nil(t, data.Sections, "options_only skips the sections")
	require.Len(t, data.Controls, 3)
	assert.Equal(t, []Option{{Value: "CA"}, {Value: "NY"}}, data.Controls[0].Options)
	assert.Equal(t, []Option{{Value: "Los Angeles"}}, data.Controls[1].Options)
	assert.Empty(t, data.Controls[2].Options, "the range control has no option list")

	// The dependent city query must have been offered the states value.
	call, ok := q.callFor("cities")
	require.True(t, ok)
	assert.Equal(t, []any{"CA"}, call.vars["states"])
}

func TestRunKeepsASectionErrorInsideTheSection(t *testing.T) {
	s := validSpec()
	s.Sections[1].Query = `query { op { by_month_boom { key { m } } } }`
	s.Sections[1].JQ = ""
	s.Sections[1].Chart = &ChartSpec{Type: "line", X: "m", Y: []string{"v"}}

	q := &stubQuerier{}
	data, err := Run(t.Context(), q, s, nil, RunOptions{})
	require.NoError(t, err, "a broken section must not take the document down")

	assert.NotEmpty(t, data.Sections[1].Error)
	assert.Empty(t, data.Sections[0].Error)
	assert.Empty(t, data.Sections[2].Error)
}

func TestRunRefusesAnUnboundedLargeTable(t *testing.T) {
	s := validSpec()
	s.Sections[2].Query = `query { m { obj_wide_open { i } } }`

	q := &stubQuerier{}
	data, err := Run(t.Context(), q, s, nil, RunOptions{})
	require.NoError(t, err)
	assert.Contains(t, data.Sections[2].Error, "sets no bound of its own")
	assert.Contains(t, data.Sections[2].Error, "AGGREGATE", "the refusal carries the way out")
}

func TestBindVariables(t *testing.T) {
	t.Run("unknown variable", func(t *testing.T) {
		_, err := Run(t.Context(), &stubQuerier{}, validSpec(), map[string]any{"region": "west"}, RunOptions{})
		require.ErrorContains(t, err, "$region is not declared")
	})

	t.Run("empty string becomes null, empty array stays", func(t *testing.T) {
		// The engine contract: only null widens; an explicit `in: []`
		// deliberately matches nothing (a cleared multiselect submits null
		// from the panel itself).
		s := validSpec()
		bound, err := s.bindVariables(map[string]any{"states": []any{}, "city": ""})
		require.NoError(t, err)
		assert.Equal(t, []any{}, bound["states"])
		assert.Nil(t, bound["city"])
	})

	t.Run("required dotted bind is enforced", func(t *testing.T) {
		s := validSpec()
		s.Variables = append(s.Variables, Variable{Name: "flt", Type: "flt_input"})
		s.Controls = append(s.Controls, Control{Label: "Range", Kind: "daterange", Required: true,
			Bind: Bind{From: "flt.from", To: "flt.to"}})
		_, err := s.bindVariables(map[string]any{"flt": map[string]any{"from": "2023-01-01"}})
		require.ErrorContains(t, err, `control "Range" is required`)

		bound, err := s.bindVariables(map[string]any{"flt": map[string]any{"from": "2023-01-01", "to": "2023-06-30"}})
		require.NoError(t, err)
		assert.NotNil(t, bound["flt"])
	})

	t.Run("template wraps query values, echo stays raw", func(t *testing.T) {
		s := validSpec()
		s.Variables = append(s.Variables, Variable{Name: "q", Type: "String"})
		s.Controls = append(s.Controls, Control{Label: "Search", Kind: "search",
			Bind: Bind{Target: "q"}, Template: "%{value}%"})
		bound, err := s.bindVariables(map[string]any{"q": "acme"})
		require.NoError(t, err)
		qv := s.applyTemplates(bound)
		assert.Equal(t, "%acme%", qv["q"], "the query side gets the wrapped value")
		assert.Equal(t, "acme", bound["q"], "the echo keeps what the user typed")
		assert.Nil(t, qv["states"], "untouched variables ride through unchanged")
	})

	t.Run("required control refuses a cleared value", func(t *testing.T) {
		s := validSpec()
		_, err := s.bindVariables(map[string]any{"date_from": nil})
		require.ErrorContains(t, err, `control "Period" is required`)
	})

	t.Run("required variable names its control", func(t *testing.T) {
		s := validSpec()
		s.Variables[1].Required = true // city, bound by the City control
		_, err := s.bindVariables(nil)
		require.ErrorContains(t, err, `$city is required`)
		require.ErrorContains(t, err, `control "City"`)
	})

	t.Run("range must be ordered", func(t *testing.T) {
		s := validSpec()
		_, err := s.bindVariables(map[string]any{"date_from": "2023-12-31", "date_to": "2023-01-01"})
		require.ErrorContains(t, err, `control "Period"`)
		require.ErrorContains(t, err, "greater than")
	})

	t.Run("min max", func(t *testing.T) {
		s := validSpec()
		s.Variables = append(s.Variables, Variable{Name: "top", Type: "Int"})
		mn, mx := 1.0, 100.0
		s.Controls = append(s.Controls, Control{Label: "Top N", Kind: "number", Bind: Bind{Target: "top"}, Min: &mn, Max: &mx})
		_, err := s.bindVariables(map[string]any{"top": 500.0})
		require.ErrorContains(t, err, `control "Top N"`)
		require.ErrorContains(t, err, "above max")
	})

	t.Run("explicit null clears a default", func(t *testing.T) {
		s := validSpec()
		s.Controls[2].Required = false
		bound, err := s.bindVariables(map[string]any{"date_from": nil})
		require.NoError(t, err)
		assert.Nil(t, bound["date_from"])
		assert.Equal(t, "2023-12-31", bound["date_to"], "the untouched default stays")
	})
}

// A spec variable that happens to be called `limit` must not mask a section
// query's own literal bound — the variable map is shared by every section.
func TestRunAtLimitIgnoresForeignLimitVariable(t *testing.T) {
	s := validSpec()
	s.Variables = append(s.Variables, Variable{Name: "limit", Type: "Int", Default: float64(500)})
	data, err := Run(t.Context(), &stubQuerier{}, s, nil, RunOptions{})
	require.NoError(t, err)
	assert.True(t, data.Sections[2].AtLimit,
		"the table section is bounded by its own `limit: 50`, not by the spec's $limit")
}

func TestCanonicalOptions(t *testing.T) {
	opts, err := canonicalOptions(map[string]any{"data": map[string]any{"m": []any{"a", 2.0, map[string]any{"value": "x", "label": "X"}}}})
	require.NoError(t, err)
	assert.Equal(t, []Option{{Value: "a"}, {Value: 2.0}, {Value: "x", Label: "X"}}, opts)

	_, err = canonicalOptions("nope")
	require.ErrorContains(t, err, "array of scalars")

	_, err = canonicalOptions([]any{map[string]any{"label": "x"}})
	require.ErrorContains(t, err, "no value")

	_, err = canonicalOptions([]any{map[string]any{"value": 1.0, "weight": 2.0}})
	require.ErrorContains(t, err, `unknown field "weight"`)

	_, err = canonicalOptions([]any{map[string]any{"value": map[string]any{"id": 1.0}, "label": "Berlin"}})
	require.ErrorContains(t, err, "must be a scalar")

	big := make([]any, OptionsCap+1)
	for i := range big {
		big[i] = float64(i)
	}
	_, err = canonicalOptions(big)
	require.ErrorContains(t, err, "narrow the query")
}
