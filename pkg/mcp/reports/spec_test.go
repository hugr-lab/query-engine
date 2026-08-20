package reports

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validSpec is the design example made real: typed variables, dependent
// option lists, a range control filling two variables, and all four section
// kinds on the grid.
func validSpec() *Spec {
	return &Spec{
		Title: "Open Payments — quarterly review",
		Variables: []Variable{
			{Name: "states", Type: "[String!]"},
			{Name: "city", Type: "String"},
			{Name: "date_from", Type: "Date", Default: "2023-01-01"},
			{Name: "date_to", Type: "Date", Default: "2023-12-31"},
		},
		Controls: []Control{
			{Label: "States", Kind: "multiselect", Bind: Bind{Target: "states"},
				OptionsQuery: &OptionsQuery{
					Query: `query { op { states_agg { key { st } } } }`,
					JQ:    ".data.op.states_agg | map(.key.st)",
				}},
			{Label: "City", Kind: "select", Bind: Bind{Target: "city"},
				OptionsQuery: &OptionsQuery{
					Query: `query($states: [String!]) { op { cities(filter: {st: {in: $states}}) { name } } }`,
					JQ:    ".data.op.cities | map(.name)",
				}},
			{Label: "Period", Kind: "daterange", Bind: Bind{From: "date_from", To: "date_to"}, Required: true},
		},
		Sections: []Section{
			{Kind: "kpi", Title: "Headline",
				Query: `query($states: [String!]) { op { total: payments_aggregation(filter: {st: {in: $states}}) { amount { sum } } } }`,
				JQ:    `[{label: "Total", value: .data.op.total.amount.sum, unit: "$"}]`},
			{Kind: "chart", Title: "Trend", Width: "two_thirds",
				Chart: &ChartSpec{Type: "line", X: "month", Y: []string{"total"}},
				Query: `query($city: String) { op { by_month(filter: {city: {eq: $city}}) { key { m } aggregations { s { sum } } } } }`,
				JQ:    ".data.op.by_month | map({month: .key.m, total: .aggregations.s.sum})"},
			{Kind: "table", Title: "Top providers", Width: "third",
				Columns: []ColumnSpec{{Field: "name"}, {Field: "total", Format: "number"}},
				Query:   `query { op { providers(limit: 50) { name total } } }`},
			{Kind: "text", Markdown: "## Notes\nNarrative between the numbers."},
		},
	}
}

func TestValidSpecValidates(t *testing.T) {
	s := validSpec()
	require.NoError(t, s.Validate())

	assert.Equal(t, DefaultTimeout, s.Timeout())
	assert.Equal(t, 12, s.Sections[0].GridSpan())
	assert.Equal(t, 8, s.Sections[1].GridSpan())
	assert.Equal(t, 4, s.Sections[2].GridSpan())

	s.TimeoutSec = 30
	assert.Equal(t, 30*time.Second, s.Timeout())
}

func TestParseIsStrict(t *testing.T) {
	// A typo must fail loudly, not silently drop the intent — and the error
	// must name the PLACE and the vocabulary: a bare `json: unknown field`
	// sent the model guessing through invented shapes on the live run.
	_, err := Parse([]byte(`{"title": "t", "sections": [{"kind": "table", "colums": [], "query": "{ m { obj(limit: 5) { a } } }"}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `sections[0] has unknown field "colums"`)
	assert.Contains(t, err.Error(), "columns", "the allowed list is the fix")

	// The exact miss from the live run: a control described with
	// name/variable instead of bind — it gets the SPELLED-OUT fix, because
	// the generic unknown-field error was read as "controls are broken".
	_, err = Parse([]byte(`{"title": "t",
		"variables": [{"name": "d", "type": "Date"}],
		"controls": [{"control": "date", "label": "С даты", "name": "d", "variable": "d"}],
		"sections": [{"kind": "table", "query": "{ m { obj(limit: 5) { a } } }"}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `controls[0] has "name"`)
	assert.Contains(t, err.Error(), `drop "name" and keep bind`)

	_, err = Parse([]byte(`{"title": "t", "sections": [{"kind": "chart",
		"chart": {"type": "line", "x": "a", "y": ["b"], "colour": "red"},
		"query": "{ m { obj { a } } }"}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `sections[0].chart has unknown field "colour"`)

	s, err := Parse([]byte(`{"title": "t", "sections": [{"kind": "table", "query": "{ m { obj(limit: 5) { a } } }"}]}`))
	require.NoError(t, err)
	assert.Len(t, s.Sections, 1)
}

func TestValidateVariables(t *testing.T) {
	s := validSpec()
	s.Variables[0].Name = "2states"
	require.ErrorContains(t, s.Validate(), "not a valid variable name")

	s = validSpec()
	s.Variables[1].Name = "states"
	require.ErrorContains(t, s.Validate(), `"states" is declared twice`)

	s = validSpec()
	s.Variables[0].Type = "[String!"
	err := s.Validate()
	require.ErrorContains(t, err, "not a GraphQL type reference")
	require.ErrorContains(t, err, `"states"`)

	s = validSpec()
	s.Variables[0].Type = "String) { x } query($y: Int"
	require.ErrorContains(t, s.Validate(), "not a GraphQL type reference")
}

func TestValidateSectionQueries(t *testing.T) {
	s := validSpec()
	s.Sections[0].Query = `query($year: Int) { op { t: agg { c } } }`
	err := s.Validate()
	require.ErrorContains(t, err, "$year which the spec does not define")
	require.ErrorContains(t, err, `section 0 ("Headline")`)

	s = validSpec()
	s.Sections[0].Query = `query($states: String) { op { t: agg { c } } }`
	require.ErrorContains(t, s.Validate(), "declares $states as String but the spec types it [String!]")

	// A nullable variable cannot feed a non-null declaration…
	s = validSpec()
	s.Sections[1].Query = `query($city: String!) { op { t { c } } }`
	require.ErrorContains(t, s.Validate(), "marked required")

	// …unless it IS required, or the declaration carries its own default.
	s = validSpec()
	s.Sections[1].Query = `query($city: String!) { op { t { c } } }`
	s.Variables[1].Required = true
	require.NoError(t, s.Validate())

	s = validSpec()
	s.Sections[1].Query = `query($city: String! = "all") { op { t { c } } }`
	require.NoError(t, s.Validate())

	s = validSpec()
	s.Sections[2].Query = `mutation { m { delete_obj(filter: {}) { affected_rows } } }`
	require.ErrorContains(t, s.Validate(), "read-only")

	s = validSpec()
	s.Sections[2].Query = `query { a { b } } query { c { d } }`
	require.ErrorContains(t, s.Validate(), "exactly one operation")

	s = validSpec()
	s.Sections[2].Query = `query { unbalanced`
	require.ErrorContains(t, s.Validate(), "does not parse")
}

func TestValidateSectionShapes(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*Spec)
		wantErr string
	}{
		"no sections":      {func(s *Spec) { s.Sections = nil }, "at least one section"},
		"no title":         {func(s *Spec) { s.Title = " " }, "title is required"},
		"bad kind":         {func(s *Spec) { s.Sections[0].Kind = "graph" }, `kind "graph" is not one of`},
		"width and span":   {func(s *Spec) { s.Sections[1].Span = 6 }, "at most one"},
		"bad width":        {func(s *Spec) { s.Sections[1].Width = "wide" }, `width "wide"`},
		"bad span":         {func(s *Spec) { s.Sections[1].Width = ""; s.Sections[1].Span = 13 }, "1..12"},
		"bad page break":   {func(s *Spec) { s.Sections[0].PageBreak = "after" }, `only "before"`},
		"text w/o md":      {func(s *Spec) { s.Sections[3].Markdown = "" }, "carries markdown"},
		"text with query":  {func(s *Spec) { s.Sections[3].Query = "{ a { b } }" }, "markdown only"},
		"md outside text":  {func(s *Spec) { s.Sections[0].Markdown = "x" }, "markdown belongs to text"},
		"no query":         {func(s *Spec) { s.Sections[0].Query = "" }, "fed by its query"},
		"chart w/o chart":  {func(s *Spec) { s.Sections[1].Chart = nil }, "needs its chart mapping"},
		"chart bad type":   {func(s *Spec) { s.Sections[1].Chart.Type = "spline" }, `chart.type "spline"`},
		"chart w/o x":      {func(s *Spec) { s.Sections[1].Chart.X = "" }, "chart.x is required"},
		"chart w/o y":      {func(s *Spec) { s.Sections[1].Chart.Y = nil }, "at least one value field"},
		"chart w/ columns": {func(s *Spec) { s.Sections[1].Columns = []ColumnSpec{{Field: "a"}} }, "its own section"},
		"table w/ chart":   {func(s *Spec) { s.Sections[2].Chart = &ChartSpec{Type: "line", X: "a", Y: []string{"b"}} }, "chart belongs to chart sections"},
		"kpi w/ columns":   {func(s *Spec) { s.Sections[0].Columns = []ColumnSpec{{Field: "a"}} }, "query + jq only"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			tc.mutate(s)
			require.ErrorContains(t, s.Validate(), tc.wantErr)
		})
	}

	s := validSpec()
	for len(s.Sections) <= MaxSections {
		s.Sections = append(s.Sections, Section{Kind: "text", Markdown: "x"})
	}
	require.ErrorContains(t, s.Validate(), "at most")
}

func TestValidateControls(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*Spec)
		wantErr string
	}{
		"no label":       {func(s *Spec) { s.Controls[0].Label = "" }, "label is required"},
		"bad kind":       {func(s *Spec) { s.Controls[0].Kind = "combo" }, `"combo" is not one of`},
		"no bind":        {func(s *Spec) { s.Controls[0].Bind = Bind{} }, "bind is required"},
		"range scalar":   {func(s *Spec) { s.Controls[2].Bind = Bind{Target: "date_from"} }, "binds {from, to}"},
		"scalar range":   {func(s *Spec) { s.Controls[1].Bind = Bind{From: "date_from", To: "date_to"} }, "belongs to numrange/daterange"},
		"unknown var":    {func(s *Spec) { s.Controls[1].Bind.Target = "region" }, `unknown variable "region"`},
		"bad path":       {func(s *Spec) { s.Controls[1].Bind.Target = "city..x" }, "not a variable name or dotted path"},
		"bad template":   {func(s *Spec) { s.Controls[1].Template = "%value%" }, "{value}"},
		"min above max":  {func(s *Spec) { mn, mx := 10.0, 1.0; s.Controls[1].Min, s.Controls[1].Max = &mn, &mx }, "min is greater than max"},
		"options + oq":   {func(s *Spec) { s.Controls[1].Options = []Option{{Value: "a"}} }, "mutually exclusive"},
		"oq unknown var": {func(s *Spec) { s.Controls[1].OptionsQuery.Query = `query($region: String) { a { b } }` }, "$region which the spec does not define"},
		"oq empty":       {func(s *Spec) { s.Controls[1].OptionsQuery.Query = " " }, "options_query.query is required"},
		"self dep": {func(s *Spec) {
			s.Controls[1].OptionsQuery.Query = `query($city: String) { op { cities(filter: {near: {eq: $city}}) { name } } }`
		}, "the very variable it fills"},
		"jq self dep": {func(s *Spec) {
			s.Controls[1].OptionsQuery = &OptionsQuery{Query: "query { op { cities { name } } }", JQ: "map(select(. != $city))"}
		}, "the very variable it fills"},
		"duplicate bind": {func(s *Spec) {
			s.Controls = append(s.Controls, Control{Label: "City 2", Kind: "select", Bind: Bind{Target: "city"}})
		}, `both bind "city"`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			tc.mutate(s)
			require.ErrorContains(t, s.Validate(), tc.wantErr)
		})
	}

	// A two-control cycle: states' options depend on city, city's on states.
	s := validSpec()
	s.Controls[0].OptionsQuery = &OptionsQuery{
		Query: `query($city: String) { op { states(filter: {city: {eq: $city}}) { st } } }`,
	}
	require.ErrorContains(t, s.Validate(), "dependency cycle")
}

// The exact control shapes the live model reached for and declared "rejected
// in any form" — minus the invented "name" field, which is what actually got
// them rejected. Every legitimate shape must pass wire-to-validate.
func TestLiveControlShapesParse(t *testing.T) {
	s, err := Parse([]byte(`{
		"title": "op2023 — обзор",
		"variables": [
			{"name": "states", "type": "[String!]"},
			{"name": "date_from", "type": "Date", "default": "2023-01-01"},
			{"name": "date_to", "type": "Date", "default": "2023-12-31"}
		],
		"controls": [
			{"label": "Штат", "control": "multiselect", "bind": "states",
			 "options": [{"label": "CA", "value": "CA"}, {"label": "NY", "value": "NY"}, "TX"]},
			{"label": "Период", "control": "daterange", "bind": {"from": "date_from", "to": "date_to"}, "required": true}
		],
		"sections": [
			{"kind": "kpi", "title": "Итоги",
			 "jq": "[{label: \"Записей\", value: .data.op2023.gp._rows_count}]",
			 "query": "query($states: [String!], $date_from: Date, $date_to: Date) { op2023 { gp: general_payments_aggregation(filter: {Recipient_State: {in: $states}, Date_of_Payment: {gte: $date_from, lte: $date_to}}) { _rows_count } } }"}
		]}`))
	require.NoError(t, err)
	require.Len(t, s.Controls, 2)
	assert.Equal(t, "CA", s.Controls[0].Options[0].Value)
	assert.Equal(t, "TX", s.Controls[0].Options[2].Value)

	// Plain single-date controls, one per variable.
	_, err = Parse([]byte(`{
		"title": "t",
		"variables": [
			{"name": "date_from", "type": "Date", "default": "2023-01-01"},
			{"name": "date_to", "type": "Date", "default": "2023-12-31"}
		],
		"controls": [
			{"label": "С даты", "control": "date", "bind": "date_from"},
			{"label": "По дату", "control": "date", "bind": "date_to"}
		],
		"sections": [
			{"kind": "table", "query": "query($date_from: Date, $date_to: Date) { m { obj(limit: 5) { a } } }"}
		]}`))
	require.NoError(t, err)
}

func TestBindJSON(t *testing.T) {
	var b Bind
	require.NoError(t, json.Unmarshal([]byte(`"states"`), &b))
	assert.Equal(t, []string{"states"}, b.Targets())
	assert.False(t, b.IsRange())

	require.NoError(t, json.Unmarshal([]byte(`{"from": "a", "to": "b"}`), &b))
	assert.True(t, b.IsRange())
	assert.Equal(t, []string{"a", "b"}, b.Targets())

	assert.ErrorContains(t, json.Unmarshal([]byte(`{"from": "a"}`), &Bind{}), "bind.to")
	assert.ErrorContains(t, json.Unmarshal([]byte(`{"from": "a", "to": "b", "x": 1}`), &Bind{}), `unknown field "x"`)
	assert.ErrorContains(t, json.Unmarshal([]byte(`5`), &Bind{}), "variable name")

	out, err := json.Marshal(Bind{From: "a", To: "b"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"from": "a", "to": "b"}`, string(out))
}

func TestOptionJSON(t *testing.T) {
	var o Option
	require.NoError(t, json.Unmarshal([]byte(`"CA"`), &o))
	assert.Equal(t, "CA", o.Value)

	require.NoError(t, json.Unmarshal([]byte(`{"value": 1, "label": "one"}`), &o))
	assert.Equal(t, 1.0, o.Value)
	assert.Equal(t, "one", o.Label)

	assert.ErrorContains(t, json.Unmarshal([]byte(`[1]`), &Option{}), "scalar")
	assert.ErrorContains(t, json.Unmarshal([]byte(`{"label": "x"}`), &Option{}), "needs a value")
	assert.ErrorContains(t, json.Unmarshal([]byte(`{"value": 1, "weight": 2}`), &Option{}), `unknown field "weight"`)
	assert.ErrorContains(t, json.Unmarshal([]byte(`{"value": {"id": 1}, "label": "x"}`), &Option{}), "must be a scalar")
}
