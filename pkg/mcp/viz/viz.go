// Package viz is the visualization core shared by the MCP viz tools
// (pkg/mcp) and the report renderer (pkg/mcp/reports): the canonical data
// contracts — flat rows and KPI cards — the chart/column specs, the caps, and
// the query+jq pipeline that feeds them (query.go). It deliberately knows
// nothing about MCP itself: tool surfaces, result envelopes and the HTML views
// stay with their owners. The package lives under pkg/mcp because that is
// where these contracts were born — they are the viz tools' wire format first,
// and the report spec reuses them verbatim.
package viz

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	// KPICap bounds a KPI panel: it is a glance surface, not a table. The
	// whole panel always travels inline — no sampling, no refresh channel —
	// and the cap is what keeps that honest.
	KPICap = 32

	// Above the soft limit a browser table stops being usable and the payload
	// stops being reasonable; the hard cap is what a view will never be asked
	// to hold. Between them sits a window the caller opened deliberately with
	// limit/offset, and that is their call to make.
	RowSoftLimit = 2000
	RowHardCap   = 10000
)

var (
	ChartTypes    = []string{"line", "bar", "area", "pie", "scatter"}
	KPIDirections = []string{"up_good", "down_good", "neutral"}
	KPIFormats    = []string{"number", "percent"}

	kpiCardKeys = []string{"label", "value", "unit", "format", "delta", "delta_pct", "direction", "trend", "subtitle"}
)

type ChartSpec struct {
	Type    string   `json:"type" jsonschema_description:"line | bar | area | pie | scatter"`
	X       string   `json:"x,omitempty" jsonschema_description:"Row field for the category/x value"`
	Y       []string `json:"y,omitempty" jsonschema_description:"Row fields holding values; several = one series each (wide form)"`
	Series  string   `json:"series,omitempty" jsonschema_description:"Row field whose distinct values become series (long form; uses y[0] as the value)"`
	Stacked bool     `json:"stacked,omitempty" jsonschema_description:"Stack the series"`
}

type ColumnSpec struct {
	Field  string `json:"field" jsonschema_description:"Row field"`
	Label  string `json:"label,omitempty" jsonschema_description:"Header label (default: field name)"`
	Format string `json:"format,omitempty" jsonschema_description:"number to right-align and group digits"`
	Align  string `json:"align,omitempty" jsonschema_description:"left | right"`
}

// KPI is one card on a KPI panel. Label and value are the card; everything
// else decorates it.
type KPI struct {
	Label     string    `json:"label" jsonschema_description:"Card caption"`
	Value     any       `json:"value" jsonschema_description:"The headline value: a number, or a short string"`
	Unit      string    `json:"unit,omitempty" jsonschema_description:"Rendered next to the value ($, %, ms, …)"`
	Format    string    `json:"format,omitempty" jsonschema_description:"number | percent"`
	Delta     *float64  `json:"delta,omitempty" jsonschema_description:"Absolute change vs the comparison period"`
	DeltaPct  *float64  `json:"delta_pct,omitempty" jsonschema_description:"Change in percent vs the comparison period"`
	Direction string    `json:"direction,omitempty" jsonschema_description:"up_good | down_good | neutral — colours the delta"`
	Trend     []float64 `json:"trend,omitempty" jsonschema_description:"Values drawn as a sparkline under the number"`
	Subtitle  string    `json:"subtitle,omitempty" jsonschema_description:"Small line under the value (e.g. \"vs July\")"`
}

// CanonicalRows validates the canonical-rows contract: an array of flat
// objects with scalar values. A single-key object chain (the usual
// {"module":{"table":[...]}} GraphQL shape) is unwrapped first, so most
// queries need no jq at all.
func CanonicalRows(v any) ([]map[string]any, error) {
	for {
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			break
		}
		for _, inner := range m {
			v = inner
		}
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected an array of row objects after unwrapping, got %s — use jq_transform to shape the result into [{...}, ...]", jsonKind(v))
	}
	rows := make([]map[string]any, 0, len(arr))
	for i, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("row %d is a %s, not an object — use jq_transform to produce objects like {\"x\": ..., \"y\": ...}", i, jsonKind(item))
		}
		for k, fv := range m {
			switch fv.(type) {
			case nil, string, float64, bool, json.Number:
			default:
				// The fix differs by shape, and a wrong hint costs the caller
				// a whole round trip: an object needs a field picked out of
				// it, a list needs collapsing to one scalar.
				hint := fmt.Sprintf("pick a scalar out of it, e.g. {%s: .%s.<field>}", k, k)
				if _, isArr := fv.([]any); isArr {
					hint = fmt.Sprintf("collapse it to one value, e.g. {%s: (.%s | length)} or {%s: ([.%s[].<field>] | join(\", \"))}", k, k, k, k)
				}
				return nil, fmt.Errorf("row %d field %q holds a nested %s — use jq_transform to %s, or drop the field", i, k, jsonKind(fv), hint)
			}
		}
		rows = append(rows, m)
	}
	return rows, nil
}

// CanonicalKPIs validates the canonical KPI-card contract: an array of card
// objects, label+value required, trend the only array field. Unknown keys are
// rejected by name — a typo like delta_percent must fail loudly with the
// allowed list, not render a bare card.
func CanonicalKPIs(v any) ([]KPI, error) {
	for {
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			break
		}
		for _, inner := range m {
			v = inner
		}
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected an array of KPI cards, got %s — shape the result into [{\"label\": ..., \"value\": ...}, ...] (with a query, jq_transform is where the cards are assembled)", jsonKind(v))
	}
	if len(arr) == 0 {
		return nil, fmt.Errorf("no KPI cards — the panel needs at least one {label, value}")
	}
	if len(arr) > KPICap {
		return nil, fmt.Errorf("%d KPI cards — a panel reads at %d at most; aggregate further or split into panels", len(arr), KPICap)
	}

	str := func(m map[string]any, key string, i int) (string, error) {
		fv, ok := m[key]
		if !ok || fv == nil {
			return "", nil
		}
		s, ok := fv.(string)
		if !ok {
			return "", fmt.Errorf("card %d: %s must be a string, got %s", i, key, jsonKind(fv))
		}
		return s, nil
	}
	num := func(m map[string]any, key string, i int) (*float64, error) {
		fv, ok := m[key]
		if !ok || fv == nil {
			return nil, nil
		}
		f, ok := fv.(float64)
		if !ok {
			return nil, fmt.Errorf("card %d: %s must be a number, got %s", i, key, jsonKind(fv))
		}
		return &f, nil
	}

	kpis := make([]KPI, 0, len(arr))
	for i, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("card %d is a %s, not an object — cards look like {\"label\": ..., \"value\": ...}", i, jsonKind(item))
		}
		for k := range m {
			if !slices.Contains(kpiCardKeys, k) {
				return nil, fmt.Errorf("card %d has unknown field %q — allowed: %s", i, k, strings.Join(kpiCardKeys, ", "))
			}
		}
		var c KPI
		var err error
		if c.Label, err = str(m, "label", i); err != nil {
			return nil, err
		}
		if c.Label == "" {
			return nil, fmt.Errorf("card %d: label is required", i)
		}
		switch val := m["value"].(type) {
		case string, float64, bool, json.Number:
			c.Value = val
		case nil:
			return nil, fmt.Errorf("card %d (%s): value is required", i, c.Label)
		default:
			return nil, fmt.Errorf("card %d (%s): value holds a nested %s — a card's value is one scalar", i, c.Label, jsonKind(val))
		}
		if c.Unit, err = str(m, "unit", i); err != nil {
			return nil, err
		}
		if c.Format, err = str(m, "format", i); err != nil {
			return nil, err
		}
		if c.Format != "" && !slices.Contains(KPIFormats, c.Format) {
			return nil, fmt.Errorf("card %d (%s): format %q is not one of %s", i, c.Label, c.Format, strings.Join(KPIFormats, "|"))
		}
		if c.Delta, err = num(m, "delta", i); err != nil {
			return nil, err
		}
		if c.DeltaPct, err = num(m, "delta_pct", i); err != nil {
			return nil, err
		}
		if c.Direction, err = str(m, "direction", i); err != nil {
			return nil, err
		}
		if c.Direction != "" && !slices.Contains(KPIDirections, c.Direction) {
			return nil, fmt.Errorf("card %d (%s): direction %q is not one of %s", i, c.Label, c.Direction, strings.Join(KPIDirections, "|"))
		}
		if c.Subtitle, err = str(m, "subtitle", i); err != nil {
			return nil, err
		}
		if tv, ok := m["trend"]; ok && tv != nil {
			ta, ok := tv.([]any)
			if !ok {
				return nil, fmt.Errorf("card %d (%s): trend must be an array of numbers, got %s", i, c.Label, jsonKind(tv))
			}
			c.Trend = make([]float64, len(ta))
			for j, t := range ta {
				f, ok := t.(float64)
				if !ok {
					return nil, fmt.Errorf("card %d (%s): trend[%d] is a %s, not a number", i, c.Label, j, jsonKind(t))
				}
				c.Trend[j] = f
			}
		}
		kpis = append(kpis, c)
	}
	return kpis, nil
}

// JQPathHint recovers the first two selection levels of the query so a failed
// transform can show the caller the path it probably meant to write.
func JQPathHint(query string) string {
	fields := make([]string, 0, 2)
	for _, chunk := range strings.Split(query, "{")[1:] {
		f := strings.TrimSpace(chunk)
		if i := strings.IndexAny(f, " \t\n({"); i >= 0 {
			f = f[:i]
		}
		if f == "" || strings.HasPrefix(f, "$") {
			continue
		}
		fields = append(fields, f)
		if len(fields) == 2 {
			break
		}
	}
	if len(fields) == 0 {
		return "<root field>"
	}
	return strings.Join(fields, ".")
}

func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case bool:
		return "boolean"
	default:
		return "number"
	}
}

// QueryHasLimit reports whether the caller bounded the query themselves,
// either with a $limit variable or a literal limit: argument. It is a textual
// check on purpose: the query belongs to the caller and is not re-parsed here.
func QueryHasLimit(query string) bool {
	return strings.Contains(query, "$limit") || strings.Contains(query, "limit:")
}

var literalLimit = regexp.MustCompile(`\blimit\s*:\s*(\d+)`)

// QueryLimit recovers the row bound the caller set, from a literal `limit: N`
// or from the value bound to a $limit variable. Knowing it is what lets the
// result say "this page is exactly full, there is probably more" instead of
// leaving the caller to guess from a suspiciously round number.
func QueryLimit(query string, vars map[string]any) (int, bool) {
	if v, ok := vars["limit"]; ok {
		switch n := v.(type) {
		case int:
			return n, true
		case int64:
			return int(n), true
		case float64:
			return int(n), true
		}
	}
	// The innermost limit is the one that bounds the rows we get back.
	if m := literalLimit.FindAllStringSubmatch(query, -1); len(m) > 0 {
		if n, err := strconv.Atoi(m[len(m)-1][1]); err == nil {
			return n, true
		}
	}
	return 0, false
}

// PagingRecipe is the one place that spells out how to bound a query, so
// every refusal points the same way.
const PagingRecipe = `Three ways, best first: (1) AGGREGATE — a chart wants ` +
	`<object>_bucket_aggregation, not raw rows, and an aggregate is usually what the question ` +
	`actually asked for; (2) NARROW — put a condition in the query's own filter argument, which ` +
	`is also how "only for X" is answered; (3) PAGE — obj(limit: 500, offset: 0) with order_by ` +
	`so the order is stable, then call again with offset: 500 for the next page, each call ` +
	`rendering its own view. Any limit you set is honoured, including a deliberate "first N of ` +
	`the count above" — just tell the user that is what they are looking at.`
