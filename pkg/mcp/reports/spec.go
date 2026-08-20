// Package reports implements design/039: a report is a pure function of
// (spec, variables, identity). The JSON spec describes sections — KPI panels,
// charts, tables, narrative text — each fed by its own GraphQL query + jq
// transform over the shared variable set; controls describe the filter panel
// as a separate layer over the variables. Nothing is stored: the spec travels
// with every call. This PR ships the MCP surface only — the HTTP endpoint and
// stored specifications come later with the stored-reports line.
package reports

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/hugr-lab/query-engine/pkg/mcp/viz"
)

const (
	// MaxSections bounds a report: a document, not a dashboard farm.
	MaxSections = 24

	// DefaultTimeout is the shared deadline for one report run — every
	// section query, options query and jq transform together. pkg/jq has no
	// execution budget of its own yet, so the report-level deadline is what
	// keeps a fan-out of N sections × M aliases bounded.
	DefaultTimeout = 60 * time.Second
)

var (
	SectionKinds = []string{"kpi", "chart", "table", "text"}

	// ControlKinds mirror hugr's filter operators one to one: eq → select/
	// number/date/toggle, in → multiselect, gte+lte → numrange/daterange,
	// ilike → search (wrapped by the control's template).
	ControlKinds = []string{"select", "multiselect", "search", "number", "numrange", "date", "daterange", "toggle"}

	// widthSpans is the human layout vocabulary, sugar over a 12-column grid.
	widthSpans = map[string]int{
		"full":       12,
		"two_thirds": 8,
		"half":       6,
		"third":      4,
		"quarter":    3,
	}
)

// The chart mapping and the column list are the 038 contracts verbatim; the
// aliases let a spec be built without importing pkg/mcp/viz.
type (
	ChartSpec  = viz.ChartSpec
	ColumnSpec = viz.ColumnSpec
)

// Spec is the report definition. It is canonical JSON with 038-grade
// validation: precise errors naming the section, control and field.
type Spec struct {
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	Variables   []Variable `json:"variables,omitempty"`
	Controls    []Control  `json:"controls,omitempty"`
	Sections    []Section  `json:"sections"`
	// TimeoutSec overrides the shared run deadline (default 60), capped by
	// the server's own limit at run time.
	TimeoutSec int `json:"timeout_seconds,omitempty"`
}

// Variable is a plain, typed query parameter. Nothing about UI here — the
// panel is the controls' business. Type is a hugr GraphQL type reference in
// query syntax (String, Int, Date, [String!], an ENUM or INPUT type name);
// values are coerced by the engine's own schema machinery at run time, so the
// spec carries no second type system.
type Variable struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Default applies when no value is submitted at all; an explicit null
	// stays null (that is how a filter is cleared).
	Default  any  `json:"default,omitempty"`
	Required bool `json:"required,omitempty"`
}

// Control is one element of the filter panel — a separate layer OVER the
// variables. One control may fill several targets (a date range fills two);
// a variable bound by no control is simply fixed by its default.
type Control struct {
	Label string `json:"label"`
	// Kind is one of ControlKinds; empty means inferred from the bound
	// leaf's GraphQL type where it is known.
	Kind     string `json:"control,omitempty"`
	Bind     Bind   `json:"bind"`
	Required bool   `json:"required,omitempty"`
	// Template wraps the raw input for the QUERIES — the placeholder is
	// {value}, so "%{value}%" turns a search box into an ilike argument.
	// Applied server-side at bind time; the echoed variables stay raw, so
	// the panel always shows what the user typed.
	Template string   `json:"template,omitempty"`
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`
	// Options (static) and OptionsQuery (resolved by a data query) are
	// mutually exclusive; an enum-typed bind needs neither — its options
	// come from the schema.
	Options      []Option      `json:"options,omitempty"`
	OptionsQuery *OptionsQuery `json:"options_query,omitempty"`
}

// Bind is a control's target: a scalar target (a variable name, or a dotted
// path into an input-typed variable, "flt.period.from") or a {from, to} pair
// for range controls. In JSON it is a string or an object — never both.
type Bind struct {
	Target string
	From   string
	To     string
}

func (b *Bind) UnmarshalJSON(data []byte) error {
	*b = Bind{}
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '"' {
		return json.Unmarshal(data, &b.Target)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("bind must be a variable name/path or {\"from\": ..., \"to\": ...}")
	}
	for k := range m {
		if k != "from" && k != "to" {
			return fmt.Errorf("bind object has unknown field %q — only from and to are allowed", k)
		}
	}
	if err := json.Unmarshal(m["from"], &b.From); err != nil || b.From == "" {
		return fmt.Errorf("bind.from must be a variable name/path")
	}
	if err := json.Unmarshal(m["to"], &b.To); err != nil || b.To == "" {
		return fmt.Errorf("bind.to must be a variable name/path")
	}
	return nil
}

func (b Bind) MarshalJSON() ([]byte, error) {
	if b.Target != "" {
		return json.Marshal(b.Target)
	}
	return json.Marshal(map[string]string{"from": b.From, "to": b.To})
}

// IsRange reports whether the bind is the {from, to} form.
func (b Bind) IsRange() bool { return b.Target == "" && b.From != "" }

// Targets lists the bound paths — one for a scalar bind, two for a range.
func (b Bind) Targets() []string {
	if b.IsRange() {
		return []string{b.From, b.To}
	}
	if b.Target == "" {
		return nil
	}
	return []string{b.Target}
}

// Option is one static choice: a bare scalar in JSON, or {value, label}.
type Option struct {
	Value any    `json:"value"`
	Label string `json:"label,omitempty"`
}

func (o *Option) UnmarshalJSON(data []byte) error {
	*o = Option{}
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '{' {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		for k := range m {
			if k != "value" && k != "label" {
				return fmt.Errorf("option object has unknown field %q — only value and label are allowed", k)
			}
		}
		if raw, ok := m["value"]; ok {
			if err := json.Unmarshal(raw, &o.Value); err != nil {
				return err
			}
		}
		if raw, ok := m["label"]; ok {
			if err := json.Unmarshal(raw, &o.Label); err != nil {
				return fmt.Errorf("option label must be a string")
			}
		}
		if o.Value == nil {
			return fmt.Errorf("option needs a value")
		}
		switch o.Value.(type) {
		case string, float64, bool:
		default:
			return fmt.Errorf("option value must be a scalar (string, number or boolean)")
		}
		return nil
	}
	if err := json.Unmarshal(data, &o.Value); err != nil {
		return err
	}
	switch o.Value.(type) {
	case string, float64, bool:
		return nil
	default:
		return fmt.Errorf("an option is a scalar or {\"value\": ..., \"label\": ...}")
	}
}

// OptionsQuery resolves a control's choices from data: the query runs with
// the CURRENT variable values (so dependent lists come for free), jq shapes
// the result into a list of scalars or {value, label} objects.
type OptionsQuery struct {
	Query string `json:"query"`
	JQ    string `json:"jq,omitempty"`
}

// Section is one block of the document. kpi/chart/table sections are fed by
// their own query + jq producing the 038 canonical shape; text sections are
// markdown. A chart with its companion table is deliberately two sections
// side by side, not a composite.
type Section struct {
	Kind        string `json:"kind"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	// Width is the human layout word (full | two_thirds | half | third |
	// quarter); Span is the fine-grained 1..12 alternative. At most one.
	Width string `json:"width,omitempty"`
	Span  int    `json:"span,omitempty"`
	// PageBreak "before" forces a page break in print.
	PageBreak string `json:"page_break,omitempty"`

	Query    string       `json:"query,omitempty"`
	JQ       string       `json:"jq,omitempty"`
	Chart    *ChartSpec   `json:"chart,omitempty"`
	Columns  []ColumnSpec `json:"columns,omitempty"`
	Markdown string       `json:"markdown,omitempty"`
}

// GridSpan resolves the section's grid width in columns (12 = full row).
func (s *Section) GridSpan() int {
	if s.Span > 0 {
		return s.Span
	}
	if n, ok := widthSpans[s.Width]; ok {
		return n
	}
	return 12
}

// Timeout is the run deadline the spec asks for; the runner caps it.
func (s *Spec) Timeout() time.Duration {
	if s.TimeoutSec > 0 {
		return time.Duration(s.TimeoutSec) * time.Second
	}
	return DefaultTimeout
}

// Parse decodes a spec strictly — unknown fields are rejected, so a typo like
// "colums" or an invented control field fails loudly instead of silently
// dropping the intent — and validates it. Strictness is a hand-rolled walk
// rather than DisallowUnknownFields because the stdlib error carries no path
// and no vocabulary: `json: unknown field "name"` sends the caller guessing,
// while `controls[0] has unknown field "name" — allowed: …, bind, …` is a
// one-round-trip fix.
func Parse(data []byte) (*Spec, error) {
	if err := checkSpecFields(data); err != nil {
		return nil, err
	}
	var s Spec
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("report spec: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// The spec vocabulary, one list per object kind — what the strict walk
// reports when it meets a field it does not know.
var (
	specKeys         = []string{"title", "description", "variables", "controls", "sections", "timeout_seconds"}
	variableKeys     = []string{"name", "type", "default", "required"}
	controlKeys      = []string{"label", "control", "bind", "required", "template", "min", "max", "options", "options_query"}
	optionsQueryKeys = []string{"query", "jq"}
	sectionKeys      = []string{"kind", "title", "description", "width", "span", "page_break", "query", "jq", "chart", "columns", "markdown"}
	chartKeys        = []string{"type", "x", "y", "series", "stacked"}
	columnKeys       = []string{"field", "label", "format", "align"}
)

func checkSpecFields(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("report spec: %w", err)
	}
	if err := unknownKey("the spec", raw, specKeys); err != nil {
		return err
	}
	for i, v := range asObjects(raw["variables"]) {
		if err := unknownKey(fmt.Sprintf("variables[%d]", i), v, variableKeys); err != nil {
			return err
		}
	}
	for i, c := range asObjects(raw["controls"]) {
		path := fmt.Sprintf("controls[%d]", i)
		// The one miss every model makes: carrying the viz-filter shape over.
		// The generic unknown-field error was read as "controls are broken",
		// so this one spells out the fix.
		for _, k := range []string{"name", "field"} {
			if _, ok := c[k]; ok {
				return fmt.Errorf("report spec: %s has %q, which report controls do not have — unlike viz-chart filters, the only target reference is 'bind' (drop %q and keep bind: \"<variable>\")", path, k, k)
			}
		}
		if err := unknownKey(path, c, controlKeys); err != nil {
			return err
		}
		if oq, ok := c["options_query"].(map[string]any); ok {
			if err := unknownKey(path+".options_query", oq, optionsQueryKeys); err != nil {
				return err
			}
		}
	}
	for i, sec := range asObjects(raw["sections"]) {
		path := fmt.Sprintf("sections[%d]", i)
		if err := unknownKey(path, sec, sectionKeys); err != nil {
			return err
		}
		if ch, ok := sec["chart"].(map[string]any); ok {
			if err := unknownKey(path+".chart", ch, chartKeys); err != nil {
				return err
			}
		}
		for j, col := range asObjects(sec["columns"]) {
			if err := unknownKey(fmt.Sprintf("%s.columns[%d]", path, j), col, columnKeys); err != nil {
				return err
			}
		}
	}
	return nil
}

func asObjects(v any) []map[string]any {
	arr, _ := v.([]any)
	out := make([]map[string]any, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func unknownKey(path string, m map[string]any, allowed []string) error {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic: the same typo always names itself first
	for _, k := range keys {
		if !slices.Contains(allowed, k) {
			return fmt.Errorf("report spec: %s has unknown field %q — allowed: %s", path, k, strings.Join(allowed, ", "))
		}
	}
	return nil
}
