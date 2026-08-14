package reports

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hugr-lab/query-engine/pkg/mcp/viz"
	"golang.org/x/sync/errgroup"
)

const (
	// OptionsCap bounds one control's option list: a dropdown, not a table.
	// Exceeding it is an error asking to narrow the query — silently cutting
	// the list would offer the user a choice that quietly is not all of them.
	OptionsCap = 500

	// maxTimeoutCap is the server-side ceiling on the spec's own timeout
	// when RunOptions does not set one.
	maxTimeoutCap = 5 * time.Minute

	// sectionConcurrency bounds the fan-out: one report may hold up to 24
	// section queries, and each already runs its own aliased aggregations in
	// parallel inside the engine.
	sectionConcurrency = 8
)

// RunOptions is what the runner takes from the deployment, not the caller.
type RunOptions struct {
	// QueryTTL is the cache hint for every query this run executes
	// (MCP_QUERY_TTL) — a re-render with unchanged variables becomes cache
	// reads.
	QueryTTL time.Duration
	// MaxTimeout caps the spec's requested deadline; 0 means the built-in
	// 5-minute ceiling.
	MaxTimeout time.Duration
	// OptionsOnly resolves the control option lists and skips the sections —
	// the light mode a changed parent variable needs to refresh dependent
	// lists without re-running the whole report.
	OptionsOnly bool
}

// ReportData is one run's outcome: the variable values actually used, the
// resolved option lists (index-aligned with spec.Controls) and the section
// results (index-aligned with spec.Sections). A section that failed carries
// its error INSIDE — one broken query must not take the document down.
type ReportData struct {
	Variables map[string]any   `json:"variables"`
	Controls  []ControlOptions `json:"controls,omitempty"`
	Sections  []SectionData    `json:"sections,omitempty"`
}

// ControlOptions is the resolved option list of one control; empty for
// controls with static options or none.
type ControlOptions struct {
	Label   string   `json:"label"`
	Options []Option `json:"options,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// SectionData is one section's canonical result — rows for charts and
// tables, cards for KPI panels, nothing for text.
type SectionData struct {
	Kind      string           `json:"kind"`
	Rows      []map[string]any `json:"rows,omitempty"`
	Kpis      []viz.KPI        `json:"kpis,omitempty"`
	RowCount  int              `json:"row_count,omitempty"`
	Truncated bool             `json:"truncated,omitempty"`
	AtLimit   bool             `json:"at_limit,omitempty"`
	// RowsSampled is set only on the wire copy the model (and the first
	// widget render) receives; the view pulls the full rows itself.
	RowsSampled bool   `json:"rows_sampled,omitempty"`
	Error       string `json:"error,omitempty"`
}

// Run executes the report: bind and check the submitted variables, resolve
// option lists in dependency order, then run every section concurrently
// under one shared deadline. The spec is re-validated — a run is a pure
// function of (spec, variables, identity) and trusts nothing it was handed.
func Run(ctx context.Context, q viz.Querier, spec *Spec, vars map[string]any, opts RunOptions) (*ReportData, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	bound, err := spec.bindVariables(vars)
	if err != nil {
		return nil, err
	}

	timeout := spec.Timeout()
	if cap := opts.MaxTimeout; cap <= 0 {
		if timeout > maxTimeoutCap {
			timeout = maxTimeoutCap
		}
	} else if timeout > cap {
		timeout = cap
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	data := &ReportData{Variables: bound}
	if len(spec.Controls) > 0 {
		data.Controls = spec.resolveOptions(ctx, q, bound, opts.QueryTTL)
	}
	if opts.OptionsOnly {
		return data, nil
	}

	data.Sections = make([]SectionData, len(spec.Sections))
	var g errgroup.Group
	g.SetLimit(sectionConcurrency)
	for i := range spec.Sections {
		sec, out := &spec.Sections[i], &data.Sections[i]
		out.Kind = sec.Kind
		if sec.Kind == "text" {
			continue
		}
		g.Go(func() error {
			runSection(ctx, q, sec, bound, opts.QueryTTL, out)
			return nil
		})
	}
	g.Wait() //nolint:errcheck // section errors land inside the sections
	return data, nil
}

func runSection(ctx context.Context, q viz.Querier, sec *Section, vars map[string]any, ttl time.Duration, out *SectionData) {
	switch sec.Kind {
	case "kpi":
		v, err := viz.QueryValue(ctx, q, sec.Query, vars, sec.JQ, ttl)
		if err != nil {
			out.Error = err.Error()
			return
		}
		kpis, err := viz.CanonicalKPIs(v)
		if err != nil {
			if sec.JQ == "" {
				err = fmt.Errorf("%w — a kpi section needs jq to assemble the cards (paths start with .data, e.g. .data.%s)", err, viz.JQPathHint(sec.Query))
			}
			out.Error = err.Error()
			return
		}
		out.Kpis = kpis
	case "chart":
		// Uncapped, like 038: a chart's volume is the author's call and the
		// rows never transit a conversation.
		rows, _, err := viz.QueryRows(ctx, q, sec.Query, vars, sec.JQ, 0, ttl)
		if err != nil {
			out.Error = err.Error()
			return
		}
		out.Rows, out.RowCount = rows, len(rows)
	case "table":
		rows, truncated, err := viz.QueryRows(ctx, q, sec.Query, vars, sec.JQ, viz.RowHardCap, ttl)
		if err != nil {
			out.Error = err.Error()
			return
		}
		// The 038 volume rule per section: an unbounded query that turns out
		// large is refused with the way out, never silently cut.
		if len(rows) >= viz.RowSoftLimit && !viz.QueryHasLimit(sec.Query) {
			out.Error = fmt.Sprintf(
				"the query returned %d rows and sets no bound of its own. Up to %d render fine; beyond that say what to show rather than shipping everything. %s",
				len(rows), viz.RowSoftLimit, viz.PagingRecipe)
			return
		}
		out.Rows, out.RowCount, out.Truncated = rows, len(rows), truncated
		if limit, known := viz.QueryLimit(sec.Query, vars); known && len(rows) == limit && len(rows) > 0 {
			out.AtLimit = true
		}
	}
}

// bindVariables merges the submitted values over the spec defaults and
// re-checks what the panel promises: unknown names are rejected, empty
// strings and empty arrays become null (an empty IN () must never reach the
// engine; null drops the condition instead), required variables must hold a
// value, ranges must be ordered, min/max must hold.
func (s *Spec) bindVariables(in map[string]any) (map[string]any, error) {
	vars, err := s.varInfos()
	if err != nil {
		return nil, err
	}
	for name := range in {
		if _, ok := vars[name]; !ok {
			return nil, fmt.Errorf("variables: $%s is not declared by the spec", name)
		}
	}

	bound := make(map[string]any, len(s.Variables))
	for _, v := range s.Variables {
		val, provided := in[v.Name]
		if !provided {
			// Absence takes the default; an explicit null stays null — that
			// is how a filter is cleared.
			val = v.Default
		}
		bound[v.Name] = normalizeEmpty(val)
	}

	for _, v := range s.Variables {
		if vars[v.Name].required && bound[v.Name] == nil {
			if label := s.controlFor(v.Name); label != "" {
				return nil, fmt.Errorf("variable $%s is required — set it via control %q", v.Name, label)
			}
			return nil, fmt.Errorf("variable $%s is required and has no value", v.Name)
		}
	}

	for i := range s.Controls {
		if err := s.Controls[i].checkValues(bound); err != nil {
			return nil, err
		}
	}
	return bound, nil
}

// normalizeEmpty maps "" and [] to null at the top level. The engine drops a
// null-valued condition; an empty array would compile into IN ().
func normalizeEmpty(v any) any {
	switch t := v.(type) {
	case string:
		if t == "" {
			return nil
		}
	case []any:
		if len(t) == 0 {
			return nil
		}
	}
	return v
}

// controlFor names the control that fills the variable, for error messages.
func (s *Spec) controlFor(varName string) string {
	for i := range s.Controls {
		for _, path := range s.Controls[i].Bind.Targets() {
			if strings.SplitN(path, ".", 2)[0] == varName {
				return s.Controls[i].Label
			}
		}
	}
	return ""
}

// checkValues re-validates the control's own promises against the bound
// values. Only plain-variable targets are checkable here — a dotted path
// lands inside an input object the schema owns.
func (c *Control) checkValues(bound map[string]any) error {
	value := func(path string) (any, bool) {
		if strings.Contains(path, ".") {
			return nil, false
		}
		v, ok := bound[path]
		return v, ok && v != nil
	}
	if c.Required {
		// Required exists because null silently WIDENS: a dropped predicate
		// means the section quietly computes over the whole table.
		for _, path := range c.Bind.Targets() {
			if strings.Contains(path, ".") {
				continue
			}
			if bound[path] == nil {
				return fmt.Errorf("control %q is required — it needs a value", c.Label)
			}
		}
	}
	if c.Bind.IsRange() {
		from, okF := value(c.Bind.From)
		to, okT := value(c.Bind.To)
		if okF && okT && !rangeOrdered(from, to) {
			return fmt.Errorf("control %q: from (%v) is greater than to (%v)", c.Label, from, to)
		}
	}
	if c.Min == nil && c.Max == nil {
		return nil
	}
	for _, path := range c.Bind.Targets() {
		v, ok := value(path)
		if !ok {
			continue
		}
		n, ok := v.(float64)
		if !ok {
			continue
		}
		if c.Min != nil && n < *c.Min {
			return fmt.Errorf("control %q: %v is below min %v", c.Label, n, *c.Min)
		}
		if c.Max != nil && n > *c.Max {
			return fmt.Errorf("control %q: %v is above max %v", c.Label, n, *c.Max)
		}
	}
	return nil
}

// rangeOrdered reports from ≤ to where the two are comparable: numbers
// numerically, strings lexically (ISO dates and timestamps order correctly
// that way). Incomparable pairs pass — the engine's coercion owns them.
func rangeOrdered(from, to any) bool {
	if f, ok := from.(float64); ok {
		if t, ok := to.(float64); ok {
			return f <= t
		}
	}
	if f, ok := from.(string); ok {
		if t, ok := to.(string); ok {
			return f <= t
		}
	}
	return true
}

// resolveOptions runs the option queries in dependency order, so a control
// whose list depends on another variable sees that variable's current value.
// An option failure stays on its control — the report still renders.
func (s *Spec) resolveOptions(ctx context.Context, q viz.Querier, bound map[string]any, ttl time.Duration) []ControlOptions {
	out := make([]ControlOptions, len(s.Controls))
	for i := range s.Controls {
		out[i].Label = s.Controls[i].Label
	}
	for _, i := range s.optionOrder() {
		c := &s.Controls[i]
		if c.OptionsQuery == nil {
			continue
		}
		v, err := viz.QueryValue(ctx, q, c.OptionsQuery.Query, bound, c.OptionsQuery.JQ, ttl)
		if err != nil {
			out[i].Error = err.Error()
			continue
		}
		opts, err := canonicalOptions(v)
		if err != nil {
			out[i].Error = fmt.Sprintf("control %q: %s", c.Label, err)
			continue
		}
		out[i].Options = opts
	}
	return out
}

// optionOrder is the topological order of controls by their option
// dependencies (validation guaranteed a DAG). Independent controls keep
// their spec order.
func (s *Spec) optionOrder() []int {
	fills := map[string]int{} // variable → control that binds it
	for i := range s.Controls {
		for _, path := range s.Controls[i].Bind.Targets() {
			fills[strings.SplitN(path, ".", 2)[0]] = i
		}
	}
	waits := make([][]int, len(s.Controls)) // control → controls it waits for
	for i := range s.Controls {
		for _, dep := range s.Controls[i].optionDeps() {
			if j, ok := fills[dep]; ok && j != i {
				waits[i] = append(waits[i], j)
			}
		}
	}
	order := make([]int, 0, len(s.Controls))
	done := make([]bool, len(s.Controls))
	for range s.Controls { // ≤ N rounds; validation rejected cycles
		for i := range s.Controls {
			if done[i] {
				continue
			}
			ready := true
			for _, j := range waits[i] {
				if !done[j] {
					ready = false
					break
				}
			}
			if ready {
				done[i] = true
				order = append(order, i)
			}
		}
	}
	return order
}

// optionDeps lists the variable names the control's option list depends on:
// the options query's declared variables plus any $name its jq mentions.
// The spec is already validated, so parse errors cannot happen here.
func (c *Control) optionDeps() []string {
	if c.OptionsQuery == nil {
		return nil
	}
	var deps []string
	if op, err := parseOneQuery(c.OptionsQuery.Query); err == nil {
		for _, vd := range op.VariableDefinitions {
			deps = append(deps, vd.Variable)
		}
	}
	for _, m := range jqVarRef.FindAllStringSubmatch(c.OptionsQuery.JQ, -1) {
		found := false
		for _, d := range deps {
			if d == m[1] {
				found = true
				break
			}
		}
		if !found {
			deps = append(deps, m[1])
		}
	}
	return deps
}

// canonicalOptions validates an option list: an array of scalars, or of
// {value, label} objects — after the usual single-key unwrap.
func canonicalOptions(v any) ([]Option, error) {
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
		return nil, fmt.Errorf("the options query must produce an array of scalars or {value, label} objects — shape it with jq")
	}
	if len(arr) > OptionsCap {
		return nil, fmt.Errorf("the options query returned %d options — a control offers %d at most; narrow the query", len(arr), OptionsCap)
	}
	opts := make([]Option, 0, len(arr))
	for i, item := range arr {
		switch t := item.(type) {
		case string, float64, bool:
			opts = append(opts, Option{Value: t})
		case map[string]any:
			val, ok := t["value"]
			if !ok || val == nil {
				return nil, fmt.Errorf("option %d has no value", i)
			}
			o := Option{Value: val}
			if l, ok := t["label"]; ok && l != nil {
				ls, ok := l.(string)
				if !ok {
					return nil, fmt.Errorf("option %d: label must be a string", i)
				}
				o.Label = ls
			}
			for k := range t {
				if k != "value" && k != "label" {
					return nil, fmt.Errorf("option %d has unknown field %q — only value and label are allowed", i, k)
				}
			}
			opts = append(opts, o)
		default:
			return nil, fmt.Errorf("option %d is not a scalar or a {value, label} object", i)
		}
	}
	return opts, nil
}
