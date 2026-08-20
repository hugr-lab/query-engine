package reports

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/mcp/viz"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

var (
	graphQLName  = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)
	typeRefChars = regexp.MustCompile(`^[\[\]!_0-9A-Za-z]+$`)
	jqVarRef     = regexp.MustCompile(`\$([_A-Za-z][_0-9A-Za-z]*)`)
)

type varInfo struct {
	typ      *ast.Type
	required bool
}

// varInfos parses the declared variables into their type references. A
// non-null type counts as required: GraphQL coercion refuses null for it
// anyway, so the render refuses earlier and names the variable.
func (s *Spec) varInfos() (map[string]varInfo, error) {
	vars := make(map[string]varInfo, len(s.Variables))
	for i, v := range s.Variables {
		if !graphQLName.MatchString(v.Name) {
			return nil, fmt.Errorf("variables[%d]: %q is not a valid variable name", i, v.Name)
		}
		if _, dup := vars[v.Name]; dup {
			return nil, fmt.Errorf("variable %q is declared twice", v.Name)
		}
		typ, err := parseTypeRef(v.Type)
		if err != nil {
			return nil, fmt.Errorf("variable %q: type %q is not a GraphQL type reference (like String, Date or [String!])", v.Name, v.Type)
		}
		vars[v.Name] = varInfo{typ: typ, required: v.Required || typ.NonNull}
	}
	return vars, nil
}

// Validate checks the spec fail-fast with errors that name the section,
// control and field — the 038 discipline: a vague error costs the caller a
// whole round trip. It is spec-internal on purpose: whether a named type
// exists, and whether a value coerces into it, is the engine schema's
// business at run time.
func (s *Spec) Validate() error {
	if strings.TrimSpace(s.Title) == "" {
		return fmt.Errorf("report title is required")
	}
	if s.TimeoutSec < 0 {
		return fmt.Errorf("timeout_seconds must not be negative")
	}

	vars, err := s.varInfos()
	if err != nil {
		return err
	}

	if len(s.Sections) == 0 {
		return fmt.Errorf("a report needs at least one section")
	}
	if len(s.Sections) > MaxSections {
		return fmt.Errorf("%d sections — a report holds %d at most; split the document", len(s.Sections), MaxSections)
	}
	for i := range s.Sections {
		if err := validateSection(i, &s.Sections[i], vars); err != nil {
			return err
		}
	}

	// The options dependency graph: an edge D → V says "the control filling V
	// resolves its options with D's current value". It must be a DAG, or
	// render-time resolution has no order to run in.
	edges := map[string][]string{}
	boundBy := map[string]string{} // full bound path → control label
	for i := range s.Controls {
		c := &s.Controls[i]
		deps, err := validateControl(i, c, vars)
		if err != nil {
			return err
		}
		for _, path := range c.Bind.Targets() {
			if prev, dup := boundBy[path]; dup {
				return fmt.Errorf("controls %q and %q both bind %q — one target, one control", prev, c.Label, path)
			}
			boundBy[path] = c.Label
			bound := strings.SplitN(path, ".", 2)[0]
			for _, dep := range deps {
				if dep == bound {
					return fmt.Errorf("control %q: its options depend on the very variable it fills ($%s)", c.Label, dep)
				}
				edges[dep] = append(edges[dep], bound)
			}
		}
	}
	if cyc := findCycle(edges); cyc != "" {
		return fmt.Errorf("controls form an options dependency cycle involving variable %q", cyc)
	}
	return nil
}

func validateSection(i int, sec *Section, vars map[string]varInfo) error {
	name := fmt.Sprintf("section %d", i)
	if sec.Title != "" {
		name = fmt.Sprintf("section %d (%q)", i, sec.Title)
	}
	if !slices.Contains(SectionKinds, sec.Kind) {
		return fmt.Errorf("%s: kind %q is not one of %s", name, sec.Kind, strings.Join(SectionKinds, "|"))
	}
	if sec.Width != "" && sec.Span != 0 {
		return fmt.Errorf("%s: width and span are two spellings of one thing — set at most one", name)
	}
	if sec.Width != "" {
		if _, ok := widthSpans[sec.Width]; !ok {
			return fmt.Errorf("%s: width %q is not one of full|two_thirds|half|third|quarter (use span: 1..12 for finer control)", name, sec.Width)
		}
	}
	if sec.Span < 0 || sec.Span > 12 {
		return fmt.Errorf("%s: span must be 1..12 grid columns", name)
	}
	if sec.PageBreak != "" && sec.PageBreak != "before" {
		return fmt.Errorf("%s: page_break %q — only \"before\" is supported", name, sec.PageBreak)
	}

	if sec.Kind == "text" {
		if strings.TrimSpace(sec.Markdown) == "" {
			return fmt.Errorf("%s: a text section carries markdown", name)
		}
		if sec.Query != "" || sec.JQ != "" || sec.Chart != nil || len(sec.Columns) > 0 {
			return fmt.Errorf("%s: a text section carries markdown only — no query, jq, chart or columns", name)
		}
		return nil
	}

	if sec.Markdown != "" {
		return fmt.Errorf("%s: markdown belongs to text sections", name)
	}
	if strings.TrimSpace(sec.Query) == "" {
		return fmt.Errorf("%s: a %s section is fed by its query", name, sec.Kind)
	}
	switch sec.Kind {
	case "chart":
		if sec.Chart == nil {
			return fmt.Errorf("%s: a chart section needs its chart mapping", name)
		}
		if !slices.Contains(viz.ChartTypes, sec.Chart.Type) {
			return fmt.Errorf("%s: chart.type %q is not one of %s", name, sec.Chart.Type, strings.Join(viz.ChartTypes, "|"))
		}
		if sec.Chart.X == "" {
			return fmt.Errorf("%s: chart.x is required", name)
		}
		if len(sec.Chart.Y) == 0 {
			return fmt.Errorf("%s: chart.y needs at least one value field (long form uses y[0])", name)
		}
		if len(sec.Columns) > 0 {
			return fmt.Errorf("%s: columns belong to table sections — a chart's companion table is its own section", name)
		}
	case "table":
		if sec.Chart != nil {
			return fmt.Errorf("%s: chart belongs to chart sections", name)
		}
	case "kpi":
		if sec.Chart != nil || len(sec.Columns) > 0 {
			return fmt.Errorf("%s: a kpi section carries query + jq only — no chart or columns", name)
		}
	}

	op, err := parseOneQuery(sec.Query)
	if err != nil {
		return fmt.Errorf("%s: query %s", name, err)
	}
	for _, vd := range op.VariableDefinitions {
		vi, ok := vars[vd.Variable]
		if !ok {
			return fmt.Errorf("%s: query declares $%s which the spec does not define", name, vd.Variable)
		}
		if !varAssignable(vi, vd) {
			return fmt.Errorf("%s: query declares $%s as %s but the spec types it %s — an optional (nullable) variable cannot feed a non-null declaration unless it is marked required", name, vd.Variable, vd.Type.String(), vi.typ.String())
		}
	}
	return nil
}

// validateControl checks one control and returns the variables its option
// list depends on — the declared variables of its options query, plus any
// $name the options jq mentions (jq sees every variable, declared or not).
func validateControl(i int, c *Control, vars map[string]varInfo) ([]string, error) {
	if c.Label == "" {
		return nil, fmt.Errorf("controls[%d]: label is required", i)
	}
	name := fmt.Sprintf("control %q", c.Label)
	if c.Kind != "" && !slices.Contains(ControlKinds, c.Kind) {
		return nil, fmt.Errorf("%s: control %q is not one of %s", name, c.Kind, strings.Join(ControlKinds, "|"))
	}
	targets := c.Bind.Targets()
	if len(targets) == 0 {
		return nil, fmt.Errorf("%s: bind is required — a variable name/path, or {from, to} for ranges", name)
	}
	isRange := c.Kind == "numrange" || c.Kind == "daterange"
	if isRange && !c.Bind.IsRange() {
		return nil, fmt.Errorf("%s: a %s control binds {from, to} — two targets, one value each", name, c.Kind)
	}
	if c.Kind != "" && !isRange && c.Bind.IsRange() {
		return nil, fmt.Errorf("%s: a {from, to} bind belongs to numrange/daterange controls, not %s", name, c.Kind)
	}
	for _, path := range targets {
		for _, seg := range strings.Split(path, ".") {
			if !graphQLName.MatchString(seg) {
				return nil, fmt.Errorf("%s: bind %q is not a variable name or dotted path", name, path)
			}
		}
		if _, ok := vars[strings.SplitN(path, ".", 2)[0]]; !ok {
			return nil, fmt.Errorf("%s: bind %q targets unknown variable %q", name, path, strings.SplitN(path, ".", 2)[0])
		}
	}
	// The placeholder is {value} — NOT %{value}: with the latter the leading
	// per-cent of an ilike template ("%{value}%") is eaten by the
	// placeholder itself and the wrap comes out one-sided ("acme%").
	if c.Template != "" && !strings.Contains(c.Template, "{value}") {
		return nil, fmt.Errorf("%s: template must contain {value} — it wraps the raw input (e.g. \"%%{value}%%\" for ilike)", name)
	}
	if c.Min != nil && c.Max != nil && *c.Min > *c.Max {
		return nil, fmt.Errorf("%s: min is greater than max", name)
	}
	if len(c.Options) > 0 && c.OptionsQuery != nil {
		return nil, fmt.Errorf("%s: options and options_query are mutually exclusive", name)
	}

	if c.OptionsQuery == nil {
		return nil, nil
	}
	if strings.TrimSpace(c.OptionsQuery.Query) == "" {
		return nil, fmt.Errorf("%s: options_query.query is required", name)
	}
	op, err := parseOneQuery(c.OptionsQuery.Query)
	if err != nil {
		return nil, fmt.Errorf("%s: options query %s", name, err)
	}
	var deps []string
	for _, vd := range op.VariableDefinitions {
		vi, ok := vars[vd.Variable]
		if !ok {
			return nil, fmt.Errorf("%s: options query declares $%s which the spec does not define", name, vd.Variable)
		}
		if !varAssignable(vi, vd) {
			return nil, fmt.Errorf("%s: options query declares $%s as %s but the spec types it %s", name, vd.Variable, vd.Type.String(), vi.typ.String())
		}
		deps = append(deps, vd.Variable)
	}
	for _, m := range jqVarRef.FindAllStringSubmatch(c.OptionsQuery.JQ, -1) {
		if _, ok := vars[m[1]]; ok && !slices.Contains(deps, m[1]) {
			deps = append(deps, m[1])
		}
	}
	return deps, nil
}

// parseTypeRef reads a type reference through the real GraphQL grammar by
// wrapping it into a variable declaration — one parser, no second type
// syntax. The character guard keeps the wrapping airtight.
func parseTypeRef(ref string) (*ast.Type, error) {
	if !typeRefChars.MatchString(ref) {
		return nil, fmt.Errorf("invalid type reference")
	}
	doc, err := parser.ParseQuery(&ast.Source{Input: "query($v: " + ref + ") { f }"})
	if err != nil || len(doc.Operations) != 1 || len(doc.Operations[0].VariableDefinitions) != 1 {
		return nil, fmt.Errorf("invalid type reference")
	}
	return doc.Operations[0].VariableDefinitions[0].Type, nil
}

func parseOneQuery(src string) (*ast.OperationDefinition, error) {
	doc, err := parser.ParseQuery(&ast.Source{Input: src})
	if err != nil {
		return nil, fmt.Errorf("does not parse: %s", err)
	}
	if len(doc.Operations) != 1 {
		return nil, fmt.Errorf("must contain exactly one operation, got %d", len(doc.Operations))
	}
	op := doc.Operations[0]
	if op.Operation != ast.Query {
		return nil, fmt.Errorf("must be a query — a report is read-only, %s operations are refused", op.Operation)
	}
	return op, nil
}

// varAssignable says whether the spec variable can feed the query's
// declaration. gqlparser's own compatibility rule does the shape and
// nullability work; on top of it, a required variable counts as non-null
// (the render refuses to run without it), and a declaration with its own
// default tolerates absence by definition.
func varAssignable(vi varInfo, vd *ast.VariableDefinition) bool {
	decl := vd.Type
	if vd.DefaultValue != nil && decl.NonNull {
		d := *decl
		d.NonNull = false
		decl = &d
	}
	if vi.typ.IsCompatible(decl) {
		return true
	}
	if vi.required && !vi.typ.NonNull {
		nn := *vi.typ
		nn.NonNull = true
		return nn.IsCompatible(decl)
	}
	return false
}

// findCycle runs a colored DFS over the dependency edges and names one
// variable on a cycle, or returns "".
func findCycle(edges map[string][]string) string {
	const (
		white = 0
		grey  = 1
		black = 2
	)
	color := map[string]int{}
	var visit func(n string) string
	visit = func(n string) string {
		color[n] = grey
		for _, next := range edges[n] {
			switch color[next] {
			case grey:
				return next
			case white:
				if c := visit(next); c != "" {
					return c
				}
			}
		}
		color[n] = black
		return ""
	}
	for n := range edges {
		if color[n] == white {
			if c := visit(n); c != "" {
				return c
			}
		}
	}
	return ""
}
