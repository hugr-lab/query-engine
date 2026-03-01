package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// --- Position helpers ---

// compiledPos creates an ast.Position tagged with a compiled-instruction source.
func compiledPos(name string) *ast.Position {
	src := "compiled-instruction"
	if name != "" {
		src = "compiled-instruction-" + name
	}
	return &ast.Position{Src: &ast.Source{Name: src}}
}

// --- Directive builders ---

// catalogDirective creates a @catalog directive with name and engine.
func catalogDirective(name, engine string) *ast.Directive {
	pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}
	return &ast.Directive{
		Name: "catalog",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "engine", Value: &ast.Value{Raw: engine, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// optsCatalogDirective creates a @catalog directive from compile options.
func optsCatalogDirective(opts base.Options) *ast.Directive {
	return catalogDirective(opts.Name, opts.EngineType)
}

// fieldAggregationDirective creates a @field_aggregation(name=X) directive.
func fieldAggregationDirective(name string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: "field_aggregation",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// --- Definition predicates ---

// isVirtualField returns true for fields that are "virtual" — they don't correspond
// to real database columns and should be excluded from filters and mutation inputs.
// This includes @function_call, @table_function_call_join, and @join fields.
func isVirtualField(f *ast.FieldDefinition) bool {
	return f.Directives.ForName("function_call") != nil ||
		f.Directives.ForName("table_function_call_join") != nil ||
		f.Directives.ForName("join") != nil
}

// isTableOrView returns true if the definition is a @table or @view.
func isTableOrView(def *ast.Definition) bool {
	return def.Directives.ForName("table") != nil || def.Directives.ForName("view") != nil
}

// lookupObjectDef looks up a definition by name, checking compilation output first,
// then falling back to source definitions. This handles cases where a structural
// Object (e.g. DictionaryRecordsData) hasn't been processed by PassthroughRule yet
// when a table referencing it is compiled during the same GENERATE phase.
func lookupObjectDef(ctx base.CompilationContext, name string) *ast.Definition {
	if def := ctx.LookupType(name); def != nil {
		return def
	}
	return ctx.Source().ForName(ctx.Context(), name)
}

// addModuleToFuncCallDirective adds module=<name> to a function_call or table_function_call_join directive.
func addModuleToFuncCallDirective(f *ast.FieldDefinition, moduleName string) {
	for _, dirName := range []string{"function_call", "table_function_call_join"} {
		d := f.Directives.ForName(dirName)
		if d == nil {
			continue
		}
		if a := d.Arguments.ForName(base.ArgModule); a != nil {
			a.Value.Raw = moduleName
		} else {
			d.Arguments = append(d.Arguments, &ast.Argument{
				Name:     "module",
				Value:    &ast.Value{Kind: ast.StringValue, Raw: moduleName},
				Position: d.Position,
			})
		}
	}
}

// --- Argument definition builders ---

// queryArgs returns the standard query arguments for list queries.
func queryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return queryArgsWithViewArgs(nil, filterName, pos)
}

// queryArgsWithViewArgs returns standard query arguments, optionally prepending
// an "args" parameter for parameterized views (@args directive).
func queryArgsWithViewArgs(info *base.ObjectInfo, filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	var args ast.ArgumentDefinitionList
	if info != nil && info.InputArgsName != "" {
		var argType *ast.Type
		if info.RequiredArgs {
			argType = ast.NonNullNamedType(info.InputArgsName, pos)
		} else {
			argType = ast.NamedType(info.InputArgsName, pos)
		}
		args = append(args, &ast.ArgumentDefinition{
			Name:        "args",
			Description: base.DescArgs,
			Type:        argType,
			Position:    pos,
		})
	}
	args = append(args,
		&ast.ArgumentDefinition{Name: "filter", Description: base.DescFilter, Type: ast.NamedType(filterName, pos), Position: pos},
		&ast.ArgumentDefinition{Name: "order_by", Description: base.DescOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		&ast.ArgumentDefinition{Name: "limit", Description: base.DescLimit, Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "2000", Kind: ast.IntValue}},
		&ast.ArgumentDefinition{Name: "offset", Description: base.DescOffset, Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "0", Kind: ast.IntValue}},
		&ast.ArgumentDefinition{Name: "distinct_on", Description: base.DescDistinctOn, Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
	)
	return args
}

// SubQueryArgs returns the standard query arguments for sub-queries (references).
func SubQueryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	args := queryArgs(filterName, pos)
	args = append(args,
		&ast.ArgumentDefinition{
			Name: "inner", Description: base.DescInnerJoinRef, Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue},
		},
		&ast.ArgumentDefinition{
			Name: "nested_order_by", Description: base.DescNestedOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos,
		},
		&ast.ArgumentDefinition{
			Name: "nested_limit", Description: base.DescNestedLimit, Type: ast.NamedType("Int", pos), Position: pos,
		},
		&ast.ArgumentDefinition{
			Name: "nested_offset", Description: base.DescNestedOffset, Type: ast.NamedType("Int", pos), Position: pos,
		},
	)
	return args
}

// CloneArgDefs creates a shallow copy of an argument definition list.
func CloneArgDefs(args ast.ArgumentDefinitionList, _ *ast.Position) ast.ArgumentDefinitionList {
	if len(args) == 0 {
		return nil
	}
	out := make(ast.ArgumentDefinitionList, len(args))
	copy(out, args)
	return out
}

// AggRefArgs returns reference field args on aggregation types:
// filter + order_by + distinct_on + inner + nested_*.
func AggRefArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "filter", Description: base.DescFilter, Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Description: base.DescOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "distinct_on", Description: base.DescDistinctOn, Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Description: base.DescInnerJoin, Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Description: base.DescNestedOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Description: base.DescNestedLimit, Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Description: base.DescNestedOffset, Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// AggSubRefArgs returns args for the _aggregation sub-field on aggregation types.
// Includes filter + order_by + limit/offset + distinct_on + inner + nested_*.
func AggSubRefArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "filter", Description: base.DescFilter, Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Description: base.DescOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "limit", Description: base.DescLimit, Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "2000", Kind: ast.IntValue}},
		{Name: "offset", Description: base.DescOffset, Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "0", Kind: ast.IntValue}},
		{Name: "distinct_on", Description: base.DescDistinctOn, Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Description: base.DescInnerJoin, Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Description: base.DescNestedOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Description: base.DescNestedLimit, Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Description: base.DescNestedOffset, Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// --- Aggregation type name helpers ---

// maxAggDepth limits sub-aggregation recursion (matches old compiler's maxAggLevel=2).
const maxAggDepth = 2

// aggTypeNameAtDepth returns the aggregation type name at a given depth.
// depth 0: _Type_aggregation
// depth 1: _Type_aggregation_sub_aggregation
// depth 2: _Type_aggregation_sub_aggregation_sub_aggregation
func AggTypeNameAtDepth(objectName string, depth int) string {
	name := "_" + objectName + "_aggregation"
	for i := 0; i < depth; i++ {
		name += "_sub_aggregation"
	}
	return name
}

// scalarSubAggTypeName maps a scalar aggregation type to its SubAggregation variant.
// Returns "" if the type is not a known scalar aggregation type.
func scalarSubAggTypeName(aggTypeName string) string {
	return types.SubAggregationTypeName(aggTypeName)
}

// isStructuralAggFieldByConvention returns true if the field on an aggregation type
// represents a structural Object aggregation (e.g., specs: _Specs_aggregation).
// Uses convention-based detection: non-scalar, no-arguments fields are structural.
// Reference fields always have arguments (inner, filter, order_by, etc.).
// Extra fields always have arguments and scalar agg types.
func isStructuralAggFieldByConvention(f *ast.FieldDefinition) bool {
	if f.Name == "_rows_count" {
		return false
	}
	if scalarSubAggTypeName(f.Type.Name()) != "" {
		return false
	}
	if len(f.Arguments) > 0 {
		return false
	}
	return true
}

// --- Scalar field helpers ---

// setScalarFieldArguments sets field arguments from scalar type definitions
// (e.g., bucket for Timestamp, transforms for Geometry, struct for JSON).
// Must be called before generateAggregationType so args get copied.
func setScalarFieldArguments(ctx base.CompilationContext, def *ast.Definition) {
	for _, f := range def.Fields {
		if f.Name == "_stub" || f.Type.NamedType == "" {
			continue
		}
		s := ctx.ScalarLookup(f.Type.Name())
		if s == nil {
			continue
		}
		if fap, ok := s.(types.FieldArgumentsProvider); ok {
			f.Arguments = fap.FieldArguments()
		}
	}
}
