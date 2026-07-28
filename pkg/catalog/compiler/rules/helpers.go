package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// What is left here after design-036 deleted the GENERATE rules falls in two
// halves: catalogDirective, which belongs to the PREPARE rule that uses it, and
// the exported argument builders, whose only callers are now the catalog
// storage's on-read generators (store/gen_*). The split is the subject of the
// next step; the file is pruned to exactly those two.

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

// --- Argument definition builders ---

// QueryArgsWithViewArgs returns standard query arguments, optionally prepending
// an "args" parameter for parameterized views (@args directive).
func QueryArgsWithViewArgs(info *base.ObjectInfo, filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
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

// SubQueryArgsWithViewArgs is the sub-query (reference) argument set for a
// target that may be a PARAMETERIZED VIEW, and it takes the view's args exactly
// as the root query does. A nested field is the ONLY place those arguments can
// be passed — there is no root query in the path — so without this the view is
// unreachable through the reference: a back navigation field on a table that a
// parameterized view references could not be given the view's parameters at
// all.
func SubQueryArgsWithViewArgs(info *base.ObjectInfo, filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	args := QueryArgsWithViewArgs(info, filterName, pos)
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

// ViewArgsArgument returns the "args" argument for a field whose TARGET is a
// parameterized view, or nil when it is not one.
//
// Every field that reaches a parameterized view needs it — a navigation field,
// its aggregation twins, a @join, a sub-aggregation member — because the view
// cannot run without its parameters and a nested field is the only place to
// pass them. Requiredness is the same rule the root query follows: an input
// with a NonNull member makes args itself NonNull, so "you must parameterize
// this view" is stated once and enforced everywhere it is reachable.
func ViewArgsArgument(info *base.ObjectInfo, pos *ast.Position) *ast.ArgumentDefinition {
	if info == nil || info.InputArgsName == "" {
		return nil
	}
	argType := ast.NamedType(info.InputArgsName, pos)
	if info.RequiredArgs {
		argType = ast.NonNullNamedType(info.InputArgsName, pos)
	}
	return &ast.ArgumentDefinition{
		Name: "args", Description: base.DescArgs, Type: argType, Position: pos,
	}
}

// PrependViewArgs puts the target's view args in front of an argument list.
// A no-op for a target that is not a parameterized view.
func PrependViewArgs(info *base.ObjectInfo, args ast.ArgumentDefinitionList, pos *ast.Position) ast.ArgumentDefinitionList {
	arg := ViewArgsArgument(info, pos)
	if arg == nil {
		return args
	}
	return append(ast.ArgumentDefinitionList{arg}, args...)
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

// --- Shared query-time object args (_join / _spatial) ---

// JoinObjectQueryArgsWithViewArgs creates args for _join type fields, optionally
// prepending view args for parameterized views.
func JoinObjectQueryArgsWithViewArgs(info *base.ObjectInfo, filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return append(sharedViewArgs(info, pos), joinObjectQueryArgs(filterName, pos)...)
}

// JoinObjectAggArgsWithViewArgs creates args for _join_aggregation type fields,
// optionally prepending view args for parameterized views.
func JoinObjectAggArgsWithViewArgs(info *base.ObjectInfo, filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return append(sharedViewArgs(info, pos), joinObjectAggArgs(filterName, pos)...)
}

// sharedViewArgs is the leading "args" parameter of a parameterized-view target,
// as its own list so a caller can append its own set behind it.
func sharedViewArgs(info *base.ObjectInfo, pos *ast.Position) ast.ArgumentDefinitionList {
	if arg := ViewArgsArgument(info, pos); arg != nil {
		return ast.ArgumentDefinitionList{arg}
	}
	return nil
}

// joinObjectQueryArgs creates args for _join type fields (includes limit/offset).
func joinObjectQueryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", pos), pos), Position: pos},
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

// joinObjectAggArgs creates args for _join_aggregation type fields (no limit/offset).
func joinObjectAggArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", pos), pos), Position: pos},
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

// SpatialObjectQueryArgs creates args for _spatial type fields (includes limit/offset).
func SpatialObjectQueryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "field", Type: ast.NonNullNamedType("String", pos), Position: pos},
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

// SpatialObjectAggArgs creates args for _spatial_aggregation type fields (no limit/offset).
func SpatialObjectAggArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "field", Type: ast.NonNullNamedType("String", pos), Position: pos},
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

// VectorSearchArgs is the similarity/semantic argument decorator:
// hasVector only → similarity; hasEmbeddings → similarity + semantic.
func VectorSearchArgs(hasVector, hasEmbeddings bool, pos *ast.Position) ast.ArgumentDefinitionList {
	if !hasVector {
		return nil
	}
	args := ast.ArgumentDefinitionList{
		{
			Name:        "similarity",
			Description: "Search for vector similarity",
			Type:        ast.NamedType("VectorSearchInput", pos),
			Position:    pos,
		},
	}
	if hasEmbeddings {
		args = append(args, &ast.ArgumentDefinition{
			Name:        "semantic",
			Description: "Search for semantic similarity",
			Type:        ast.NamedType("SemanticSearchInput", pos),
			Position:    pos,
		})
	}
	return args
}

// --- Aggregation type name helpers ---

// AggTypeNameAtDepth returns the aggregation type name at a given depth.
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
