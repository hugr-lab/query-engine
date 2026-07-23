package store

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Shared decode helpers for the directive↔bag pair tables (pairs_object.go /
// pairs_field.go / pairs_function.go). The pair tables are the single wiring
// point per directive; everything here is a reusable primitive.

// Directive-argument names without base-package constants (repo idiom: the
// compiler rules reference these by literal too).
const (
	fieldExcludeFilterDirectiveName  = "exclude_filter"
	fieldFilterRequiredDirectiveName = "filter_required"
	fieldUniqueRuleDirectiveName     = "unique_rule"
	argOperations                    = "operations"
	argRule                          = "rule"
	argReason                        = "reason"
	argTTL                           = "ttl"
	argKey                           = "key"
	argTags                          = "tags"
	argVersion                       = "version"
	argTimestamp                     = "timestamp"
	argSkipNullArg                   = "skip_null_arg"
	argJSONCast                      = "json_cast"
	argSRID                          = "srid"
	argEngine                        = "engine"
	argFields                        = "fields"
)

// mapFunctionCallBinding normalizes @function_call / @table_function_call_join.
func mapFunctionCallBinding(d *ast.Directive, tableJoin bool) *functionCallBinding {
	bag := &functionCallBinding{
		Function: functionRef{
			Module: base.DirectiveArgString(d, base.ArgModule),
			Name:   base.DirectiveArgString(d, base.ArgReferencesName),
		},
		SQL: base.DirectiveArgString(d, base.ArgSQL),
	}
	if v, ok := dirArgValue(d, base.ArgArgs); ok {
		bag.Args = v
	}
	if tableJoin {
		bag.SourceFields = base.DirectiveArgStrings(d, base.ArgSourceFields)
		bag.ReferencesFields = base.DirectiveArgStrings(d, base.ArgReferencesFields)
	}
	return bag
}

// mapFunctionArgs builds the ordered functions.args entries (structural members
// + the argument-level directives @arg_default / @deprecated).
func mapFunctionArgs(f *ast.FieldDefinition) []functionArgument {
	if len(f.Arguments) == 0 {
		return nil
	}
	args := make([]functionArgument, 0, len(f.Arguments))
	for _, a := range f.Arguments {
		arg := functionArgument{
			Name:        a.Name,
			Type:        a.Type.String(),
			Description: a.Description,
		}
		if a.DefaultValue != nil {
			arg.Default = a.DefaultValue.String()
		}
		if ad := a.Directives.ForName(base.ArgDefaultDirectiveName); ad != nil {
			arg.ArgDefault = base.DirectiveArgString(ad, base.ArgValue)
		}
		arg.DeprecationReason = deprecationReason(a.Directives)
		args = append(args, arg)
	}
	return args
}

// deprecationReason reads @deprecated into the storage column value: "" means
// active; a bare @deprecated stores the GraphQL spec default reason so the
// column alone distinguishes deprecated-without-reason from active.
// deprecatedDefaultReason is the GraphQL spec default a bare @deprecated
// carries — stored explicitly, emitted back as the bare directive.
const deprecatedDefaultReason = "No longer supported"

func deprecationReason(dd ast.DirectiveList) string {
	d := dd.ForName(base.DeprecatedDirectiveName)
	if d == nil {
		return ""
	}
	if r := base.DirectiveArgString(d, argReason); r != "" {
		return r
	}
	return deprecatedDefaultReason
}

// deprecatedDirective restores the bare form for the spec-default reason.
func deprecatedDirective(reason string) *ast.Directive {
	if reason == deprecatedDefaultReason {
		return directive(base.DeprecatedDirectiveName)
	}
	return directive(base.DeprecatedDirectiveName, strArg(argReason, reason))
}

func mapCacheSettings(d *ast.Directive) *cacheSettings {
	if d == nil {
		return nil
	}
	return &cacheSettings{
		TTL:  base.DirectiveArgString(d, argTTL),
		Key:  base.DirectiveArgString(d, argKey),
		Tags: base.DirectiveArgStrings(d, argTags),
	}
}

// dirArgValue returns a directive argument as its Go value (bool/int/string/
// map/list — gqlparser value conversion).
func dirArgValue(d *ast.Directive, name string) (any, bool) {
	if d == nil {
		return nil, false
	}
	a := d.Arguments.ForName(name)
	if a == nil || a.Value == nil {
		return nil, false
	}
	v, err := a.Value.Value(nil)
	if err != nil {
		return a.Value.String(), true
	}
	return v, true
}

func dirArgBool(d *ast.Directive, name string) (bool, bool) {
	v, ok := dirArgValue(d, name)
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}
