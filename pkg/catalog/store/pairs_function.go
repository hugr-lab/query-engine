package store

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// functionPairs is the SINGLE place a function-level SDL directive is wired
// into the store (see fieldPairs for the contract). Rows are catalog.functions:
// module / deprecation_reason are columns, bindings live in the typed bag,
// arguments in the ordered args list (argument directives are handled by
// mapFunctionArgs / emitFunctionArgs below — @arg_default and @deprecated).
//
// NOT here: @catalog (provenance column; the generation layer re-attaches it
// WITH the engine from data_source_meta).
type functionPair struct {
	directive string
	collect   func(f *ast.FieldDefinition, row *function)
	emit      func(row *function) []*ast.Directive
}

var functionPairs = []functionPair{
	{
		directive: base.ModuleDirectiveName, // column module (also the row key)
		collect: func(f *ast.FieldDefinition, row *function) {
			// Module is resolved by the caller before the row exists (it is part
			// of the primary key) — nothing to collect here.
		},
		emit: func(row *function) []*ast.Directive {
			if row.Module == "" {
				return nil
			}
			return []*ast.Directive{directive(base.ModuleDirectiveName, strArg(base.ArgName, row.Module))}
		},
	},
	{
		directive: base.FunctionDirectiveName,
		collect: func(f *ast.FieldDefinition, row *function) {
			d := f.Directives.ForName(base.FunctionDirectiveName)
			if d == nil {
				return
			}
			fn := &functionBinding{
				Name: base.DirectiveArgString(d, base.ArgName),
				SQL:  base.DirectiveArgString(d, base.ArgSQL),
			}
			fn.SkipNullArg, _ = dirArgBool(d, argSkipNullArg)
			fn.IsTable, _ = dirArgBool(d, base.ArgIsTable)
			fn.JSONCast, _ = dirArgBool(d, argJSONCast)
			row.Properties.Function = fn
		},
		emit: func(row *function) []*ast.Directive {
			fn := row.Properties.Function
			if fn == nil {
				return nil
			}
			args := []*ast.Argument{strArg(base.ArgName, fn.Name)}
			if fn.SQL != "" {
				args = append(args, strArg(base.ArgSQL, fn.SQL))
			}
			if fn.SkipNullArg {
				args = append(args, boolArg(argSkipNullArg, true))
			}
			if fn.IsTable {
				args = append(args, boolArg(base.ArgIsTable, true))
			}
			if fn.JSONCast {
				args = append(args, boolArg(argJSONCast, true))
			}
			return []*ast.Directive{directive(base.FunctionDirectiveName, args...)}
		},
	},
	{
		directive: base.CacheDirectiveName,
		collect: func(f *ast.FieldDefinition, row *function) {
			row.Properties.Cache = mapCacheSettings(f.Directives.ForName(base.CacheDirectiveName))
		},
		emit: func(row *function) []*ast.Directive {
			return emitCacheDirective(row.Properties.Cache)
		},
	},
	{
		directive: base.DeprecatedDirectiveName, // column deprecation_reason
		collect: func(f *ast.FieldDefinition, row *function) {
			row.DeprecationReason = deprecationReason(f.Directives)
		},
		emit: func(row *function) []*ast.Directive {
			if row.DeprecationReason == "" {
				return nil
			}
			return []*ast.Directive{deprecatedDirective(row.DeprecationReason)}
		},
	},
}

// collectFunctionDirectives fills a function row (fresh bag + columns) from the
// declared root field via the pair table; arguments via mapFunctionArgs.
func collectFunctionDirectives(f *ast.FieldDefinition, row *function) {
	if row.Properties == nil {
		row.Properties = &functionProperties{}
	}
	for _, p := range functionPairs {
		p.collect(f, row)
	}
	row.Args = mapFunctionArgs(f)
}

// emitFunctionDirectives rebuilds a stored function's directives (table order).
func emitFunctionDirectives(row *function) ast.DirectiveList {
	if row.Properties == nil {
		row.Properties = &functionProperties{}
	}
	var out ast.DirectiveList
	for _, p := range functionPairs {
		out = append(out, p.emit(row)...)
	}
	return out
}

// emitFunctionArgs rebuilds the ordered argument definitions: structural
// members plus the argument-level directive pairs (@arg_default, @deprecated).
func emitFunctionArgs(row *function) ast.ArgumentDefinitionList {
	return emitArgumentDefs(row.Args)
}

// emitArgumentDefs is the shared argument re-emission (functions and declared
// field arguments store the same shape).
func emitArgumentDefs(args []functionArgument) ast.ArgumentDefinitionList {
	var out ast.ArgumentDefinitionList
	for _, a := range args {
		def := &ast.ArgumentDefinition{
			Name:        a.Name,
			Type:        parseFieldType(a.Type),
			Description: a.Description,
			Position:    reconPos,
		}
		if a.Default != "" {
			def.DefaultValue = parseValueRaw(a.Default)
		}
		if a.ArgDefault != "" {
			def.Directives = append(def.Directives,
				directive(base.ArgDefaultDirectiveName, strArg(base.ArgValue, a.ArgDefault)))
		}
		if a.DeprecationReason != "" {
			def.Directives = append(def.Directives, deprecatedDirective(a.DeprecationReason))
		}
		out = append(out, def)
	}
	return out
}
