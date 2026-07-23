package store

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// fieldPairs is the SINGLE place a field-level SDL directive is wired into the
// store: the collect half reads the declared field into the row (bag member or
// column), the emit half rebuilds the directive on read. The two halves sit
// side by side so they cannot drift — a new field directive is ONE entry here
// (plus, when it needs a new typed member, the bag/DDL column it fills).
//
// The gis surface (@wfs_field / @wfs_exclude) is deliberately absent — dropped,
// moves to a separate service (decision 2026-07-23). @field_references is not a
// bag member either: it becomes a relations row (collectFieldReferences).
type fieldPair struct {
	directive string
	collect   func(f *ast.FieldDefinition, row *field)
	emit      func(row *field) []*ast.Directive
}

var fieldPairs = []fieldPair{
	{
		directive: base.FieldPrimaryKeyDirectiveName, // column is_pk
		collect: func(f *ast.FieldDefinition, row *field) {
			row.IsPK = f.Directives.ForName(base.FieldPrimaryKeyDirectiveName) != nil
		},
		emit: func(row *field) []*ast.Directive {
			if !row.IsPK {
				return nil
			}
			return []*ast.Directive{directive(base.FieldPrimaryKeyDirectiveName)}
		},
	},
	{
		directive: base.FieldSourceDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.Source = base.FieldDefDirectiveArgString(f, base.FieldSourceDirectiveName, base.ArgField)
		},
		emit: func(row *field) []*ast.Directive {
			if row.Properties.Source == "" {
				return nil
			}
			return []*ast.Directive{directive(base.FieldSourceDirectiveName, strArg(base.ArgField, row.Properties.Source))}
		},
	},
	{
		directive: base.FieldSqlDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.Computed = f.Directives.ForName(base.FieldSqlDirectiveName) != nil
			row.Properties.SQL = base.FieldDefDirectiveArgString(f, base.FieldSqlDirectiveName, base.ArgExp)
		},
		emit: func(row *field) []*ast.Directive {
			if !row.Properties.Computed {
				return nil
			}
			return []*ast.Directive{directive(base.FieldSqlDirectiveName, strArg(base.ArgExp, row.Properties.SQL))}
		},
	},
	{
		directive: base.FieldDefaultDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			d := f.Directives.ForName(base.FieldDefaultDirectiveName)
			if d == nil {
				return
			}
			dv := &defaultValue{
				Sequence:  base.DirectiveArgString(d, base.ArgSequence),
				InsertExp: base.DirectiveArgString(d, base.FieldDefaultDirectiveInsertExprArgName),
				UpdateExp: base.DirectiveArgString(d, base.FieldDefaultDirectiveUpdateExprArgName),
			}
			if v, ok := dirArgValue(d, base.ArgValue); ok {
				dv.Value = v
			}
			if dv.Value != nil || dv.Sequence != "" || dv.InsertExp != "" || dv.UpdateExp != "" {
				row.Properties.Default = dv
			}
		},
		emit: func(row *field) []*ast.Directive {
			dv := row.Properties.Default
			if dv == nil {
				return nil
			}
			var args []*ast.Argument
			if dv.Value != nil {
				args = append(args, jsonArg(base.ArgValue, dv.Value))
			}
			if dv.Sequence != "" {
				args = append(args, strArg(base.ArgSequence, dv.Sequence))
			}
			if dv.InsertExp != "" {
				args = append(args, strArg(base.FieldDefaultDirectiveInsertExprArgName, dv.InsertExp))
			}
			if dv.UpdateExp != "" {
				args = append(args, strArg(base.FieldDefaultDirectiveUpdateExprArgName, dv.UpdateExp))
			}
			return []*ast.Directive{directive(base.FieldDefaultDirectiveName, args...)}
		},
	},
	{
		directive: base.FieldGeometryInfoDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			d := f.Directives.ForName(base.FieldGeometryInfoDirectiveName)
			if d == nil {
				return
			}
			geom := &geometryInfo{Type: base.DirectiveArgString(d, base.ArgType)}
			if v, ok := dirArgValue(d, argSRID); ok {
				geom.SRID = v
			}
			row.Properties.Geometry = geom
		},
		emit: func(row *field) []*ast.Directive {
			geom := row.Properties.Geometry
			if geom == nil {
				return nil
			}
			var args []*ast.Argument
			if geom.Type != "" {
				args = append(args, enumArg(base.ArgType, geom.Type))
			}
			if geom.SRID != nil {
				args = append(args, numArg(argSRID, geom.SRID))
			}
			return []*ast.Directive{directive(base.FieldGeometryInfoDirectiveName, args...)}
		},
	},
	{
		directive: base.FieldDimDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			if v, ok := dirArgValue(f.Directives.ForName(base.FieldDimDirectiveName), base.ArgLen); ok {
				row.Properties.Dim = v
			}
		},
		emit: func(row *field) []*ast.Directive {
			if row.Properties.Dim == nil {
				return nil
			}
			return []*ast.Directive{directive(base.FieldDimDirectiveName, numArg(base.ArgLen, row.Properties.Dim))}
		},
	},
	{
		directive: base.FieldMeasurementDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.Measurement = f.Directives.ForName(base.FieldMeasurementDirectiveName) != nil
		},
		emit: func(row *field) []*ast.Directive {
			if !row.Properties.Measurement {
				return nil
			}
			return []*ast.Directive{directive(base.FieldMeasurementDirectiveName)}
		},
	},
	{
		directive: base.FieldTimescaleKeyDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.TimescaleKey = f.Directives.ForName(base.FieldTimescaleKeyDirectiveName) != nil
		},
		emit: func(row *field) []*ast.Directive {
			if !row.Properties.TimescaleKey {
				return nil
			}
			return []*ast.Directive{directive(base.FieldTimescaleKeyDirectiveName)}
		},
	},
	{
		directive: fieldUniqueRuleDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.UniqueRule = base.FieldDefDirectiveArgString(f, fieldUniqueRuleDirectiveName, argRule)
		},
		emit: func(row *field) []*ast.Directive {
			if row.Properties.UniqueRule == "" {
				return nil
			}
			return []*ast.Directive{directive(fieldUniqueRuleDirectiveName, enumArg(argRule, row.Properties.UniqueRule))}
		},
	},
	{
		directive: fieldExcludeFilterDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			if d := f.Directives.ForName(fieldExcludeFilterDirectiveName); d != nil {
				row.Properties.ExcludeFilter = base.DirectiveArgStrings(d, argOperations)
			}
		},
		emit: func(row *field) []*ast.Directive {
			if len(row.Properties.ExcludeFilter) == 0 {
				return nil
			}
			return []*ast.Directive{directive(fieldExcludeFilterDirectiveName, enumListArg(argOperations, row.Properties.ExcludeFilter))}
		},
	},
	{
		directive: fieldFilterRequiredDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.FilterRequired = f.Directives.ForName(fieldFilterRequiredDirectiveName) != nil
		},
		emit: func(row *field) []*ast.Directive {
			if !row.Properties.FilterRequired {
				return nil
			}
			return []*ast.Directive{directive(fieldFilterRequiredDirectiveName)}
		},
	},
	{
		directive: base.FieldExcludeMCPDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.ExcludeMCP = f.Directives.ForName(base.FieldExcludeMCPDirectiveName) != nil
		},
		emit: func(row *field) []*ast.Directive {
			if !row.Properties.ExcludeMCP {
				return nil
			}
			return []*ast.Directive{directive(base.FieldExcludeMCPDirectiveName)}
		},
	},
	{
		directive: base.CacheDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			row.Properties.Cache = mapCacheSettings(f.Directives.ForName(base.CacheDirectiveName))
		},
		emit: func(row *field) []*ast.Directive {
			return emitCacheDirective(row.Properties.Cache)
		},
	},
	{
		directive: base.JoinDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			d := f.Directives.ForName(base.JoinDirectiveName)
			if d == nil {
				return
			}
			row.Properties.Join = &joinBinding{
				ReferencesName:   base.DirectiveArgString(d, base.ArgReferencesName),
				SourceFields:     base.DirectiveArgStrings(d, base.ArgSourceFields),
				ReferencesFields: base.DirectiveArgStrings(d, base.ArgReferencesFields),
				SQL:              base.DirectiveArgString(d, base.ArgSQL),
			}
		},
		emit: func(row *field) []*ast.Directive {
			j := row.Properties.Join
			if j == nil {
				return nil
			}
			args := []*ast.Argument{strArg(base.ArgReferencesName, j.ReferencesName)}
			if len(j.SourceFields) > 0 {
				args = append(args, strListArg(base.ArgSourceFields, j.SourceFields))
			}
			if len(j.ReferencesFields) > 0 {
				args = append(args, strListArg(base.ArgReferencesFields, j.ReferencesFields))
			}
			if j.SQL != "" {
				args = append(args, strArg(base.ArgSQL, j.SQL))
			}
			return []*ast.Directive{directive(base.JoinDirectiveName, args...)}
		},
	},
	{
		directive: base.FunctionCallDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			if d := f.Directives.ForName(base.FunctionCallDirectiveName); d != nil {
				row.Properties.FunctionCall = mapFunctionCallBinding(d, false)
			}
		},
		emit: func(row *field) []*ast.Directive {
			return emitFunctionCallDirective(base.FunctionCallDirectiveName, row.Properties.FunctionCall, false)
		},
	},
	{
		directive: base.FunctionCallTableJoinDirectiveName,
		collect: func(f *ast.FieldDefinition, row *field) {
			if d := f.Directives.ForName(base.FunctionCallTableJoinDirectiveName); d != nil {
				row.Properties.TableFunctionCallJoin = mapFunctionCallBinding(d, true)
			}
		},
		emit: func(row *field) []*ast.Directive {
			return emitFunctionCallDirective(base.FunctionCallTableJoinDirectiveName, row.Properties.TableFunctionCallJoin, true)
		},
	},
	{
		directive: base.DeprecatedDirectiveName, // column deprecation_reason
		collect: func(f *ast.FieldDefinition, row *field) {
			row.DeprecationReason = deprecationReason(f.Directives)
		},
		emit: func(row *field) []*ast.Directive {
			if row.DeprecationReason == "" {
				return nil
			}
			return []*ast.Directive{deprecatedDirective(row.DeprecationReason)}
		},
	},
}

// collectFieldDirectives fills a field row (fresh bag + columns) from the
// declared field via the pair table.
func collectFieldDirectives(f *ast.FieldDefinition, row *field) {
	if row.Properties == nil {
		row.Properties = &fieldProperties{}
	}
	for _, p := range fieldPairs {
		p.collect(f, row)
	}
}

// emitFieldDirectives rebuilds a stored field's directives (table order).
func emitFieldDirectives(row *field) ast.DirectiveList {
	if row.Properties == nil {
		row.Properties = &fieldProperties{}
	}
	var out ast.DirectiveList
	for _, p := range fieldPairs {
		out = append(out, p.emit(row)...)
	}
	return out
}

// emitCacheDirective is shared by the field / object / function tables.
func emitCacheDirective(c *cacheSettings) []*ast.Directive {
	if c == nil {
		return nil
	}
	var args []*ast.Argument
	if c.TTL != "" {
		args = append(args, strArg(argTTL, c.TTL))
	}
	if c.Key != "" {
		args = append(args, strArg(argKey, c.Key))
	}
	if len(c.Tags) > 0 {
		args = append(args, strListArg(argTags, c.Tags))
	}
	return []*ast.Directive{directive(base.CacheDirectiveName, args...)}
}

// emitFunctionCallDirective rebuilds @function_call / @table_function_call_join.
func emitFunctionCallDirective(name string, b *functionCallBinding, tableJoin bool) []*ast.Directive {
	if b == nil {
		return nil
	}
	args := []*ast.Argument{strArg(base.ArgReferencesName, b.Function.Name)}
	if b.Function.Module != "" {
		args = append(args, strArg(base.ArgModule, b.Function.Module))
	}
	if b.Args != nil {
		args = append(args, jsonArg(base.ArgArgs, b.Args))
	}
	if tableJoin {
		if len(b.SourceFields) > 0 {
			args = append(args, strListArg(base.ArgSourceFields, b.SourceFields))
		}
		if len(b.ReferencesFields) > 0 {
			args = append(args, strListArg(base.ArgReferencesFields, b.ReferencesFields))
		}
	}
	if b.SQL != "" {
		args = append(args, strArg(base.ArgSQL, b.SQL))
	}
	return []*ast.Directive{directive(name, args...)}
}
