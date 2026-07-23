package store

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// The mappers read a definition's directives into the typed property bags
// (models.go / properties.go). SQL bodies are stored AND exposed (typed
// members) — @view/@function/@sql/@join/@function_call/@default.
//
// The gis surface (@wfs / @wfs_field / @wfs_exclude / @feature) is deliberately
// DROPPED — rudimentary, moves to a separate service (decision 2026-07-23).

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
	argFields                        = "fields"
)

// mapDataObjectProperties builds data_objects.properties.
func mapDataObjectProperties(def *ast.Definition, obj *sdl.Object) *dataObjectProperties {
	p := &dataObjectProperties{
		Name:           obj.Name,
		SQL:            base.DefinitionDirectiveArgString(def, base.ObjectViewDirectiveName, base.ArgSQL),
		SoftDelete:     obj.SoftDelete,
		SoftDeleteCond: base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgSoftDeleteCond),
		SoftDeleteSet:  base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgSoftDeleteSet),
		IsCube:         obj.IsCube,
		IsM2M:          obj.IsM2M,
		IsHypertable:   obj.IsHypertable,
		ArgsTypeName:   obj.InputArgsName,
	}
	if obj.InputArgsName != "" {
		p.RequiredArgs = obj.RequiredArgs
	}
	if d := def.Directives.ForName(base.EmbeddingsDirectiveName); d != nil {
		p.Embeddings = &embeddingsConfig{
			Model:    base.DirectiveArgString(d, base.ArgModel),
			Vector:   base.DirectiveArgString(d, base.ArgVector),
			Distance: base.DirectiveArgString(d, base.ArgDistance),
		}
	}
	for _, d := range def.Directives.ForNames(base.ObjectUniqueDirectiveName) {
		u := uniqueConstraint{
			Fields:      base.DirectiveArgStrings(d, argFields),
			QuerySuffix: base.DirectiveArgString(d, base.ArgQuerySuffix),
		}
		if skip, ok := dirArgBool(d, base.ArgSkipQuery); ok {
			u.SkipQuery = skip
		}
		p.Unique = append(p.Unique, u)
	}
	for _, d := range def.Directives.ForNames(base.DependencyDirectiveName) {
		if n := base.DirectiveArgString(d, base.ArgName); n != "" {
			p.Dependencies = append(p.Dependencies, n)
		}
	}
	p.Cache = mapCacheSettings(def.Directives.ForName(base.CacheDirectiveName))
	if d := def.Directives.ForName(base.AtDirectiveName); d != nil {
		at := &atPin{Timestamp: base.DirectiveArgString(d, argTimestamp)}
		if v, ok := dirArgValue(d, argVersion); ok {
			at.Version = v
		}
		p.At = at
	}
	return p
}

// mapFieldProperties builds fields.properties, including the declared-field bags
// for @join / @function_call / @table_function_call_join (D17 — FIELDS, not
// relations) and the typed SQL members.
func mapFieldProperties(f *ast.FieldDefinition) *fieldProperties {
	p := &fieldProperties{
		Source:         base.FieldDefDirectiveArgString(f, base.FieldSourceDirectiveName, base.ArgField),
		Computed:       f.Directives.ForName(base.FieldSqlDirectiveName) != nil,
		SQL:            base.FieldDefDirectiveArgString(f, base.FieldSqlDirectiveName, base.ArgExp),
		Measurement:    f.Directives.ForName(base.FieldMeasurementDirectiveName) != nil,
		TimescaleKey:   f.Directives.ForName(base.FieldTimescaleKeyDirectiveName) != nil,
		UniqueRule:     base.FieldDefDirectiveArgString(f, fieldUniqueRuleDirectiveName, argRule),
		FilterRequired: f.Directives.ForName(fieldFilterRequiredDirectiveName) != nil,
		ExcludeMCP:     f.Directives.ForName(base.FieldExcludeMCPDirectiveName) != nil,
	}
	if v, ok := dirArgValue(f.Directives.ForName(base.FieldDimDirectiveName), base.ArgLen); ok {
		p.Dim = v
	}
	if d := f.Directives.ForName(base.FieldDefaultDirectiveName); d != nil {
		dv := &defaultValue{
			Sequence:  base.DirectiveArgString(d, base.ArgSequence),
			InsertExp: base.DirectiveArgString(d, base.FieldDefaultDirectiveInsertExprArgName),
			UpdateExp: base.DirectiveArgString(d, base.FieldDefaultDirectiveUpdateExprArgName),
		}
		if v, ok := dirArgValue(d, base.ArgValue); ok {
			dv.Value = v
		}
		if dv.Value != nil || dv.Sequence != "" || dv.InsertExp != "" || dv.UpdateExp != "" {
			p.Default = dv
		}
	}
	if d := f.Directives.ForName(base.FieldGeometryInfoDirectiveName); d != nil {
		geom := &geometryInfo{Type: base.DirectiveArgString(d, base.ArgType)}
		if v, ok := dirArgValue(d, argSRID); ok {
			geom.SRID = v
		}
		p.Geometry = geom
	}
	if d := f.Directives.ForName(fieldExcludeFilterDirectiveName); d != nil {
		p.ExcludeFilter = base.DirectiveArgStrings(d, argOperations)
	}
	p.Cache = mapCacheSettings(f.Directives.ForName(base.CacheDirectiveName))
	if d := f.Directives.ForName(base.JoinDirectiveName); d != nil {
		p.Join = &joinBinding{
			ReferencesName:   base.DirectiveArgString(d, base.ArgReferencesName),
			SourceFields:     base.DirectiveArgStrings(d, base.ArgSourceFields),
			ReferencesFields: base.DirectiveArgStrings(d, base.ArgReferencesFields),
			SQL:              base.DirectiveArgString(d, base.ArgSQL),
		}
	}
	if d := f.Directives.ForName(base.FunctionCallDirectiveName); d != nil {
		p.FunctionCall = mapFunctionCallBinding(d, false)
	}
	if d := f.Directives.ForName(base.FunctionCallTableJoinDirectiveName); d != nil {
		p.TableFunctionCallJoin = mapFunctionCallBinding(d, true)
	}
	return p
}

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

// mapFunctionProperties builds functions.properties.
func mapFunctionProperties(f *ast.FieldDefinition) *functionProperties {
	p := &functionProperties{
		Cache: mapCacheSettings(f.Directives.ForName(base.CacheDirectiveName)),
	}
	if d := f.Directives.ForName(base.FunctionDirectiveName); d != nil {
		fn := &functionBinding{
			Name: base.DirectiveArgString(d, base.ArgName),
			SQL:  base.DirectiveArgString(d, base.ArgSQL),
		}
		if v, ok := dirArgBool(d, argSkipNullArg); ok {
			fn.SkipNullArg = v
		}
		if v, ok := dirArgBool(d, base.ArgIsTable); ok {
			fn.IsTable = v
		}
		if v, ok := dirArgBool(d, argJSONCast); ok {
			fn.JSONCast = v
		}
		p.Function = fn
	}
	return p
}

// mapFunctionArgs builds the ordered functions.args entries.
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
func deprecationReason(dd ast.DirectiveList) string {
	d := dd.ForName(base.DeprecatedDirectiveName)
	if d == nil {
		return ""
	}
	if r := base.DirectiveArgString(d, argReason); r != "" {
		return r
	}
	return "No longer supported"
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
