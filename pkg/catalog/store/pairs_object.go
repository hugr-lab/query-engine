package store

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// objectPairs is the SINGLE place an object-level SDL directive is wired into
// the store (see fieldPairs for the contract). Rows are catalog.data_objects:
// kind / original_name / module are columns, the rest lives in the typed bag.
//
// NOT here: @references (a relations row, collectObjectReferences ↔
// referencesDirective in reconstruct_object.go), @catalog (provenance column;
// re-attached WITH the engine from data_source_meta by reconstructDataObject)
// and the dropped gis surface (@wfs / @feature).
type objectPair struct {
	directive string
	collect   func(def *ast.Definition, row *dataObject)
	emit      func(row *dataObject) []*ast.Directive
}

var objectPairs = []objectPair{
	{
		directive: base.OriginalNameDirectiveName, // column original_name
		collect: func(def *ast.Definition, row *dataObject) {
			row.OriginalName = originalName(def)
		},
		emit: func(row *dataObject) []*ast.Directive {
			// The compiler carries @original_name only when prefixing renamed
			// the object — an unprefixed compiled def has none.
			if row.OriginalName == "" || row.OriginalName == row.Name {
				return nil
			}
			return []*ast.Directive{directive(base.OriginalNameDirectiveName, strArg(base.ArgName, row.OriginalName))}
		},
	},
	{
		directive: base.ModuleDirectiveName, // column module
		collect: func(def *ast.Definition, row *dataObject) {
			row.Module = sdl.ObjectModule(def)
		},
		emit: func(row *dataObject) []*ast.Directive {
			if row.Module == "" {
				return nil
			}
			return []*ast.Directive{directive(base.ModuleDirectiveName, strArg(base.ArgName, row.Module))}
		},
	},
	{
		directive: base.ObjectTableDirectiveName + " / " + base.ObjectViewDirectiveName, // column kind + bag
		collect: func(def *ast.Definition, row *dataObject) {
			if d := def.Directives.ForName(base.ObjectTableDirectiveName); d != nil {
				row.Kind = "table"
				row.Properties.Name = base.DirectiveArgString(d, base.ArgName)
				row.Properties.IsM2M, _ = dirArgBool(d, base.ArgIsM2M)
				row.Properties.SoftDelete, _ = dirArgBool(d, base.ArgSoftDelete)
				row.Properties.SoftDeleteCond = base.DirectiveArgString(d, base.ArgSoftDeleteCond)
				row.Properties.SoftDeleteSet = base.DirectiveArgString(d, base.ArgSoftDeleteSet)
				return
			}
			if d := def.Directives.ForName(base.ObjectViewDirectiveName); d != nil {
				row.Kind = "view"
				row.Properties.Name = base.DirectiveArgString(d, base.ArgName)
				row.Properties.SQL = base.DirectiveArgString(d, base.ArgSQL)
			}
		},
		emit: func(row *dataObject) []*ast.Directive {
			p := row.Properties
			var args []*ast.Argument
			if p.Name != "" {
				args = append(args, strArg(base.ArgName, p.Name))
			}
			if row.Kind == "view" {
				if p.SQL != "" {
					args = append(args, strArg(base.ArgSQL, p.SQL))
				}
				return []*ast.Directive{directive(base.ObjectViewDirectiveName, args...)}
			}
			if p.IsM2M {
				args = append(args, boolArg(base.ArgIsM2M, true))
			}
			if p.SoftDelete {
				args = append(args, boolArg(base.ArgSoftDelete, true))
			}
			if p.SoftDeleteCond != "" {
				args = append(args, strArg(base.ArgSoftDeleteCond, p.SoftDeleteCond))
			}
			if p.SoftDeleteSet != "" {
				args = append(args, strArg(base.ArgSoftDeleteSet, p.SoftDeleteSet))
			}
			return []*ast.Directive{directive(base.ObjectTableDirectiveName, args...)}
		},
	},
	{
		directive: base.ViewArgsDirectiveName,
		collect: func(def *ast.Definition, row *dataObject) {
			d := def.Directives.ForName(base.ViewArgsDirectiveName)
			if d == nil {
				return
			}
			row.Properties.ArgsTypeName = base.DirectiveArgString(d, base.ArgName)
			row.Properties.RequiredArgs, _ = dirArgBool(d, base.ArgRequired)
		},
		emit: func(row *dataObject) []*ast.Directive {
			if row.Properties.ArgsTypeName == "" {
				return nil
			}
			args := []*ast.Argument{strArg(base.ArgName, row.Properties.ArgsTypeName)}
			if row.Properties.RequiredArgs {
				args = append(args, boolArg(base.ArgRequired, true))
			}
			return []*ast.Directive{directive(base.ViewArgsDirectiveName, args...)}
		},
	},
	{
		directive: base.ObjectCubeDirectiveName,
		collect: func(def *ast.Definition, row *dataObject) {
			row.Properties.IsCube = def.Directives.ForName(base.ObjectCubeDirectiveName) != nil
		},
		emit: func(row *dataObject) []*ast.Directive {
			if !row.Properties.IsCube {
				return nil
			}
			return []*ast.Directive{directive(base.ObjectCubeDirectiveName)}
		},
	},
	{
		directive: base.ObjectHyperTableDirectiveName,
		collect: func(def *ast.Definition, row *dataObject) {
			row.Properties.IsHypertable = def.Directives.ForName(base.ObjectHyperTableDirectiveName) != nil
		},
		emit: func(row *dataObject) []*ast.Directive {
			if !row.Properties.IsHypertable {
				return nil
			}
			return []*ast.Directive{directive(base.ObjectHyperTableDirectiveName)}
		},
	},
	{
		directive: base.EmbeddingsDirectiveName,
		collect: func(def *ast.Definition, row *dataObject) {
			d := def.Directives.ForName(base.EmbeddingsDirectiveName)
			if d == nil {
				return
			}
			row.Properties.Embeddings = &embeddingsConfig{
				Model:    base.DirectiveArgString(d, base.ArgModel),
				Vector:   base.DirectiveArgString(d, base.ArgVector),
				Distance: base.DirectiveArgString(d, base.ArgDistance),
			}
		},
		emit: func(row *dataObject) []*ast.Directive {
			e := row.Properties.Embeddings
			if e == nil {
				return nil
			}
			return []*ast.Directive{directive(base.EmbeddingsDirectiveName,
				strArg(base.ArgModel, e.Model), strArg(base.ArgVector, e.Vector), enumArg(base.ArgDistance, e.Distance))}
		},
	},
	{
		directive: base.ObjectUniqueDirectiveName, // repeatable
		collect: func(def *ast.Definition, row *dataObject) {
			for _, d := range def.Directives.ForNames(base.ObjectUniqueDirectiveName) {
				u := uniqueConstraint{
					Fields:      base.DirectiveArgStrings(d, argFields),
					QuerySuffix: base.DirectiveArgString(d, base.ArgQuerySuffix),
				}
				u.SkipQuery, _ = dirArgBool(d, base.ArgSkipQuery)
				row.Properties.Unique = append(row.Properties.Unique, u)
			}
		},
		emit: func(row *dataObject) []*ast.Directive {
			var out []*ast.Directive
			for _, u := range row.Properties.Unique {
				args := []*ast.Argument{strListArg(argFields, u.Fields)}
				if u.QuerySuffix != "" {
					args = append(args, strArg(base.ArgQuerySuffix, u.QuerySuffix))
				}
				if u.SkipQuery {
					args = append(args, boolArg(base.ArgSkipQuery, true))
				}
				out = append(out, directive(base.ObjectUniqueDirectiveName, args...))
			}
			return out
		},
	},
	{
		directive: base.DependencyDirectiveName, // repeatable
		collect: func(def *ast.Definition, row *dataObject) {
			for _, d := range def.Directives.ForNames(base.DependencyDirectiveName) {
				if n := base.DirectiveArgString(d, base.ArgName); n != "" {
					row.Properties.Dependencies = append(row.Properties.Dependencies, n)
				}
			}
		},
		emit: func(row *dataObject) []*ast.Directive {
			var out []*ast.Directive
			for _, n := range row.Properties.Dependencies {
				out = append(out, directive(base.DependencyDirectiveName, strArg(base.ArgName, n)))
			}
			return out
		},
	},
	{
		directive: base.CacheDirectiveName,
		collect: func(def *ast.Definition, row *dataObject) {
			row.Properties.Cache = mapCacheSettings(def.Directives.ForName(base.CacheDirectiveName))
		},
		emit: func(row *dataObject) []*ast.Directive {
			return emitCacheDirective(row.Properties.Cache)
		},
	},
	{
		directive: base.AtDirectiveName,
		collect: func(def *ast.Definition, row *dataObject) {
			d := def.Directives.ForName(base.AtDirectiveName)
			if d == nil {
				return
			}
			at := &atPin{Timestamp: base.DirectiveArgString(d, argTimestamp)}
			if v, ok := dirArgValue(d, argVersion); ok {
				at.Version = v
			}
			row.Properties.At = at
		},
		emit: func(row *dataObject) []*ast.Directive {
			at := row.Properties.At
			if at == nil {
				return nil
			}
			var args []*ast.Argument
			if at.Version != nil {
				args = append(args, numArg(argVersion, at.Version))
			}
			if at.Timestamp != "" {
				args = append(args, strArg(argTimestamp, at.Timestamp))
			}
			return []*ast.Directive{directive(base.AtDirectiveName, args...)}
		},
	},
}

// collectObjectDirectives fills a data-object row (fresh bag + columns) from
// the compiled definition via the pair table.
func collectObjectDirectives(def *ast.Definition, row *dataObject) {
	if row.Properties == nil {
		row.Properties = &dataObjectProperties{}
	}
	for _, p := range objectPairs {
		p.collect(def, row)
	}
}

// emitObjectDirectives rebuilds a stored object's directives (table order).
func emitObjectDirectives(row *dataObject) ast.DirectiveList {
	if row.Properties == nil {
		row.Properties = &dataObjectProperties{}
	}
	var out ast.DirectiveList
	for _, p := range objectPairs {
		out = append(out, p.emit(row)...)
	}
	return out
}
