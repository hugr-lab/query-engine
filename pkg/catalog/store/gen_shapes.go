package store

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// The queryShapes registry entries (Ш4.4): one per query/mutation kind.
// Today they emit the def-level @query/@mutation markers the planner reads
// off the data-object definition; Ш4.7 extends them with the root-field
// builders that instantiate the same shapes on module roots.

// objectTraits bundles what shape/field rules branch on for ONE object.
type objectTraits struct {
	row    *dataObject
	fields []*field
	src    activeSource
	srcs   map[string]activeSource
}

func (g *genContext) objectTraits(ctx context.Context, row *dataObject) *objectTraits {
	srcs := g.s.activeSources(ctx)
	return &objectTraits{
		row:    row,
		fields: g.s.readFields(ctx, row.Name),
		src:    srcs[row.DataSource],
		srcs:   srcs,
	}
}

// queryName is the marker/root base name: the ORIGINAL (unprefixed) name for
// AsModule sources, the compiled name otherwise.
func (t *objectTraits) queryName() string {
	if t.src.AsModule && t.row.OriginalName != "" {
		return t.row.OriginalName
	}
	return t.row.Name
}

// objInfo bridges the traits into the compiler's arg-profile input.
func (t *objectTraits) objInfo() *base.ObjectInfo {
	info := &base.ObjectInfo{Name: t.row.Name, OriginalName: t.row.OriginalName}
	if t.row.Properties != nil {
		info.InputArgsName = t.row.Properties.ArgsTypeName
		info.RequiredArgs = t.row.Properties.RequiredArgs
	}
	return info
}

func (t *objectTraits) isM2M() bool {
	return t.row.Properties != nil && t.row.Properties.IsM2M
}

func (t *objectTraits) hasPK() bool {
	for _, f := range t.fields {
		if f.IsPK {
			return true
		}
	}
	return false
}

// writable: mutation shapes exist for tables of non-read-only sources.
func (t *objectTraits) writable() bool {
	return t.row.Kind == "table" && !t.src.ReadOnly
}

func queryMarker(name, queryType string) []*ast.Directive {
	return []*ast.Directive{directive("query",
		strArg(base.ArgName, name),
		enumArg("type", queryType),
	)}
}

func mutationMarker(name, mutationType, dataInput string) []*ast.Directive {
	return []*ast.Directive{directive("mutation",
		strArg("data_input", dataInput),
		strArg(base.ArgName, name),
		enumArg("type", mutationType),
	)}
}

// Root-field marker builders. Note the compiler's asymmetry: @query markers
// on root fields carry the COMPILED object name, @aggregation_query markers
// carry the AsModule-aware query name, @mutation markers the compiled name.
func rootQueryMarker(t *objectTraits, queryType string) *ast.Directive {
	return directive("query", strArg(base.ArgName, t.row.Name), enumArg("type", queryType))
}

func rootAggMarker(qn string, isBucket bool) *ast.Directive {
	return directive("aggregation_query",
		strArg(base.ArgName, qn), boolArg(base.ArgIsBucket, isBucket))
}

func rootMutationMarker(t *objectTraits, mutationType, dataInput string) *ast.Directive {
	return directive("mutation",
		strArg(base.ArgName, t.row.Name),
		enumArg("type", mutationType),
		strArg("data_input", dataInput),
	)
}

func (t *objectTraits) rootCatalog() *ast.Directive {
	return catalogDirective(t.row.DataSource, t.src.Engine)
}

var selectShape = queryShape{
	kind:     "select",
	rootKind: sdl.ModuleQuery,
	matches:  func(*objectTraits) bool { return true },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn, "SELECT")
	},
	root: func(t *objectTraits) *ast.FieldDefinition {
		return &ast.FieldDefinition{
			Name:        t.queryName(),
			Description: t.row.Description,
			Type:        ast.ListType(ast.NamedType(t.row.Name, reconPos), reconPos),
			Arguments:   rules.QueryArgsWithViewArgs(t.objInfo(), t.row.Name+filterSuffix, reconPos),
			Directives:  ast.DirectiveList{rootQueryMarker(t, "SELECT"), t.rootCatalog()},
			Position:    reconPos,
		}
	},
}

var selectOnePKShape = queryShape{
	kind:     "select_one_pk",
	rootKind: sdl.ModuleQuery,
	matches:  func(t *objectTraits) bool { return t.hasPK() && !t.isM2M() },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn+"_by_pk", "SELECT_ONE")
	},
	root: func(t *objectTraits) *ast.FieldDefinition {
		fd := &ast.FieldDefinition{
			Name:        t.queryName() + "_by_pk",
			Description: t.row.Description,
			Type:        ast.NamedType(t.row.Name, reconPos),
			Directives:  ast.DirectiveList{rootQueryMarker(t, "SELECT_ONE"), t.rootCatalog()},
			Position:    reconPos,
		}
		if info := t.objInfo(); info.InputArgsName != "" {
			argType := ast.NamedType(info.InputArgsName, reconPos)
			if info.RequiredArgs {
				argType = ast.NonNullNamedType(info.InputArgsName, reconPos)
			}
			fd.Arguments = append(fd.Arguments, &ast.ArgumentDefinition{
				Name: "args", Description: base.DescArgs, Type: argType, Position: reconPos,
			})
		}
		for _, f := range t.fields {
			if !f.IsPK {
				continue
			}
			fd.Arguments = append(fd.Arguments, &ast.ArgumentDefinition{
				Name:     f.Name,
				Type:     ast.NonNullNamedType(parseFieldType(f.FieldType).Name(), reconPos),
				Position: reconPos,
			})
		}
		return fd
	},
}

var aggregateShape = queryShape{
	kind:     "aggregate",
	rootKind: sdl.ModuleQuery,
	matches:  func(*objectTraits) bool { return true },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn+"_aggregation", "AGGREGATE")
	},
	root: func(t *objectTraits) *ast.FieldDefinition {
		qn := t.queryName()
		return &ast.FieldDefinition{
			Name:        qn + "_aggregation",
			Description: "The aggregation for " + qn,
			Type:        ast.NamedType("_"+t.row.Name+"_aggregation", reconPos),
			Arguments:   rules.QueryArgsWithViewArgs(t.objInfo(), t.row.Name+filterSuffix, reconPos),
			Directives:  ast.DirectiveList{rootAggMarker(qn, false), t.rootCatalog()},
			Position:    reconPos,
		}
	},
}

var bucketAggShape = queryShape{
	kind:     "bucket_agg",
	rootKind: sdl.ModuleQuery,
	matches:  func(*objectTraits) bool { return true },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn+"_bucket_aggregation", "AGGREGATE_BUCKET")
	},
	root: func(t *objectTraits) *ast.FieldDefinition {
		qn := t.queryName()
		return &ast.FieldDefinition{
			Name:        qn + "_bucket_aggregation",
			Description: "The aggregation for " + qn,
			Type:        ast.ListType(ast.NamedType("_"+t.row.Name+"_aggregation_bucket", reconPos), reconPos),
			Arguments:   rules.QueryArgsWithViewArgs(t.objInfo(), t.row.Name+filterSuffix, reconPos),
			Directives:  ast.DirectiveList{rootAggMarker(qn, true), t.rootCatalog()},
			Position:    reconPos,
		}
	},
}

var insertShape = queryShape{
	kind:     "insert",
	rootKind: sdl.ModuleMutation,
	matches:  (*objectTraits).writable,
	markers: func(t *objectTraits, qn string) []*ast.Directive {
		return mutationMarker("insert_"+qn, "INSERT", t.row.Name+mutInputDataSuffix)
	},
	root: func(t *objectTraits) *ast.FieldDefinition {
		inputName := t.row.Name + mutInputDataSuffix
		outType := ast.NamedType(t.row.Name, reconPos)
		if !t.hasPK() || t.isM2M() {
			outType = ast.NamedType("OperationResult", reconPos)
		}
		return &ast.FieldDefinition{
			Name:        "insert_" + t.queryName(),
			Description: t.row.Description,
			Type:        outType,
			Arguments: ast.ArgumentDefinitionList{
				{Name: "data", Type: ast.NonNullNamedType(inputName, reconPos), Position: reconPos},
			},
			Directives: ast.DirectiveList{rootMutationMarker(t, "INSERT", inputName), t.rootCatalog()},
			Position:   reconPos,
		}
	},
}

var updateShape = queryShape{
	kind:     "update",
	rootKind: sdl.ModuleMutation,
	matches:  (*objectTraits).writable,
	markers: func(t *objectTraits, qn string) []*ast.Directive {
		return mutationMarker("update_"+qn, "UPDATE", t.row.Name+mutDataSuffix)
	},
	root: func(t *objectTraits) *ast.FieldDefinition {
		inputName := t.row.Name + mutDataSuffix
		return &ast.FieldDefinition{
			Name:        "update_" + t.queryName(),
			Description: t.row.Description,
			Type:        ast.NamedType("OperationResult", reconPos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "filter", Type: ast.NamedType(t.row.Name+filterSuffix, reconPos), Position: reconPos},
				{Name: "data", Type: ast.NonNullNamedType(inputName, reconPos), Position: reconPos},
			},
			Directives: ast.DirectiveList{rootMutationMarker(t, "UPDATE", inputName), t.rootCatalog()},
			Position:   reconPos,
		}
	},
}

var deleteShape = queryShape{
	kind:     "delete",
	rootKind: sdl.ModuleMutation,
	matches:  (*objectTraits).writable,
	markers: func(t *objectTraits, qn string) []*ast.Directive {
		return mutationMarker("delete_"+qn, "DELETE", "")
	},
	root: func(t *objectTraits) *ast.FieldDefinition {
		return &ast.FieldDefinition{
			Name:        "delete_" + t.queryName(),
			Description: t.row.Description,
			Type:        ast.NamedType("OperationResult", reconPos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "filter", Type: ast.NamedType(t.row.Name+filterSuffix, reconPos), Position: reconPos},
			},
			Directives: ast.DirectiveList{rootMutationMarker(t, "DELETE", ""), t.rootCatalog()},
			Position:   reconPos,
		}
	},
}

// shapeMarkers emits every matching shape's def-level markers for the object.
func shapeMarkers(t *objectTraits) ast.DirectiveList {
	qn := t.queryName()
	var out ast.DirectiveList
	for i := range queryShapes {
		if !queryShapes[i].matches(t) {
			continue
		}
		out = append(out, queryShapes[i].markers(t, qn)...)
	}
	return out
}
