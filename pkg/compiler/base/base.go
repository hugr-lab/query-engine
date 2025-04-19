package base

import (
	_ "embed"
	"strconv"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/validator"
)

//go:embed "base.graphql"
var baseSchemaData string

//go:embed "scalar_types.graphql"
var scalarTypesDef string

//go:embed "system_types.graphql"
var systemTypesDef string

//go:embed "query_directives.graphql"
var queryDirectivesDef string

var Schema *ast.Schema

const (
	WithDeletedDirective = "with_deleted"
	StatsDirective       = "stats"
)

const (
	FunctionTypeName         = "Function"
	FunctionMutationTypeName = "MutationFunction"

	ModuleDirectiveName = "module"
)

const (
	ViewArgsDirectiveName = "args"
)

const (
	FieldGeometryInfoDirectiveName = "geometry_info"
	FieldSqlDirectiveName          = "sql"
	FieldExtraFieldDirectiveName   = "extra_field"
)

func Init() {
	Schema = gqlparser.MustLoadSchema(Sources()...)
}

func Sources() []*ast.Source {
	return []*ast.Source{
		validator.Prelude,
		{Name: "system_types.graphql", Input: systemTypesDef},
		{Name: "scalar_types.graphql", Input: scalarTypesDef},
		{Name: "base.graphql", Input: baseSchemaData},
		{Name: "query_directives.graphql", Input: queryDirectivesDef},
	}
}

func QuerySideDirectives() []string {
	return []string{
		"include", "skip", "defer",
		CacheDirectiveName,
		NoCacheDirectiveName,
		InvalidateCacheDirectiveName,
		StatsDirective,
		WithDeletedDirective,
	}
}

func FieldGeometryInfoDirective(geomType string, srid int) *ast.Directive {
	return &ast.Directive{
		Name: FieldGeometryInfoDirectiveName,
		Arguments: []*ast.Argument{
			{
				Name: "type",
				Value: &ast.Value{
					Raw:      geomType,
					Kind:     ast.StringValue,
					Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
				},
				Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
			},
			{
				Name: "srid",
				Value: &ast.Value{
					Raw:      strconv.Itoa(srid),
					Kind:     ast.IntValue,
					Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
				},
				Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
			},
		},
		Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
	}
}

func FieldSqlDirective(sql string) *ast.Directive {
	return &ast.Directive{
		Name: FieldSqlDirectiveName,
		Arguments: []*ast.Argument{
			{
				Name: "exp",
				Value: &ast.Value{
					Raw:      sql,
					Kind:     ast.StringValue,
					Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
				},
				Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
			},
		},
		Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
	}
}

func ModuleDirective(name string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: ModuleDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: name, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}
