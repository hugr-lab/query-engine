package base

import (
	_ "embed"

	"github.com/vektah/gqlparser/v2/ast"
)

const (
	GisFeatureDirectiveName    = "feature"
	GisWFSDirectiveName        = "wfs"
	GisWFSFieldDirectiveName   = "wfs_field"
	GisWFSExcludeDirectiveName = "wfs_exclude"

	GisWFSTypeName = "_wfs_features"
)

//go:embed gis.graphql
var gisDirectives string

func GisWFSTypeTemplate() *ast.Definition {
	return &ast.Definition{
		Name:        GisWFSTypeName,
		Kind:        ast.Object,
		Description: `A type that represents a supported collection of WFS features.`,
		Position:    CompiledPos("gis.graphql"),
	}
}
