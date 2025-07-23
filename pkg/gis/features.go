package gis

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/vektah/gqlparser/v2/ast"
)

type featureDefinition struct {
	Name           string
	Description    string
	GeometryField  string
	IdField        string
	PropertiesJQ   string
	GeometryType   string
	GeometrySRID   int
	Definition     *openapi3.Schema
	Variables      *openapi3.Schema
	BBoxFilterPath string
	IsPaginated    bool // if true, the feature is paginated
	// meta information for WFS
	Summary    string
	ExtentPath string
	CountPath  string

	WriteBBox   bool // if true, write bbox to feature
	transformer *jq.Transformer
}

func newFeatureDefinition(d *ast.Directive, vars map[string]any) (featureDefinition, error) {
	name := compiler.DirectiveArgValue(d, "name", vars)
	if name == "" {
		return featureDefinition{}, errors.New("missing feature name")
	}
	v := compiler.DirectiveArgValue(d, "geometry_srid", vars)
	srid, _ := strconv.Atoi(v)
	if srid == 0 {
		srid = 4326 // default SRID
	}
	def, err := encodeOpenApiDefinition(d.Arguments.ForName("definition"), vars)
	if err != nil {
		return featureDefinition{}, err
	}
	vv, err := encodeOpenApiDefinition(d.Arguments.ForName("variables"), vars)
	if err != nil {
		return featureDefinition{}, err
	}

	return featureDefinition{
		Name:          name,
		Description:   compiler.DirectiveArgValue(d, "description", vars),
		GeometryField: compiler.DirectiveArgValue(d, "geometry", vars),
		IdField:       compiler.DirectiveArgValue(d, "id", vars),
		PropertiesJQ:  compiler.DirectiveArgValue(d, "properties", vars),
		GeometryType:  compiler.DirectiveArgValue(d, "geometry_type", vars),
		GeometrySRID:  srid,
		Definition:    def,
		Variables:     vv,
		Summary:       compiler.DirectiveArgValue(d, "summary", vars),
		ExtentPath:    compiler.DirectiveArgValue(d, "extent_path", vars),
		CountPath:     compiler.DirectiveArgValue(d, "count_path", vars),
		WriteBBox:     compiler.DirectiveArgValue(d, "write_bbox", vars) == "true",
		IsPaginated:   compiler.DirectiveArgValue(d, "is_paginated", vars) == "true",
	}, nil
}

func encodeOpenApiDefinition(arg *ast.Argument, vars map[string]any) (*openapi3.Schema, error) {
	if arg == nil || arg.Value == nil {
		return nil, nil
	}
	v, err := arg.Value.Value(vars)
	if err != nil {
		return nil, err
	}
	var def openapi3.Schema
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &def); err != nil {
		return nil, err
	}
	return &def, nil
}
