package gis

import (
	"github.com/getkin/kin-openapi/openapi3"
)

// Collection represents a GIS collection with its metadata and links.
type Collection struct {
	Name               string
	Description        string
	Namespace          string
	NamespaceBaseURL   string
	NamespaceSchemaDir string

	Variables  *openapi3.Schema
	Definition *openapi3.Schema

	Links []Link

	QueryType string // Type of query for the collection, data object or saved query

	// GraphQL paths for the data object
	Query                 string
	QueryFeature          string
	QueryFeatureIdVarName string
	QueryFeatureIdType    *openapi3.Schema

	IsReadOnly     bool
	MutationInsert string
	MutationUpdate string
	MutationDelete string

	// Saved query
	QueryName string // Name of the saved query
}
