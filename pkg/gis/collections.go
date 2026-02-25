package gis

import (
	"context"
	"strconv"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	dataObjectQueryType = "DataObject"
	savedQueryType      = "SavedQuery"
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
	feature   featureDefinition

	// Roles for access control
	roles []string
}

// This method should return the list of collections available in the GIS service.
// It fetches the collections from schema (_wfs_features) and saved queries (core.saved_queries tables and publications).
func (s *Service) collections(ctx context.Context) ([]*Collection, error) {

	// get wfs features from data object
	collections, err := s.wfsSchemaFeatures(ctx, "")
	if err != nil {
		return nil, err
	}

	savedQueries, err := s.wfsSavedQueries(ctx, "")
	if err != nil {
		return nil, err
	}
	collections = append(collections, savedQueries...)

	return collections, nil
}

func (s *Service) wfsSchemaFeatures(ctx context.Context, name string) (collections []*Collection, err error) {
	// This method should return the schema for WFS features
	// It fetches the schema from the _wfs_features data object
	provider := s.schema.Provider()
	var defs base.DefinitionsSource = provider

	wfsType := provider.ForName(ctx, base.GisWFSTypeName)
	if wfsType == nil {
		return nil, nil // No WFS features defined
	}

	perms := perm.PermissionsFromCtx(ctx)
	for _, field := range wfsType.Fields {
		_, ok := perms.Enabled(wfsType.Name, field.Name)
		if !ok {
			continue
		}
		collection, err := s.wfsCollectionField(ctx, defs, field, name)
		if err != nil {
			return nil, err
		}
		if collection == nil {
			continue // Not a WFS field or no definition found
		}
		collections = append(collections, collection)
	}

	return collections, nil
}

func (s *Service) wfsCollectionField(ctx context.Context, defs base.DefinitionsSource, field *ast.FieldDefinition, name string) (collection *Collection, err error) {
	wfsd := field.Directives.ForName(base.GisWFSDirectiveName)
	if wfsd == nil {
		return nil, nil // Not a WFS field
	}

	def := defs.ForName(ctx, field.Type.Name())
	if def == nil || !sdl.IsDataObject(def) {
		return nil, nil // No definition found
	}

	perms := perm.PermissionsFromCtx(ctx)

	// Create a new collection for the field
	collection = &Collection{
		Name:        sdl.DirectiveArgValue(wfsd, "name", nil),
		Description: sdl.DirectiveArgValue(wfsd, "description", nil),
		IsReadOnly:  sdl.DirectiveArgValue(wfsd, "readonly", nil) == "true" || def.Directives.ForName(base.ObjectViewDirectiveName) != nil,
		QueryType:   dataObjectQueryType,
		// Set other fields as needed
		feature: featureDefinition{
			Name:          field.Name,
			Description:   field.Description,
			GeometryField: sdl.DirectiveArgValue(wfsd, "geometry", nil),
		},
	}
	if name != "" && collection.Name != name {
		return nil, nil // Not the requested collection
	}
	if collection.feature.GeometryField != "" {
		geomField := def.Fields.ForName(collection.feature.GeometryField)
		if geomField == nil {
			return nil, sdl.ErrorPosf(field.Position, "geometry field %s not found in definition %s", collection.feature.GeometryField, def.Name)
		}
		gi := field.Directives.ForName(base.FieldGeometryInfoDirectiveName)
		if gi != nil {
			collection.feature.GeometryType = sdl.DirectiveArgValue(gi, "type", nil)
			collection.feature.GeometrySRID, _ = strconv.Atoi(sdl.DirectiveArgValue(gi, "srid", nil))
		}
		if collection.feature.GeometrySRID == 0 {
			collection.feature.GeometrySRID = 4326 // Default SRID
		}
	}
	// make openAPI definition for the fields
	var fieldDefs []wfsFieldDefinition
	collection.Definition = openapi3.NewObjectSchema()
	var pks []string
	for _, f := range def.Fields {
		if f.Directives.ForName(base.FieldPrimaryKeyDirectiveName) != nil {
			pks = append(pks, f.Name)
		}
		if f.Name == collection.feature.GeometryField {
			// Skip geometry field, it is already handled
			// parse geometry info from directive
			continue
		}
		if f.Directives.ForName(base.GisWFSExcludeDirectiveName) != nil {
			continue // Excluded from WFS
		}
		if _, ok := perms.Enabled(def.Name, f.Name); !ok {
			continue // Not allowed to access this field
		}
		if !sdl.IsDataObjectFieldDefinition(f) {
			continue
		}
		for _, wfsField := range wfsFieldDef(ctx, f, defs, 0) {
			fieldDefs = append(fieldDefs, wfsField)
		}
	}

	qp, query := sdl.ObjectQueryDefinition(ctx, defs, def, sdl.QueryTypeSelect)
	if query == nil {
		return nil, sdl.ErrorPosf(field.Position, "query definition not found for collection %s", def.Name)
	}
	// create query for the feature
	collection.Query, collection.Variables, err = s.wfsFeaturesQuery(ctx, defs, qp, query)
	if err != nil {
		return nil, err
	}
	if len(pks) == 1 { // If there is a single primary key, use it for the feature query
		collection.feature.IdField = pks[0]
		qp, query = sdl.ObjectQueryDefinition(ctx, defs, def, sdl.QueryTypeSelectOne)
		if query != nil {
			collection.QueryFeature, collection.QueryFeatureIdType, err = s.wfsFeatureQuery(ctx, defs, qp, query)
		}
	}

	// add mutations
	if !collection.IsReadOnly {
		mp, mutation := sdl.ObjectMutationDefinition(ctx, defs, def, sdl.MutationTypeInsert)
		if mutation == nil {
			return nil, sdl.ErrorPosf(field.Position, "insert mutation definition not found for collection %s", def.Name)
		}
		collection.MutationInsert, collection.Variables, err = s.wfsFeatureInsert(ctx, defs, mp, mutation)
		if err != nil {
			return nil, err
		}
		mp, mutation = sdl.ObjectMutationDefinition(ctx, defs, def, sdl.MutationTypeUpdate)
		if mutation == nil {
			return nil, sdl.ErrorPosf(field.Position, "update mutation definition not found for collection %s", def.Name)
		}
		collection.MutationUpdate, collection.Variables, err = s.wfsFeatureUpdate(ctx, defs, mp, mutation)
		if err != nil {
			return nil, err
		}
		mp, mutation = sdl.ObjectMutationDefinition(ctx, defs, def, sdl.MutationTypeDelete)
		if mutation == nil {
			return nil, sdl.ErrorPosf(field.Position, "delete mutation definition not found for collection %s", def.Name)
		}
		collection.MutationDelete, collection.Variables, err = s.wfsFeatureDelete(ctx, defs, mp, mutation)
		if err != nil {
			return nil, err
		}
	}
	return collection, nil
}

type wfsFieldDefinition struct {
	Name      string
	Schema    *openapi3.Schema
	JQConvert string
}

func wfsFieldDef(ctx context.Context, field *ast.FieldDefinition, defs base.DefinitionsSource, depth int) []wfsFieldDefinition {
	if field.Type.NamedType != "" || field.Directives.ForName(base.GisWFSExcludeDirectiveName) != nil {
		return nil
	}
	var result []wfsFieldDefinition
	name := field.Name
	d := field.Directives.ForName(base.GisWFSFieldDirectiveName)
	if n := sdl.DirectiveArgValue(d, "name", nil); n != "" {
		name = n
	}
	if sdl.IsScalarType(field.Type.Name()) ||
		sdl.DirectiveArgValue(d, "flatten", nil) != "true" {
		return []wfsFieldDefinition{{
			Name:      name,
			Schema:    wfsFieldSchema(ctx, field, defs, 0),
			JQConvert: "." + field.Name,
		}}
	}
	def := defs.ForName(ctx, field.Type.Name())
	if def == nil && depth > 10 {
		return nil // No definition found, return empty slice
	}
	perms := perm.PermissionsFromCtx(ctx)
	for _, f := range def.Fields {
		if f.Directives.ForName(base.GisWFSExcludeDirectiveName) != nil {
			continue // Excluded from WFS
		}
		if _, ok := perms.Enabled(def.Name, f.Name); !ok {
			continue // Not allowed to access this field
		}
		d := f.Directives.ForName(base.GisWFSFieldDirectiveName)
		sep := sdl.DirectiveArgValue(d, "flatten_sep", nil)
		if f.Type.NamedType != "" {
			result = append(result, wfsFieldDefinition{
				Name:      name + sep + f.Name,
				Schema:    wfsFieldSchema(ctx, f, defs, 0),
				JQConvert: "." + name + "." + f.Name,
			})
			continue
		}
		subFields := wfsFieldDef(ctx, f, defs, depth+1)
		for _, v := range subFields {
			result = append(result, wfsFieldDefinition{
				Name:      name + sep + v.Name,
				Schema:    v.Schema,
				JQConvert: "." + name + v.JQConvert,
			})
		}
	}
	return result
}

func wfsFieldSchema(ctx context.Context, field *ast.FieldDefinition, defs base.DefinitionsSource, depth int) *openapi3.Schema {
	switch {
	case sdl.IsScalarType(field.Type.Name()):
		if field.Type.NamedType != "" {
			return scalarOpenAPISchema(field.Type.Name())
		}
		return openapi3.NewArraySchema().WithItems(scalarOpenAPISchema(field.Type.Name()))
	default:
		if depth > 10 {
			return nil // Prevent deep recursion
		}
		obj := openapi3.NewObjectSchema()
		def := defs.ForName(ctx, field.Type.Name())
		if def == nil {
			if field.Type.NamedType != "" {
				return obj // No definition found, return empty object schema
			}
			return openapi3.NewArraySchema().WithItems(obj) // Return array schema for non-named types
		}
		perms := perm.PermissionsFromCtx(ctx)
		for _, f := range def.Fields {
			if f.Directives.ForName(base.GisWFSExcludeDirectiveName) != nil {
				continue // Excluded from WFS
			}
			if _, ok := perms.Enabled(def.Name, f.Name); !ok {
				continue // Not allowed to access this field
			}
			schema := wfsFieldSchema(ctx, f, defs, depth+1)
			if schema == nil {
				continue // Skip if no schema found
			}
			obj.WithProperty(f.Name, schema)
		}
		if field.Type.NamedType != "" {
			return obj
		}
		return openapi3.NewArraySchema().WithItems(obj) // Return array schema for non-named types
	}
}

// Returns the WFS collection GraphQL query and OpenAPI schema for parameters.
func (s *Service) wfsFeaturesQuery(ctx context.Context, defs base.DefinitionsSource, path string, def *ast.FieldDefinition) (string, *openapi3.Schema, error) {
	return "", nil, nil
}

// Returns the WFS feature GraphQL query and OpenAPI schema for parameters.
func (s *Service) wfsFeatureQuery(ctx context.Context, defs base.DefinitionsSource, path string, def *ast.FieldDefinition) (string, *openapi3.Schema, error) {
	return "", nil, nil
}

// Returns the WFS feature GraphQL insert mutation and OpenAPI schema for parameters.
func (s *Service) wfsFeatureInsert(ctx context.Context, defs base.DefinitionsSource, path string, def *ast.FieldDefinition) (string, *openapi3.Schema, error) {
	// This method should return the insert mutation for WFS features
	// It fetches the insert mutation from the _wfs_features data object
	return "", nil, nil // Placeholder implementation
}

// Returns the WFS feature GraphQL update mutation and OpenAPI schema for parameters.
func (s *Service) wfsFeatureUpdate(ctx context.Context, defs base.DefinitionsSource, path string, def *ast.FieldDefinition) (string, *openapi3.Schema, error) {
	// This method should return the update mutation for WFS features
	// It fetches the update mutation from the _wfs_features data object
	return "", nil, nil // Placeholder implementation
}

// Returns the WFS feature GraphQL delete mutation and OpenAPI schema for parameters.
func (s *Service) wfsFeatureDelete(ctx context.Context, defs base.DefinitionsSource, path string, def *ast.FieldDefinition) (string, *openapi3.Schema, error) {
	// This method should return the delete mutation for WFS features
	// It fetches the delete mutation from the _wfs_features data object
	return "", nil, nil // Placeholder implementation
}

// Returns the collections for the saved queries in WFS.
func (s *Service) wfsSavedQueries(ctx context.Context, name string) ([]*Collection, error) {
	// This method should return the saved queries for WFS features
	// It fetches the saved queries from the _wfs_saved_queries data object
	return nil, nil // Placeholder implementation
}

// scalarOpenAPISchema returns the OpenAPI schema for a scalar GraphQL type name.
func scalarOpenAPISchema(typeName string) *openapi3.Schema {
	switch typeName {
	case "String":
		return openapi3.NewStringSchema()
	case "Int":
		return openapi3.NewInt32Schema()
	case "BigInt":
		return openapi3.NewInt64Schema()
	case "Float":
		return openapi3.NewFloat64Schema()
	case "Boolean":
		return openapi3.NewBoolSchema()
	case "Date":
		return openapi3.NewStringSchema().WithFormat("date")
	case "Timestamp":
		return openapi3.NewStringSchema().WithFormat("date-time")
	case "Time":
		return openapi3.NewStringSchema().WithFormat("time")
	case "Interval":
		return openapi3.NewStringSchema().WithFormat("interval")
	case "JSON":
		return openapi3.NewObjectSchema()
	case "IntRange", "BigIntRange":
		return openapi3.NewStringSchema().WithFormat("int-range")
	case "TimestampRange":
		return openapi3.NewStringSchema().WithFormat("timestamp-range")
	case "Geometry", "GeometryAggregation":
		return openapi3.NewObjectSchema().WithProperty("type",
			openapi3.NewStringSchema().WithEnum([]string{
				"Point", "LineString", "Polygon",
				"MultiPoint", "MultiLineString", "MultiPolygon",
				"GeometryCollection",
			}))
	case "H3Cell":
		return openapi3.NewStringSchema().WithFormat("h3string")
	case "Vector":
		return openapi3.NewStringSchema().WithFormat("vector")
	default:
		return openapi3.NewStringSchema()
	}
}
