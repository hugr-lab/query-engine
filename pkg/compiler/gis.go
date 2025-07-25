package compiler

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// GIS operations and directives

// makeWFSType creates a WFS type with fields that have queries for the wfs features.
func makeWFSType(schema *ast.SchemaDocument) gqlerror.List {
	def := base.GisWFSTypeTemplate()
	errs := gqlerror.List{}
	for _, dt := range schema.Definitions {
		if dt.Kind != ast.Object &&
			!IsDataObject(dt) {
			continue
		}
		wfs := dt.Directives.ForName(base.GisWFSDirectiveName)
		if wfs == nil {
			continue
		}
		field, err := wfsCollectionField(schema, dt)
		if err != nil {
			errs = append(errs, gqlerror.WrapIfUnwrapped(err))
			continue
		}
		if def.Fields.ForName(field.Name) != nil {
			errs = append(errs, gqlerror.Errorf("WFS field %s already exists in WFS type %s", field.Name, def.Name))
			continue
		}
		def.Fields = append(def.Fields, field)
	}
	if len(errs) != 0 {
		return errs
	}
	if len(def.Fields) == 0 {
		return nil
	}
	schema.Definitions = append(schema.Definitions, def)

	return nil
}

func addWFSCollectionField(schema *ast.SchemaDocument, def *ast.Definition) error {
	field, err := wfsCollectionField(schema, def)
	if err != nil || field == nil {
		return err
	}
	dt := schema.Definitions.ForName(base.GisWFSTypeName)
	if dt == nil {
		dt = base.GisWFSTypeTemplate()
		schema.Definitions = append(schema.Definitions, dt)
	}
	if def.Fields.ForName(field.Name) != nil {
		return ErrorPosf(def.Position, "WFS field %s already exists in WFS type %s", field.Name, def.Name)
	}
	dt.Fields = append(dt.Fields, field)
	return nil
}

func wfsCollectionField(schema *ast.SchemaDocument, def *ast.Definition) (*ast.FieldDefinition, error) {
	d := def.Directives.ForName(base.GisWFSDirectiveName)
	// arguments from original query
	var qtn string
	for _, d := range def.Directives.ForNames(queryDirectiveName) {
		if directiveArgValue(d, "type") == queryTypeTextSelect {
			qtn = d.Arguments.ForName("name").Value.Raw
			break
		}
	}
	m := objectModuleType(schema.Definitions, def, ModuleQuery)
	if m == nil {
		return nil, ErrorPosf(def.Position, "module query %s not found for WFS collection field", ObjectModule(def))
	}
	qt := m.Fields.ForName(qtn)
	if qt == nil {
		return nil, ErrorPosf(def.Position, "query field %s not found in module %s for WFS collection field", qtn, ObjectModule(def))
	}
	wfsName := strings.ReplaceAll(directiveArgValue(d, "name"), ":", "_")

	return &ast.FieldDefinition{
		Name:        wfsName,
		Description: directiveArgValue(d, "description"),
		Arguments:   copyArgumentDefinitionList(schema, def, qt.Arguments),
		Position:    CompiledPosName("gis"),
		Directives:  copyDirectiveList(schema, def, qt.Directives),
		Type: ast.ListType(
			ast.NamedType(def.Name, CompiledPosName("gis")),
			CompiledPosName("gis"),
		),
	}, nil
}

func validateGisDirectives(def *ast.Definition, opt *Options) error {
	if def.Kind != ast.Object && !IsDataObject(def) {
		return ErrorPosf(def.Position, "definition %s should be a data object", def.Name)
	}
	prefix := ""
	if opt != nil && opt.AsModule {
		prefix = opt.Name + ":"
	}
	d := def.Directives.ForName(base.GisWFSDirectiveName)
	if d == nil {
		return nil
	}
	wfsName := directiveArgValue(d, "name")
	if d.Arguments.ForName("name") == nil {
		d.Arguments = append(d.Arguments, &ast.Argument{
			Name:     "name",
			Position: d.Position,
		})
	}
	if wfsName == "" {
		wfsName = def.Name
	}
	wfsName = prefix + wfsName
	d.Arguments.ForName("name").Value = &ast.Value{
		Raw:      wfsName,
		Kind:     ast.StringValue,
		Position: d.Position,
	}
	if strings.ContainsAny(wfsName, " ,;!@#$%^&*()+=-[]{}|\\\"'<>?/") {
		return ErrorPosf(d.Position, "WFS name %s should not contain special characters", wfsName)
	}
	if strings.Count(wfsName, ":") > 1 || wfsName[0] == ':' {
		return ErrorPosf(d.Position, "WFS name %s should not contain more than one colon and should not start with a colon", wfsName)
	}
	desc := directiveArgValue(d, "description")
	if d.Arguments.ForName("description") == nil {
		d.Arguments = append(d.Arguments, &ast.Argument{
			Name:     "description",
			Position: d.Position,
		})
	}
	if desc == "" {
		desc = def.Description
		if desc == "" {
			desc = "WFS for " + def.Name
		}
		d.Arguments.ForName("description").Value = &ast.Value{
			Raw:      desc,
			Kind:     ast.StringValue,
			Position: d.Position,
		}
	}

	geomField := directiveArgValue(d, "geometry")
	if geomField == "" {
		return nil
	}

	field := def.Fields.ForName(geomField)
	if field == nil {
		return ErrorPosf(d.Position, "WFS geometry field %s not found in object %s", geomField, def.Name)
	}
	if field.Directives.ForName(base.FieldGeometryInfoDirectiveName) == nil {
		return ErrorPosf(d.Position, "WFS geometry field %s of object %s should have @%s directive", geomField, def.Name, base.FieldGeometryInfoDirectiveName)
	}

	return nil
}
