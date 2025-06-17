package compiler

import (
	"regexp"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	fieldPrimaryKeyDirectiveName     = "pk"
	fieldUniqueRuleDirectiveName     = "unique_rule"
	fieldExcludeFilterDirectiveName  = "exclude_filter"
	fieldDefaultDirectiveName        = "default"
	fieldFilterRequiredDirectiveName = "filter_required"
	fieldTimescaleKeyDirectiveName   = "timescale_key"
	FieldMeasurementDirectiveName    = "measurement"
	FieldMeasurementFuncArgName      = "measurement_func"
)

func validateObjectField(defs Definitions, def *ast.Definition, field *ast.FieldDefinition) error {
	if IsFunctionCall(field) {
		return validateFunctionCall(defs, def, field, false)
	}
	if strings.HasPrefix(field.Name, "_") {
		return ErrorPosf(field.Position, "field %s of object %s can't start with _", field.Name, def.Name)
	}
	for _, d := range field.Directives {
		switch d.Name {
		case fieldReferencesDirectiveName:
			if !IsDataObject(def) {
				return ErrorPosf(d.Position, "field %s of object %s can't have directive %s", field.Name, def.Name, d.Name)
			}
			ref := fieldReferencesInfo(field.Name, d)
			dir := ref.directive()
			if err := validateObjectReferences(defs, def, dir); err != nil {
				return err
			}
			def.Directives = append(def.Directives, dir)
		case fieldUniqueRuleDirectiveName, fieldDefaultDirectiveName:
			if DataObjectType(def) != Table {
				return ErrorPosf(d.Position, "field %s of object %s can't have directive %s", field.Name, def.Name, d.Name)
			}
			// check scalar value
			if !IsScalarType(field.Type.Name()) &&
				field.Type.Name() != JSONTypeName {
				return ErrorPosf(d.Position, "field %s of object %s should be a scalar type", field.Name, def.Name)
			}
		case fieldPrimaryKeyDirectiveName, fieldExcludeFilterDirectiveName,
			fieldFilterRequiredDirectiveName, base.FieldSourceDirectiveName:
		case base.FieldGeometryInfoDirectiveName:
			if field.Type.Name() != GeometryTypeName {
				return ErrorPosf(d.Position, "field %s of object %s should be a Geometry type", field.Name, def.Name)
			}
		case fieldTimescaleKeyDirectiveName:
			if !IsDataObject(def) {
				return ErrorPosf(d.Position, "field %s of object %s can't have directive %s", field.Name, def.Name, d.Name)
			}
			if def.Directives.ForName(objectHyperTableDirectiveName) == nil {
				return ErrorPosf(d.Position, "field %s of object %s should have @hypertable directive", field.Name, def.Name)
			}
		case JoinDirectiveName:
			if !IsDataObject(def) {
				return ErrorPosf(d.Position, "field %s of object %s can't have directive %s", field.Name, def.Name, d.Name)
			}
			if err := validateJoin(defs, def, field); err != nil {
				return err
			}
		case FieldMeasurementDirectiveName:
			if def.Directives.ForName(objectCubeDirectiveName) == nil {
				return ErrorPosf(d.Position, "field %s of object %s should have @cube directive", field.Name, def.Name)
			}
			if field.Type.NamedType == "" || !IsScalarType(field.Type.Name()) {
				return ErrorPosf(d.Position, "measurement field %s of object %s should be scalar type", field.Name, def.Name)
			}
			if ScalarTypes[field.Type.Name()].MeasurementAggs == "" {
				return ErrorPosf(d.Position, "measurement field %s of object %s should be a valid scalar type", field.Name, def.Name)
			}
		case base.DeprecatedDirectiveName, base.FieldSqlDirectiveName:
		default:
			return ErrorPosf(d.Position, "field %s of object %s has unknown directive %s", field.Name, def.Name, d.Name)
		}
	}
	if td := defs.ForName(field.Type.Name()); td == nil && !IsScalarType(field.Type.Name()) {
		return ErrorPosf(field.Position, "field %s of object %s has unknown type %s", field.Name, def.Name, field.Type.Name())
	}

	if !IsScalarType(field.Type.Name()) {
		return nil
	}
	// add field transformation parameters
	if field.Type.NamedType != "" {
		field.Arguments = ScalarTypes[field.Type.Name()].Arguments
		if field.Type.NamedType == TimestampTypeName &&
			field.Directives.ForName(fieldTimescaleKeyDirectiveName) != nil {
			field.Arguments = append(field.Arguments, &ast.ArgumentDefinition{
				Name:        "gapfill",
				Description: "Extracts the specified part of the timestamp",
				Type:        ast.NamedType("Boolean", compiledPos()),
				Position:    compiledPos(),
			})
		}
		if efFunc := ScalarTypes[field.Type.Name()].ExtraField; efFunc != nil {
			if ef := efFunc(field); ef != nil {
				def.Fields = append(def.Fields, ef)
			}
		}
	}
	// add measurement aggregation arguments
	if field.Directives.ForName(FieldMeasurementDirectiveName) != nil {
		field.Arguments = append(field.Arguments, &ast.ArgumentDefinition{
			Name:        FieldMeasurementFuncArgName,
			Description: "Aggregation function for measurement field",
			Type:        ast.NamedType(ScalarTypes[field.Type.Name()].MeasurementAggs, compiledPos()),
			Position:    compiledPos(),
		})
	}
	info := DataObjectInfo(def)
	if info == nil {
		return nil
	}
	fieldInfo := info.FieldForName(field.Name)
	if fieldInfo == nil {
		return ErrorPosf(field.Position, "field %s of object %s not found", field.Name, def.Name)
	}
	// check runtime calculations
	return fieldInfo.validate()
}

func IsTimescaleKey(def *ast.FieldDefinition) bool {
	if def.Type.NamedType != TimestampTypeName {
		return false
	}
	return def.Directives.ForName(fieldTimescaleKeyDirectiveName) != nil
}

type Field struct {
	Name     string
	dbName   string
	sql      string
	sequence string
	field    *ast.Field
	def      *ast.FieldDefinition
	object   *ast.Definition

	geometrySRID string
	geometryType string
}

func FieldInfo(field *ast.Field) *Field {
	info := fieldInfo(field.Definition, field.ObjectDefinition)
	info.field = field

	return info
}

func fieldInfo(field *ast.FieldDefinition, object *ast.Definition) *Field {
	return &Field{
		Name:         field.Name,
		dbName:       fieldDirectiveArgValue(field, base.FieldSourceDirectiveName, "field"),
		sql:          fieldDirectiveArgValue(field, base.FieldSqlDirectiveName, "exp"),
		geometrySRID: fieldDirectiveArgValue(field, base.FieldGeometryInfoDirectiveName, "srid"),
		geometryType: fieldDirectiveArgValue(field, base.FieldGeometryInfoDirectiveName, "type"),
		sequence:     fieldDirectiveArgValue(field, fieldDefaultDirectiveName, "sequence"),
		def:          field,
		object:       object,
	}
}

func (f *Field) IsRequired() bool {
	return f.def.Directives.ForName(fieldPrimaryKeyDirectiveName) != nil || f.def.Type.NonNull
}

func (f *Field) SequenceName() string {
	return f.sequence
}

func (f *Field) IsReferencesSubquery() bool {
	return f.def.Directives.ForName(fieldReferencesQueryDirectiveName) != nil
}

func (f *Field) IsNotDBField() bool {
	return f.IsCalcField() || f.IsReferencesSubquery() ||
		f.def.Directives.ForName(functionCallDirectiveName) != nil ||
		f.def.Directives.ForName(functionCallTableJoinDirectiveName) != nil ||
		f.def.Directives.ForName(JoinDirectiveName) != nil ||
		f.def.Name == QueryTimeJoinsFieldName ||
		f.def.Name == QueryTimeSpatialFieldName
}

func (f *Field) IsGeometry() bool {
	return f.def.Type.Name() == GeometryTypeName
}

func (f *Field) GeometrySRID() string {
	if f.geometrySRID == "" {
		return "4326"
	}
	return f.geometrySRID
}

func (f *Field) IsCalcField() bool {
	return f.sql != ""
}

func (f *Field) FieldSourceName(prefix string, ident bool) string {
	fs := f.dbName
	if fs == "" {
		fs = f.Name
	}
	if ident {
		fs = base.Ident(fs)
	}
	if prefix != "" {
		prefix += "."
	}
	return prefix + fs
}

func (f *Field) SQL(prefix string) string {
	sp := prefix
	if prefix != "" {
		sp += "."
	}
	fs := f.dbName
	if fs == "" {
		fs = f.Name
	}
	if f.sql == "" {
		return sp + base.Ident(fs)
	}
	fields := f.UsingFields()
	sql := f.sql
	for _, field := range fields {
		if field == f.Name {
			sql = strings.ReplaceAll(sql, "["+field+"]", sp+base.Ident(fs))
			continue
		}
		fd := f.object.Fields.ForName(field)
		if fd == nil {
			f.sql = strings.ReplaceAll(f.sql, "["+field+"]", "NULL")
			continue
		}
		info := fieldInfo(fd, f.object)
		fs := info.SQL(prefix)
		sql = strings.ReplaceAll(sql, "["+field+"]", fs)
	}
	return sql
}

func (f *Field) IsTransformed() bool {
	if f.sql == "" {
		return false
	}
	fields := f.UsingFields()
	return len(fields) == 1 && fields[0] == f.Name
}

func (f *Field) TransformSQL(sql string) string {
	if f.sql == "" {
		return sql
	}

	return strings.ReplaceAll(f.sql, "["+f.Name+"]", sql)
}

func (f *Field) validate() error {
	if f.IsCalcField() {
		return nil
	}
	if f.sql == "" {
		return nil
	}

	deps := f.UsingFields()
	i := 0
	for ; i < 4; i++ { // check 3 degrees of dependencies
		var newDeps []string
		for _, dep := range deps {
			fd := f.object.Fields.ForName(dep)
			if fd == nil {
				return ErrorPosf(f.def.Position, "field %s has unknown field %s in SQL expression", f.Name, dep)
			}
			info := fieldInfo(fd, f.object)
			newDeps = append(newDeps, info.UsingFields()...)
		}
		if len(newDeps) == 0 {
			break
		}
		deps = append(deps, newDeps...)
	}

	if slices.Contains(deps, f.Name) || i == 4 {
		return ErrorPosf(f.def.Position, "field %s has circular dependency in SQL expression", f.Name)
	}
	return nil
}

// extract using fields from sql query
func (f *Field) UsingFields() []string {
	return ExtractFieldsFromSQL(f.sql)
}

func (f *Field) Definition() *ast.FieldDefinition {
	return f.def
}

var reSQLField = regexp.MustCompile(`\[\$?[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\]`)

func ExtractFieldsFromSQL(sql string) []string {
	if sql == "" {
		return nil
	}
	matches := reSQLField.FindAllString(sql, -1)
	if matches == nil {
		return nil
	}
	fields := make([]string, 0, len(matches))
	for _, match := range matches {
		fields = append(fields, strings.Trim(match, "[]"))
	}
	return RemoveFieldsDuplicates(fields)
}

func TransformBaseFieldType(field *ast.FieldDefinition) string {
	if d := field.Directives.ForName(base.FieldExtraFieldDirectiveName); d != nil {
		if t := directiveArgValue(d, "base_type"); t != "" {
			return t
		}
	}

	if field.Type.NamedType != "" {
		return field.Type.NamedType
	}
	return ""
}

func IsExtraField(def *ast.FieldDefinition) bool {
	return def.Directives.ForName(base.FieldExtraFieldDirectiveName) != nil
}
