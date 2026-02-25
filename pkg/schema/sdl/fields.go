package sdl

import (
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

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
	Dim          int
}

func FieldInfo(field *ast.Field) *Field {
	info := FieldDefinitionInfo(field.Definition, field.ObjectDefinition)
	info.field = field
	return info
}

func FieldDefinitionInfo(field *ast.FieldDefinition, object *ast.Definition) *Field {
	dim, _ := strconv.Atoi(fieldDirectiveArgValue(field, base.FieldDimDirectiveName, "len"))
	return &Field{
		Name:         field.Name,
		dbName:       fieldDirectiveArgValue(field, base.FieldSourceDirectiveName, "field"),
		sql:          fieldDirectiveArgValue(field, base.FieldSqlDirectiveName, "exp"),
		geometrySRID: fieldDirectiveArgValue(field, base.FieldGeometryInfoDirectiveName, "srid"),
		geometryType: fieldDirectiveArgValue(field, base.FieldGeometryInfoDirectiveName, "type"),
		sequence:     fieldDirectiveArgValue(field, base.FieldDefaultDirectiveName, "sequence"),
		Dim:          dim,
		def:          field,
		object:       object,
	}
}

func (f *Field) IsRequired() bool {
	return f.def.Directives.ForName(base.FieldPrimaryKeyDirectiveName) != nil || f.def.Type.NonNull
}

func (f *Field) SequenceName() string {
	return f.sequence
}

func (f *Field) IsReferencesSubquery() bool {
	return f.def.Directives.ForName(base.FieldReferencesQueryDirectiveName) != nil
}

func (f *Field) IsNotDBField() bool {
	return f.IsCalcField() || f.IsReferencesSubquery() ||
		f.def.Directives.ForName(base.FunctionCallDirectiveName) != nil ||
		f.def.Directives.ForName(base.FunctionCallTableJoinDirectiveName) != nil ||
		f.def.Directives.ForName(base.JoinDirectiveName) != nil ||
		f.def.Name == base.QueryTimeJoinsFieldName ||
		f.def.Name == base.QueryTimeSpatialFieldName
}

func (f *Field) IsGeometry() bool {
	return f.def.Type.Name() == base.GeometryTypeName
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
	if fs == "-" {
		return "-"
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
	return f.SQLFieldFunc(prefix, func(s string) string { return base.Ident(s) })
}

func (f *Field) SQLFieldFunc(prefix string, fn func(string) string) string {
	sp := prefix
	if prefix != "" {
		sp += "."
	}
	fs := f.dbName
	if fs == "" {
		fs = f.Name
	}
	if fs == "-" {
		return "NULL"
	}
	if f.sql == "" {
		return sp + fn(fs)
	}
	fields := f.UsingFields()
	sql := f.sql
	for _, field := range fields {
		if field == f.Name {
			sql = strings.ReplaceAll(sql, "["+field+"]", sp+fn(fs))
			continue
		}
		fd := f.object.Fields.ForName(field)
		if fd == nil {
			f.sql = strings.ReplaceAll(f.sql, "["+field+"]", "NULL")
			continue
		}
		info := FieldDefinitionInfo(fd, f.object)
		fs := info.SQLFieldFunc(prefix, fn)
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

func (f *Field) UsingFields() []string {
	return ExtractFieldsFromSQL(f.sql)
}

func (f *Field) Definition() *ast.FieldDefinition {
	return f.def
}

func IsTimescaleKey(def *ast.FieldDefinition) bool {
	if def.Type.NamedType != base.TimestampTypeName {
		return false
	}
	return def.Directives.ForName(base.FieldTimescaleKeyDirectiveName) != nil
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

func ExtraFieldName(def *ast.FieldDefinition) string {
	if d := def.Directives.ForName(base.FieldExtraFieldDirectiveName); d != nil {
		if t := directiveArgValue(d, "name"); t != "" {
			return t
		}
	}
	return ""
}
