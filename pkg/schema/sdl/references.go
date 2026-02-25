package sdl

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

type References struct {
	Name                  string
	ReferencesName        string
	sourceFields          []string
	referencesFields      []string
	Query                 string
	Description           string
	ReferencesQuery       string
	ReferencesDescription string
	isBackRef             bool

	IsM2M   bool
	M2MName string

	def        *ast.Directive
	sourceName string
}

func ReferencesInfo(def *ast.Directive) *References {
	if def == nil {
		return nil
	}
	if def.Name != base.ReferencesDirectiveName {
		return nil
	}
	ref := &References{
		def: def,
	}
	ref.ReferencesName = directiveArgValue(def, "references_name")
	ref.sourceFields = directiveArgChildValues(def, "source_fields")
	ref.referencesFields = directiveArgChildValues(def, "references_fields")
	ref.Query = directiveArgValue(def, "query")
	ref.Description = directiveArgValue(def, "description")
	ref.ReferencesQuery = directiveArgValue(def, "references_query")
	ref.ReferencesDescription = directiveArgValue(def, "references_description")
	ref.IsM2M = directiveArgValue(def, "is_m2m") == "true"
	ref.M2MName = directiveArgValue(def, "m2m_name")
	ref.Name = directiveArgValue(def, "name")
	return ref
}

func FieldReferencesInfo(ctx context.Context, defs base.DefinitionsSource, def *ast.Definition, field *ast.FieldDefinition) *References {
	fieldRefName := fieldDirectiveArgValue(field, base.FieldReferencesQueryDirectiveName, "name")
	for _, d := range def.Directives.ForNames(base.ReferencesDirectiveName) {
		name := directiveArgValue(d, "name")
		if name == fieldRefName {
			ref := ReferencesInfo(d)
			ref.sourceName = def.Name
			if ref.Query != field.Name && def.Name == ref.ReferencesName {
				ref.isBackRef = true
			}
			return ref
		}
	}
	refDef := defs.ForName(ctx, field.Type.Name())
	if refDef == nil {
		return nil
	}
	for _, d := range refDef.Directives.ForNames(base.ReferencesDirectiveName) {
		name := directiveArgValue(d, "name")
		if name == fieldRefName {
			ref := ReferencesInfo(d)
			ref.sourceName = refDef.Name
			ref.isBackRef = true
			return ref
		}
	}
	return nil
}

func referencesInfo(ref *ast.Directive, sourceName string, isBackRef bool) *References {
	if ref.Name != base.ReferencesDirectiveName {
		return nil
	}
	info := ReferencesInfo(ref)
	info.sourceName = sourceName
	info.isBackRef = isBackRef
	return info
}

func (info *References) JoinConditions(ctx context.Context, defs base.DefinitionsSource, leftAlias, rightAlias string, isDBLeft, isDBRight bool) (string, error) {
	left := info.sourceName
	right := info.ReferencesName
	leftFields := info.sourceFields
	rightFields := info.referencesFields
	if info.isBackRef {
		left = info.ReferencesName
		right = info.sourceName
		leftFields = info.referencesFields
		rightFields = info.sourceFields
	}

	leftObject := defs.ForName(ctx, left)
	if leftObject == nil {
		return "", ErrorPosf(base.CompiledPos(""), "object %s has unknown references object %s", left, right)
	}
	leftInfo := DataObjectInfo(leftObject)
	rightObject := defs.ForName(ctx, right)
	if rightObject == nil {
		return "", ErrorPosf(base.CompiledPos(""), "object %s has unknown references object %s", right, left)
	}
	rightInfo := DataObjectInfo(rightObject)

	return joinConditions(leftInfo, rightInfo, leftAlias, rightAlias, leftFields, rightFields, isDBLeft, isDBRight)
}

func (info *References) ToM2MJoinConditions(ctx context.Context, defs base.DefinitionsSource, leftAlias, m2mAlias string, isDBLeft, isDBRight bool) (string, error) {
	if !info.IsM2M {
		return "", ErrorPosf(base.CompiledPos(""), "object %s is not m2m relation", info.Name)
	}
	leftObject := defs.ForName(ctx, info.sourceName)
	if leftObject == nil {
		return "", ErrorPosf(base.CompiledPos(""), "object %s has unknown references object %s", info.sourceName, info.M2MName)
	}
	leftInfo := DataObjectInfo(leftObject)
	rightObject := defs.ForName(ctx, info.M2MName)
	if rightObject == nil {
		return "", ErrorPosf(base.CompiledPos(""), "object %s has unknown references object %s", info.M2MName, info.sourceName)
	}
	rightInfo := DataObjectInfo(rightObject)
	return joinConditions(leftInfo, rightInfo, leftAlias, m2mAlias, info.sourceFields, info.referencesFields, isDBLeft, isDBRight)
}

func (info *References) FromM2MJoinConditions(ctx context.Context, defs base.DefinitionsSource, m2mAlias, rightAlias string, isDBLeft, isDBRight bool) (string, error) {
	if !info.IsM2M {
		return "", ErrorPosf(base.CompiledPos(""), "object %s is not m2m relation", info.Name)
	}
	leftObject := defs.ForName(ctx, info.M2MName)
	if leftObject == nil {
		return "", ErrorPosf(base.CompiledPos(""), "object %s has unknown references object %s", info.M2MName, info.sourceName)
	}
	leftInfo := DataObjectInfo(leftObject)
	refObjectInfo := leftInfo.M2MReferencesQueryInfo(ctx, defs, info.Name)
	return refObjectInfo.JoinConditions(ctx, defs, m2mAlias, rightAlias, isDBLeft, isDBRight)
}

func joinConditions(left, right *Object, leftAlias, rightAlias string, leftFields, rightFields []string, isDBLeft, isDBRight bool) (string, error) {
	var conditions []string
	for i := range leftFields {
		leftField := left.FieldForName(leftFields[i])
		rightField := right.FieldForName(rightFields[i])
		if leftField == nil {
			return "", ErrorPosf(left.def.Position, "object %s has unknown source field %s", left.Name, leftFields[i])
		}
		if rightField == nil {
			return "", ErrorPosf(right.def.Position, "object %s has unknown references field %s", right.Name, rightFields[i])
		}
		lSQL := leftField.Name
		if leftAlias != "" {
			lSQL = leftAlias + "." + leftField.Name
		}
		rSQL := rightField.Name
		if rightAlias != "" {
			rSQL = rightAlias + "." + rightField.Name
		}
		if isDBLeft {
			lSQL = leftField.SQL(leftAlias)
		}
		if isDBRight {
			rSQL = rightField.SQL(rightAlias)
		}
		conditions = append(conditions, fmt.Sprintf("(%s = %s)", lSQL, rSQL))
	}
	if len(conditions) == 1 {
		return strings.Trim(conditions[0], "()"), nil
	}
	return strings.Join(conditions, " AND "), nil
}

func (info *References) ReferencesFields() []string {
	if !info.isBackRef {
		return info.referencesFields
	}
	return info.sourceFields
}

func (info *References) SourceFields() []string {
	if info.isBackRef {
		return info.referencesFields
	}
	return info.sourceFields
}

func (info *References) ReferencesObjectDef(ctx context.Context, defs base.DefinitionsSource) *ast.Definition {
	if !info.isBackRef {
		return defs.ForName(ctx, info.ReferencesName)
	}
	return defs.ForName(ctx, info.sourceName)
}
