package compiler

import (
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

const (
	referencesDirectiveName      = "references"
	fieldReferencesDirectiveName = "field_references"
)

func validateObjectReferences(defs Definitions, def *ast.Definition, refDirective *ast.Directive) error {
	ref := ReferencesInfo(refDirective)
	if ref == nil {
		return ErrorPosf(def.Position, "object %s @%s is not references directive", def.Name, refDirective.Name)
	}
	refObject := defs.ForName(ref.ReferencesName)
	if ref.IsM2M { // m2m relation should be generated after validation step
		return ErrorPosf(def.Position, "references %s should not have is_m2m arguments", ref.ReferencesName)
	}
	if refObject == nil {
		return ErrorPosf(def.Position, "object %s has unknown references object %s", def.Name, ref.ReferencesName)
	}
	// query and description should be set for both objects
	if ref.Query == "" {
		ref.Query = ref.ReferencesName
	}
	if ref.Description == "" {
		ref.Description = refObject.Description
	}
	if ref.ReferencesQuery == "" {
		ref.ReferencesQuery = def.Name
	}
	if ref.ReferencesDescription == "" {
		ref.ReferencesDescription = def.Description
	}
	if ref.Name == "" {
		ref.Name = def.Name + "_" + ref.ReferencesName
		if len(ref.sourceFields) == 1 {
			ref.Name += "_" + ref.sourceFields[0]
		}
	}

	if len(ref.sourceFields) == 0 {
		return ErrorPosf(def.Position, "object %s has empty source_fields", def.Name)
	}
	if len(ref.referencesFields) == 0 {
		pk := objectPrimaryKeys(refObject)
		if len(pk) != 1 {
			return ErrorPosf(def.Position, "field references object %s has no or multiple primary key fields, set up references_fields ", ref.ReferencesName)
		}
		ref.referencesFields = pk
	}

	if len(ref.sourceFields) != len(ref.referencesFields) {
		return ErrorPosf(def.Position, "object %s has different number of fields in references and source_fields", def.Name)
	}

	for i := range ref.sourceFields {
		sourceField := def.Fields.ForName(ref.sourceFields[i])
		refField := refObject.Fields.ForName(ref.referencesFields[i])
		if sourceField == nil {
			return ErrorPosf(def.Position, "object %s has unknown source field %s", def.Name, ref.sourceFields[i])
		}
		if refField == nil {
			return ErrorPosf(def.Position, "object %s has unknown references field %s", def.Name, ref.referencesFields[i])
		}
		if !IsScalarType(sourceField.Type.Name()) {
			return ErrorPosf(def.Position, "object %s has non-scalar type of source field %s", def.Name, ref.sourceFields[i])
		}
		if sourceField.Type.Name() != refField.Type.Name() {
			return ErrorPosf(def.Position, "object %s has different types of source field %s and references field %s", def.Name, ref.sourceFields[i], ref.referencesFields[i])
		}
	}

	ref.updateDefinition()
	return nil
}

func validateObjectReferencesDirectives(def *ast.Definition) error {
	names := make(map[string]struct{})
	for _, ref := range def.Directives.ForNames(referencesDirectiveName) {
		r := ReferencesInfo(ref)
		if r.Name == "" {
			return ErrorPosf(def.Position, "object %s has empty references name", def.Name)
		}
		if _, ok := names[r.Name]; ok {
			return ErrorPosf(def.Position, "object %s has non-unique references name %s", def.Name, r.Name)
		}
		names[r.Name] = struct{}{}
	}
	return nil
}

func addM2MReferences(defs Definitions, def *ast.Definition) {
	// add M2M directives to referenced objects
	aInfo := ReferencesInfo(def.Directives.ForNames(referencesDirectiveName)[0])
	bInfo := ReferencesInfo(def.Directives.ForNames(referencesDirectiveName)[1])

	aReferences := &References{
		Name:                  aInfo.Name,
		ReferencesName:        bInfo.ReferencesName,
		sourceFields:          aInfo.referencesFields,
		referencesFields:      aInfo.sourceFields,
		Query:                 aInfo.ReferencesQuery,
		Description:           aInfo.ReferencesDescription,
		ReferencesQuery:       bInfo.ReferencesQuery,
		ReferencesDescription: bInfo.ReferencesDescription,
		IsM2M:                 true,
		M2MName:               def.Name,
	}
	bReferences := &References{
		Name:                  bInfo.Name,
		ReferencesName:        aInfo.ReferencesName,
		sourceFields:          bInfo.referencesFields,
		referencesFields:      bInfo.sourceFields,
		Query:                 bInfo.ReferencesQuery,
		Description:           bInfo.ReferencesDescription,
		ReferencesQuery:       aInfo.ReferencesQuery,
		ReferencesDescription: aInfo.ReferencesDescription,
		IsM2M:                 true,
		M2MName:               def.Name,
	}
	source := defs.ForName(aInfo.ReferencesName)
	references := defs.ForName(bInfo.ReferencesName)
	source.Directives = append(source.Directives, aReferences.directive())
	references.Directives = append(references.Directives, bReferences.directive())
}

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
	if def.Name != referencesDirectiveName {
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

func fieldReferencesInfo(fieldName string, ref *ast.Directive) *References {
	d := &References{
		sourceFields: []string{fieldName},
	}
	d.ReferencesName = directiveArgValue(ref, "references_name")
	d.referencesFields = []string{directiveArgValue(ref, "field")}
	d.Query = directiveArgValue(ref, "query")
	d.Description = directiveArgValue(ref, "description")
	d.ReferencesQuery = directiveArgValue(ref, "references_query")
	d.ReferencesDescription = directiveArgValue(ref, "references_description")
	d.Name = directiveArgValue(ref, "name")

	return d
}

func referencesInfo(ref *ast.Directive, sourceName string, isBackRef bool) *References {
	if ref.Name != referencesDirectiveName {
		return nil
	}
	info := ReferencesInfo(ref)
	info.sourceName = sourceName
	info.isBackRef = isBackRef
	return info
}

func (ref *References) directive() *ast.Directive {
	var sourceFields, refFields ast.ChildValueList
	for _, f := range ref.sourceFields {
		sourceFields = append(sourceFields, &ast.ChildValue{Value: &ast.Value{Raw: f, Kind: ast.StringValue}})
	}
	for _, f := range ref.referencesFields {
		refFields = append(refFields, &ast.ChildValue{Value: &ast.Value{Raw: f, Kind: ast.StringValue}})
	}
	isM2M := "false"
	if ref.IsM2M {
		isM2M = "true"
	}
	if ref.Name == "" {
		ref.Name = ref.ReferencesName
		if len(ref.sourceFields) == 1 {
			ref.Name += "_" + ref.sourceFields[0]
		}
	}
	return &ast.Directive{
		Name: "references",
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Raw: ref.Name, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "references_name", Value: &ast.Value{Raw: ref.ReferencesName, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "source_fields", Value: &ast.Value{Children: sourceFields, Kind: ast.ListValue}, Position: compiledPos()},
			{Name: "references_fields", Value: &ast.Value{Children: refFields, Kind: ast.ListValue}, Position: compiledPos()},
			{Name: "query", Value: &ast.Value{Raw: ref.Query, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "description", Value: &ast.Value{Raw: ref.Description, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "references_query", Value: &ast.Value{Raw: ref.ReferencesQuery, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "references_description", Value: &ast.Value{Raw: ref.ReferencesDescription, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "is_m2m", Value: &ast.Value{Raw: isM2M, Kind: ast.BooleanValue}, Position: compiledPos()},
			{Name: "m2m_name", Value: &ast.Value{Raw: ref.M2MName, Kind: ast.StringValue}, Position: compiledPos()},
		},
		Position: compiledPos(),
	}
}

func (ref *References) updateDefinition() {
	d := ref.directive()

	for _, a := range d.Arguments {
		o := ref.def.Arguments.ForName(a.Name)
		if o == nil {
			ref.def.Arguments = append(ref.def.Arguments, a)
			continue
		}
		o.Value = a.Value
	}
}

func referenceQueryDirective(refName, name string, isM2M bool, m2mName string) *ast.Directive {
	return &ast.Directive{Name: fieldReferencesQueryDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "references_name", Value: &ast.Value{Raw: refName, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "is_m2m", Value: &ast.Value{Raw: fmt.Sprint(isM2M), Kind: ast.BooleanValue}, Position: compiledPos()},
			{Name: "m2m_name", Value: &ast.Value{Raw: m2mName, Kind: ast.StringValue}, Position: compiledPos()},
		},
		Position: compiledPos(),
	}
}

func (info *References) JoinConditions(defs Definitions, leftAlias, rightAlias string, isDBLeft, isDBRight bool) (string, error) {
	// if def is references name and is not m2m simple join
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

	leftObject := defs.ForName(left)
	if leftObject == nil {
		return "", ErrorPosf(compiledPos(), "object %s has unknown references object %s", left, right)
	}
	leftInfo := DataObjectInfo(leftObject)
	rightObject := defs.ForName(right)
	if rightObject == nil {
		return "", ErrorPosf(compiledPos(), "object %s has unknown references object %s", right, left)
	}
	rightInfo := DataObjectInfo(rightObject)

	return joinConditions(leftInfo, rightInfo, leftAlias, rightAlias, leftFields, rightFields, isDBLeft, isDBRight)
}

func (info *References) ToM2MJoinConditions(defs Definitions, leftAlias, m2mAlias string, isDBLeft, isDBRight bool) (string, error) {
	if !info.IsM2M {
		return "", ErrorPosf(compiledPos(), "object %s is not m2m relation", info.Name)
	}

	leftObject := defs.ForName(info.sourceName)
	if leftObject == nil {
		return "", ErrorPosf(compiledPos(), "object %s has unknown references object %s", info.sourceName, info.M2MName)
	}
	leftInfo := DataObjectInfo(leftObject)
	rightObject := defs.ForName(info.M2MName)
	if rightObject == nil {
		return "", ErrorPosf(compiledPos(), "object %s has unknown references object %s", info.M2MName, info.sourceName)
	}
	rightInfo := DataObjectInfo(rightObject)

	return joinConditions(leftInfo, rightInfo, leftAlias, m2mAlias, info.sourceFields, info.referencesFields, isDBLeft, isDBRight)
}

func (info *References) FromM2MJoinConditions(defs Definitions, m2mAlias, rightAlias string, isDBLeft, isDBRight bool) (string, error) {
	if !info.IsM2M {
		return "", ErrorPosf(compiledPos(), "object %s is not m2m relation", info.Name)
	}
	leftObject := defs.ForName(info.M2MName)
	if leftObject == nil {
		return "", ErrorPosf(compiledPos(), "object %s has unknown references object %s", info.M2MName, info.sourceName)
	}
	leftInfo := DataObjectInfo(leftObject)

	refObjectInfo := leftInfo.M2MReferencesQueryInfo(defs, info.Name)

	return refObjectInfo.JoinConditions(defs, m2mAlias, rightAlias, isDBLeft, isDBRight)
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

func (info *References) ReferencesObjectDef(defs Definitions) *ast.Definition {
	if !info.isBackRef {
		return defs.ForName(info.ReferencesName)
	}
	return defs.ForName(info.sourceName)
}
