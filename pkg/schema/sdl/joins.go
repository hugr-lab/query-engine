package sdl

import (
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

type Join struct {
	ReferencesName   string
	sourceFields     []string
	referencesFields []string
	SQL              string
	IsQueryTime      bool

	field *ast.FieldDefinition
}

func JoinInfo(field *ast.Field) *Join {
	d := field.Directives.ForName(base.JoinDirectiveName)
	if d == nil {
		d = field.Definition.Directives.ForName(base.JoinDirectiveName)
	}
	if d == nil {
		return nil
	}
	ref := joinInfoFromDirective(d)
	if ref.ReferencesName == "" {
		ref.ReferencesName = field.Definition.Type.Name()
	}
	ref.field = field.Definition
	ref.IsQueryTime = field.Directives.ForName(base.JoinDirectiveName) != nil
	return ref
}

func JoinDefinitionInfo(def *ast.FieldDefinition) *Join {
	d := def.Directives.ForName(base.JoinDirectiveName)
	if d == nil {
		return nil
	}
	info := joinInfoFromDirective(d)
	if info == nil {
		return nil
	}
	info.field = def
	return info
}

func joinInfoFromDirective(def *ast.Directive) *Join {
	if def == nil {
		return nil
	}
	return &Join{
		ReferencesName:   directiveArgValue(def, "references_name"),
		sourceFields:     directiveArgChildValues(def, "source_fields"),
		referencesFields: directiveArgChildValues(def, "references_fields"),
		SQL:              directiveArgValue(def, "sql"),
	}
}

func (j *Join) Catalog(defs Definitions) string {
	cat := fieldDirectiveArgValue(j.field, base.CatalogDirectiveName, "name")
	if cat != "" {
		return cat
	}
	return objectDirectiveArgValue(
		defs.ForName(j.field.Type.Name()),
		base.CatalogDirectiveName, "name",
	)
}

func (j *Join) SourceFields() ([]string, error) {
	fields := append([]string{}, j.sourceFields...)
	for _, field := range ExtractFieldsFromSQL(j.SQL) {
		parts := strings.SplitN(field, ".", 2)
		if len(parts) != 2 {
			return nil, ErrorPosf(j.field.Position, "invalid field %s in SQL", field)
		}
		if base.JoinSourceFieldPrefix == parts[0] {
			fields = append(fields, parts[1])
		}
	}
	return RemoveFieldsDuplicates(fields), nil
}

func (j *Join) ReferencesFields() ([]string, error) {
	fields := append([]string{}, j.referencesFields...)
	for _, field := range ExtractFieldsFromSQL(j.SQL) {
		parts := strings.SplitN(field, ".", 2)
		if len(parts) != 2 {
			return nil, ErrorPosf(j.field.Position, "invalid field %s in SQL", field)
		}
		if base.JoinRefFieldPrefix == parts[0] {
			fields = append(fields, parts[1])
		}
	}
	return RemoveFieldsDuplicates(fields), nil
}

func (j *Join) JoinConditionsTemplate() string {
	conditions := make([]string, 0, len(j.sourceFields))
	for i, sfn := range j.sourceFields {
		conditions = append(conditions, fmt.Sprintf(
			"[%s.%s] = [%s.%s]",
			base.JoinSourceFieldPrefix, sfn,
			base.JoinRefFieldPrefix, j.referencesFields[i],
		))
	}
	if j.SQL != "" {
		conditions = append(conditions, j.SQL)
	}
	return strings.Join(conditions, " AND ")
}
