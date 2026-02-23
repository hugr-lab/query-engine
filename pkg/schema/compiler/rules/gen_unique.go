package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*UniqueRule)(nil)

type UniqueRule struct{}

func (r *UniqueRule) Name() string     { return "UniqueRule" }
func (r *UniqueRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *UniqueRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("unique") != nil
}

func (r *UniqueRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	info := ctx.GetObject(def.Name)
	if info == nil {
		info = &base.ObjectInfo{Name: def.Name, OriginalName: def.Name}
	}
	pos := compiledPos(def.Name)

	var queryFields []*ast.FieldDefinition

	for _, dir := range def.Directives.ForNames("unique") {
		suffix := base.DirectiveArgString(dir, "query_suffix")
		if suffix == "" {
			continue
		}

		// Extract unique field names from directive args
		fieldsArg := dir.Arguments.ForName("fields")
		if fieldsArg == nil || fieldsArg.Value == nil {
			continue
		}

		var uniqueFieldNames []string
		for _, child := range fieldsArg.Value.Children {
			if child.Value != nil {
				uniqueFieldNames = append(uniqueFieldNames, child.Value.Raw)
			}
		}
		if len(uniqueFieldNames) == 0 {
			continue
		}

		// Generate query field: <defName>_by_<suffix>
		fieldName := def.Name + "_by_" + suffix
		selectOneField := &ast.FieldDefinition{
			Name: fieldName,
			Type: ast.NamedType(def.Name, pos),
			Directives: ast.DirectiveList{
				{Name: "query", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "SELECT_ONE", Kind: ast.EnumValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
			Position: pos,
		}

		// Add unique fields as required arguments
		for _, ufName := range uniqueFieldNames {
			f := def.Fields.ForName(ufName)
			if f == nil {
				continue
			}
			selectOneField.Arguments = append(selectOneField.Arguments, &ast.ArgumentDefinition{
				Name:     ufName,
				Type:     ast.NonNullNamedType(f.Type.Name(), pos),
				Position: pos,
			})
		}

		queryFields = append(queryFields, selectOneField)
	}

	if len(queryFields) > 0 {
		ctx.RegisterQueryFields(def.Name, queryFields)
	}

	return nil
}
