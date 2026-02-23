package base

import "github.com/vektah/gqlparser/v2/ast"

func CloneDefinition(def *ast.Definition, fieldFilter func(*ast.FieldDefinition) bool) *ast.Definition {
	if def == nil {
		return nil
	}
	dd := &ast.Definition{
		Kind:        def.Kind,
		Name:        def.Name,
		Description: def.Description,
		Directives:  CloneDirectiveList(def.Directives),
		Fields:      CloneFieldDefinitionListWithFilter(def.Fields, fieldFilter),
		EnumValues:  CloneEnumValueList(def.EnumValues),
		Position:    def.Position,
		BuiltIn:     def.BuiltIn,

		BeforeDescriptionComment: def.BeforeDescriptionComment,
		AfterDescriptionComment:  def.AfterDescriptionComment,
		EndOfDefinitionComment:   def.EndOfDefinitionComment,
	}

	if def.Types != nil {
		dd.Types = make([]string, len(def.Types))
		copy(dd.Types, def.Types)
	}

	if def.Interfaces != nil {
		dd.Interfaces = make([]string, len(def.Interfaces))
		copy(dd.Interfaces, def.Interfaces)
	}

	return dd
}

func CloneFieldDefinitionListWithFilter(fields ast.FieldList, fieldFilter func(*ast.FieldDefinition) bool) ast.FieldList {
	if fields == nil {
		return nil
	}
	cloned := make(ast.FieldList, 0, len(fields))
	for _, f := range fields {
		if fieldFilter == nil || fieldFilter(f) {
			cloned = append(cloned, CloneFieldDefinition(f))
		}
	}
	return cloned
}

func CloneFieldDefinition(field *ast.FieldDefinition) *ast.FieldDefinition {
	if field == nil {
		return nil
	}
	return &ast.FieldDefinition{
		Name:        field.Name,
		Description: field.Description,
		Type:        field.Type,
		Position:    field.Position,
		Directives:  CloneDirectiveList(field.Directives),
	}
}

func CloneDirectiveList(dirs ast.DirectiveList) ast.DirectiveList {
	if dirs == nil {
		return nil
	}
	cloned := make(ast.DirectiveList, len(dirs))
	for i, d := range dirs {
		cloned[i] = CloneDirective(d)
	}
	return cloned
}

func CloneDirective(dir *ast.Directive) *ast.Directive {
	if dir == nil {
		return nil
	}
	return &ast.Directive{
		Name:      dir.Name,
		Arguments: CloneArgumentList(dir.Arguments),
		Position:  dir.Position,
	}
}

func CloneArgumentList(args ast.ArgumentList) ast.ArgumentList {
	if args == nil {
		return nil
	}
	cloned := make(ast.ArgumentList, len(args))
	for i, a := range args {
		cloned[i] = CloneArgument(a)
	}
	return cloned
}

func CloneArgument(arg *ast.Argument) *ast.Argument {
	if arg == nil {
		return nil
	}
	return &ast.Argument{
		Name:     arg.Name,
		Value:    CloneValue(arg.Value),
		Position: arg.Position,
		Comment:  arg.Comment,
	}
}

func CloneValue(value *ast.Value) *ast.Value {
	if value == nil {
		return nil
	}
	vv := &ast.Value{
		Kind:     value.Kind,
		Raw:      value.Raw,
		Position: value.Position,
		Comment:  value.Comment,
	}
	if value.Children != nil {
		vv.Children = make(ast.ChildValueList, len(value.Children))
		for i, c := range value.Children {
			vv.Children[i] = &ast.ChildValue{
				Name:     c.Name,
				Value:    CloneValue(c.Value),
				Position: c.Position,
				Comment:  c.Comment,
			}
		}
	}
	return vv
}

func CloneEnumValueList(enumValues ast.EnumValueList) ast.EnumValueList {
	if enumValues == nil {
		return nil
	}
	cloned := make(ast.EnumValueList, len(enumValues))
	for i, ev := range enumValues {
		cloned[i] = &ast.EnumValueDefinition{
			Name:        ev.Name,
			Description: ev.Description,
			Position:    ev.Position,
			Directives:  CloneDirectiveList(ev.Directives),
		}
	}
	return cloned
}
