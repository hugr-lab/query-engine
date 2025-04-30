package compiler

import (
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

func addDefinitionPrefix(defs Definitions, def *ast.Definition, opt *Options, addOriginal bool) {
	switch def.Kind {
	case ast.Object:
		addObjectPrefix(defs, def, opt, addOriginal)
	case ast.InputObject:
		addInputPrefix(defs, def, opt, addOriginal)
	case ast.Interface:
		addInterfacePrefix(defs, def, opt, addOriginal)
	case ast.Union:
		addUnionPrefix(def, opt, addOriginal)
	// TODO: add other types
	default:
		def.Name = opt.Prefix + "_" + def.Name
	}
}

func validateDefinition(defs Definitions, def *ast.Definition, opt *Options) error {
	switch def.Kind {
	case ast.Object:
		return validateObject(defs, def, opt)
	case ast.InputObject:
		return validateInputObject(def)
	default:
		return nil
	}
}

type Definitions interface {
	ForName(name string) *ast.Definition
}

type DefinitionsSource interface {
	Definitions
	DirectiveForName(name string) *ast.DirectiveDefinition
}

type schemaDefs struct {
	*ast.Schema
}

func (d schemaDefs) ForName(name string) *ast.Definition {
	return d.Types[name]
}

func (d schemaDefs) DirectiveForName(name string) *ast.DirectiveDefinition {
	return d.Directives[name]
}

func SchemaDefs(schema *ast.Schema) DefinitionsSource {
	return schemaDefs{schema}
}

func addInterfacePrefix(defs Definitions, def *ast.Definition, opt *Options, addOriginal bool) {
	if def.Kind != ast.Interface {
		return
	}
	if addOriginal {
		def.Directives = append(def.Directives, base.OriginalNameDirective(def.Name))
	}
	def.Name = opt.Prefix + "_" + def.Name

	for _, field := range def.Fields {
		typeName := field.Type.Name()
		if IsScalarType(typeName) {
			continue
		}
		field.Type = typeWithPrefix(defs, field.Type, opt.Prefix+"_")
	}
}

func addUnionPrefix(def *ast.Definition, opt *Options, addOriginal bool) {
	if def.Kind != ast.Union {
		return
	}
	if addOriginal {
		def.Directives = append(def.Directives, base.OriginalNameDirective(def.Name))
	}
	def.Name = opt.Prefix + "_" + def.Name

	types := make([]string, len(def.Types))
	for i, typeName := range def.Types {
		types[i] = opt.Prefix + "_" + typeName
	}
	def.Types = types
}

func copyDefinitions(doc *ast.SchemaDocument, def *ast.Definition) *ast.Definition {
	new := &ast.Definition{
		Kind:                     def.Kind,
		Description:              def.Description,
		Name:                     def.Name,
		Interfaces:               append([]string{}, def.Interfaces...),
		Types:                    append([]string{}, def.Types...),
		Position:                 copyPosition(def.Position),
		BuiltIn:                  def.BuiltIn,
		BeforeDescriptionComment: copyComment(def.BeforeDescriptionComment),
		AfterDescriptionComment:  copyComment(def.AfterDescriptionComment),
		EndOfDefinitionComment:   copyComment(def.EndOfDefinitionComment),
	}

	new.Directives = copyDirectiveList(doc, new, def.Directives)
	new.EnumValues = copyEnumValueList(doc, new, def.EnumValues)
	new.Fields = copyFieldList(doc, new, def.Fields)

	return new
}

func copyFieldList(doc *ast.SchemaDocument, parent *ast.Definition, fl ast.FieldList) ast.FieldList {
	var new ast.FieldList
	for _, f := range fl {
		new = append(new, copyFieldDefinition(doc, parent, f))
	}
	return new
}

func copyFieldDefinition(doc *ast.SchemaDocument, parent *ast.Definition, def *ast.FieldDefinition) *ast.FieldDefinition {
	new := &ast.FieldDefinition{
		Description:              def.Description,
		Name:                     def.Name,
		Arguments:                copyArgumentDefinitionList(doc, parent, def.Arguments),
		DefaultValue:             copyValue(def.DefaultValue),
		Type:                     copyType(def.Type),
		Position:                 copyPosition(def.Position),
		BeforeDescriptionComment: copyComment(def.BeforeDescriptionComment),
		AfterDescriptionComment:  copyComment(def.AfterDescriptionComment),
	}

	new.Directives = copyDirectiveList(doc, parent, def.Directives)
	return new
}

func copyArgumentDefinitionList(doc *ast.SchemaDocument, parent *ast.Definition, al ast.ArgumentDefinitionList) ast.ArgumentDefinitionList {
	var new ast.ArgumentDefinitionList
	for _, a := range al {
		new = append(new, copyArgumentDefinition(doc, parent, a))
	}
	return new
}

func copyArgumentDefinition(doc *ast.SchemaDocument, parent *ast.Definition, def *ast.ArgumentDefinition) *ast.ArgumentDefinition {
	new := &ast.ArgumentDefinition{
		Description:              def.Description,
		Name:                     def.Name,
		DefaultValue:             copyValue(def.DefaultValue),
		Type:                     copyType(def.Type),
		Directives:               copyDirectiveList(doc, parent, def.Directives),
		Position:                 copyPosition(def.Position),
		BeforeDescriptionComment: copyComment(def.BeforeDescriptionComment),
		AfterDescriptionComment:  copyComment(def.AfterDescriptionComment),
	}

	return new
}

func copyEnumValueList(doc *ast.SchemaDocument, parent *ast.Definition, el []*ast.EnumValueDefinition) []*ast.EnumValueDefinition {
	var new ast.EnumValueList
	for _, e := range el {
		new = append(new, copyEnumValueDefinition(doc, parent, e))
	}
	return new
}

func copyEnumValueDefinition(doc *ast.SchemaDocument, parent *ast.Definition, def *ast.EnumValueDefinition) *ast.EnumValueDefinition {
	new := &ast.EnumValueDefinition{
		Name:                     def.Name,
		Description:              def.Description,
		Position:                 copyPosition(def.Position),
		BeforeDescriptionComment: copyComment(def.BeforeDescriptionComment),
		AfterDescriptionComment:  copyComment(def.AfterDescriptionComment),
	}

	new.Directives = copyDirectiveList(doc, parent, def.Directives)

	return new
}

func copyDirectiveList(doc *ast.SchemaDocument, parent *ast.Definition, dl ast.DirectiveList) ast.DirectiveList {
	var new ast.DirectiveList
	for _, d := range dl {
		new = append(new, copyDirective(doc, parent, d))
	}
	return new
}

func copyDirective(doc *ast.SchemaDocument, parent *ast.Definition, def *ast.Directive) *ast.Directive {
	new := &ast.Directive{
		Name:      def.Name,
		Arguments: copyArgumentList(def.Arguments),
		Position:  copyPosition(def.Position),

		ParentDefinition: parent,
		Definition:       doc.Directives.ForName(def.Name),
		Location:         def.Location,
	}

	return new
}

func copyArgumentList(al ast.ArgumentList) ast.ArgumentList {
	new := ast.ArgumentList{}

	for _, arg := range al {
		new = append(new, copyArgument(arg))
	}
	return new
}

func copyArgument(arg *ast.Argument) *ast.Argument {
	new := &ast.Argument{
		Name:     arg.Name,
		Value:    copyValue(arg.Value),
		Position: copyPosition(arg.Position),
		Comment:  copyComment(arg.Comment),
	}

	return new
}

func copyValue(val *ast.Value) *ast.Value {
	if val == nil {
		return nil
	}
	new := &ast.Value{
		Raw:                val.Raw,
		Kind:               val.Kind,
		Children:           copyChildValues(val.Children),
		Position:           copyPosition(val.Position),
		Comment:            copyComment(val.Comment),
		Definition:         &ast.Definition{},
		VariableDefinition: &ast.VariableDefinition{},
		ExpectedType:       copyType(val.ExpectedType),
	}

	return new
}

func copyChildValues(cvs []*ast.ChildValue) []*ast.ChildValue {
	var new []*ast.ChildValue
	for _, cv := range cvs {
		new = append(new, copyChildValue(cv))
	}
	return new
}

func copyChildValue(cv *ast.ChildValue) *ast.ChildValue {
	new := &ast.ChildValue{
		Name:     cv.Name,
		Value:    copyValue(cv.Value),
		Position: copyPosition(cv.Position),
	}

	return new
}

func copyType(t *ast.Type) *ast.Type {
	if t == nil {
		return nil
	}
	new := &ast.Type{
		NamedType: t.NamedType,
		NonNull:   t.NonNull,
		Elem:      copyType(t.Elem),
		Position:  copyPosition(t.Position),
	}

	return new
}

func copyComment(comment *ast.CommentGroup) *ast.CommentGroup {
	if comment == nil {
		return nil
	}
	new := &ast.CommentGroup{}

	for _, c := range comment.List {
		new.List = append(new.List, &ast.Comment{
			Value:    c.Value,
			Position: copyPosition(c.Position),
		})
	}
	return new
}

func copyPosition(pos *ast.Position) *ast.Position {
	if pos == nil {
		return nil
	}
	return &ast.Position{
		Line:   pos.Line,
		Column: pos.Column,
		Start:  pos.Start,
		End:    pos.End,
		Src: &ast.Source{
			Name:    pos.Src.Name,
			Input:   pos.Src.Input,
			BuiltIn: pos.Src.BuiltIn,
		},
	}
}
