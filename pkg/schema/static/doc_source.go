package static

import (
	"context"
	"slices"

	"github.com/vektah/gqlparser/v2/ast"
)

type docSource struct {
	doc *ast.SchemaDocument
}

// SourceFromDocument creates a mutable wrapper around *ast.SchemaDocument.
func SourceFromDocument(doc *ast.SchemaDocument) *docSource {
	return &docSource{doc: doc}
}

func (s *docSource) ForName(_ context.Context, name string) *ast.Definition {
	return s.doc.Definitions.ForName(name)
}

func (s *docSource) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	return s.doc.Directives.ForName(name)
}

func (s *docSource) Definitions(yield func(*ast.Definition) bool) {
	for _, def := range s.doc.Definitions {
		if !yield(def) {
			return
		}
	}
}

func (s *docSource) Extensions(yield func(*ast.Definition) bool) {
	for _, def := range s.doc.Extensions {
		if !yield(def) {
			return
		}
	}
}

func (s *docSource) AddDefinition(def *ast.Definition) {
	s.doc.Definitions = append(s.doc.Definitions, def)
}

func (s *docSource) RemoveDefinitions(filter func(*ast.Definition) bool) {
	s.doc.Definitions = slices.DeleteFunc(s.doc.Definitions, filter)
}

func (s *docSource) AddExtension(ext *ast.Definition) {
	s.doc.Extensions = append(s.doc.Extensions, ext)
}

func (s *docSource) ClearExtensions() {
	s.doc.Extensions = nil
}

func (s *docSource) DirectiveDefinitions(yield func(*ast.DirectiveDefinition) bool) {
	for _, dir := range s.doc.Directives {
		if !yield(dir) {
			return
		}
	}
}

func (s *docSource) AddDirectiveDefinition(dir *ast.DirectiveDefinition) {
	s.doc.Directives = append(s.doc.Directives, dir)
}

func (s *docSource) OperationType(operation string) string {
	if len(s.doc.Schema) == 0 {
		return ""
	}
	for _, op := range s.doc.Schema[0].OperationTypes {
		if string(op.Operation) == operation {
			return op.Type
		}
	}
	return ""
}

func (s *docSource) SetOperationType(operation string, typeName string) {
	if len(s.doc.Schema) == 0 {
		s.doc.Schema = append(s.doc.Schema, &ast.SchemaDefinition{})
	}
	s.doc.Schema[0].OperationTypes = append(s.doc.Schema[0].OperationTypes,
		&ast.OperationTypeDefinition{Operation: ast.Operation(operation), Type: typeName})
}

func (s *docSource) MergeFrom(other *ast.SchemaDocument) {
	s.doc.Merge(other)
}
