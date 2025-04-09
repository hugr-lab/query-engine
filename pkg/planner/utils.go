package planner

import (
	"github.com/vektah/gqlparser/v2/ast"
)

func filterFields(selection ast.SelectionSet, filter func(field *ast.Field) bool, recursively bool) fieldList {
	var fields []*ast.Field
	for _, s := range selection {
		switch v := s.(type) {
		case *ast.Field:
			if filter(v) {
				fields = append(fields, v)
			}
			if recursively {
				fields = append(fields, filterFields(v.SelectionSet, filter, recursively)...)
			}
		}
	}
	return fields
}

type fieldList []*ast.Field

func (f fieldList) ForAlias(alias string) *ast.Field {
	for _, field := range f {
		if field.Alias == alias {
			return field
		}
	}
	return nil
}

func (f fieldList) ForName(name string) *ast.Field {
	for _, field := range f {
		if field.Name == name {
			return field
		}
	}
	return nil
}

func (f fieldList) AsSelectionSet() ast.SelectionSet {
	var ss ast.SelectionSet
	for _, field := range f {
		ss = append(ss, field)
	}
	return ss
}
