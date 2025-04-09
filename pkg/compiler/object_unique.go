package compiler

import (
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

func validateObjectUnique(defs Definitions, def *ast.Definition, unique *ast.Directive) error {
	if unique == nil {
		return nil
	}

	return UniqueInfo(unique).validate(defs, def)
}

type Unique struct {
	Fields      []string
	QuerySuffix string
	Skip        bool
	def         *ast.Directive
}

func UniqueInfo(def *ast.Directive) *Unique {
	if def == nil {
		return nil
	}

	if def.Name != objectUniqueDirectiveName {
		return nil
	}

	return &Unique{
		def:         def,
		Fields:      directiveArgChildValues(def, "fields"),
		QuerySuffix: directiveArgValue(def, "query_suffix"),
		Skip:        directiveArgValue(def, "skip_query") == "true",
	}
}

func (u *Unique) validate(defs Definitions, def *ast.Definition) error {

	for _, fn := range u.Fields {
		field := objectFieldByPath(defs, def.Name, fn, false, false)
		if field == nil {
			return ErrorPosf(def.Position, "field %s not found", fn)
		}
		if field.Type.NamedType == "" {
			return ErrorPosf(field.Position, "field %s must be a scalar type", fn)
		}
		if !IsScalarType(field.Type.Name()) {
			return ErrorPosf(field.Position, "field %s must be a scalar type", fn)
		}
		// check if it is JSON or AttributeValues
		if IsJSONType(field.Type.Name()) {
			return ErrorPosf(field.Position, "field %s must not be a JSON type", fn)
		}
	}

	if u.QuerySuffix == "" && !u.Skip {
		names := make([]string, len(u.Fields))
		for i, f := range u.Fields {
			names[i] = strings.ReplaceAll(f, ".", "-")
		}
		u.QuerySuffix = "unique_" + strings.Join(names, "_")
		a := u.def.Arguments.ForName("query_suffix")
		if a == nil {
			u.def.Arguments = append(u.def.Arguments, &ast.Argument{
				Name:     "query_suffix",
				Value:    &ast.Value{Raw: u.QuerySuffix, Kind: ast.StringValue},
				Position: compiledPos(),
			})
		}
		if a != nil {
			a.Value.Raw = u.QuerySuffix
		}
	}

	return nil
}
