package rules

import (
	"strconv"

	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ValuesOfCorrectType checks that literal values are compatible with their expected type.
type ValuesOfCorrectType struct{}

func (r *ValuesOfCorrectType) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	var checkValue func(value *ast.Value)
	checkValue = func(value *ast.Value) {
		if value == nil || value.Definition == nil || value.ExpectedType == nil {
			return
		}
		if value.Kind == ast.NullValue {
			if value.ExpectedType.NonNull {
				errs = append(errs, gqlerror.ErrorPosf(value.Position,
					`Expected value of type "%s", found %s.`, value.ExpectedType.String(), value.String()))
			}
			return
		}
		if value.Kind == ast.Variable {
			return
		}
		if value.Definition.Kind == ast.Scalar {
			if !value.Definition.OneOf("Int", "Float", "String", "Boolean", "ID") {
				return // Custom scalar — skip validation
			}
		}

		switch value.Kind {
		case ast.ListValue:
			if value.ExpectedType.Elem == nil {
				errs = append(errs, gqlerror.ErrorPosf(value.Position,
					`Expected value of type "%s", found %s.`, value.ExpectedType.String(), value.String()))
			}
		case ast.IntValue:
			if !value.Definition.OneOf("Int", "Float", "ID") {
				errs = append(errs, valuesTypeError(value))
			}
		case ast.FloatValue:
			if !value.Definition.OneOf("Float") {
				errs = append(errs, valuesTypeError(value))
			}
		case ast.StringValue, ast.BlockValue:
			if value.Definition.Kind == ast.Enum {
				errs = append(errs, gqlerror.ErrorPosf(value.Position,
					`Enum "%s" cannot represent non-enum value: %s.`, value.ExpectedType.String(), value.String()))
			} else if !value.Definition.OneOf("String", "ID") {
				errs = append(errs, valuesTypeError(value))
			}
		case ast.EnumValue:
			if value.Definition.Kind != ast.Enum {
				errs = append(errs, valuesTypeError(value))
			} else if value.Definition.EnumValues.ForName(value.Raw) == nil {
				errs = append(errs, gqlerror.ErrorPosf(value.Position,
					`Value "%s" does not exist in "%s" enum.`, value.String(), value.ExpectedType.String()))
			}
		case ast.BooleanValue:
			if !value.Definition.OneOf("Boolean") {
				errs = append(errs, valuesTypeError(value))
			}
		case ast.ObjectValue:
			for _, field := range value.Definition.Fields {
				if field.Type.NonNull {
					fieldValue := value.Children.ForName(field.Name)
					if fieldValue == nil && field.DefaultValue == nil {
						errs = append(errs, gqlerror.ErrorPosf(value.Position,
							`Field "%s.%s" of required type "%s" was not provided.`,
							value.Definition.Name, field.Name, field.Type.String()))
					}
				}
			}
			for _, child := range value.Children {
				if value.Definition.Fields.ForName(child.Name) == nil {
					errs = append(errs, gqlerror.ErrorPosf(child.Position,
						`Field "%s" is not defined by type "%s".`, child.Name, value.Definition.Name))
				}
			}
			// Check @oneOf
			for _, directive := range value.Definition.Directives {
				if directive.Name == "oneOf" {
					if len(value.Children) != 1 {
						errs = append(errs, gqlerror.ErrorPosf(value.Position,
							`OneOf Input Object "%s" must specify exactly one key.`, value.Definition.Name))
					} else {
						fieldValue := value.Children[0].Value
						if fieldValue == nil || fieldValue.Kind == ast.NullValue {
							errs = append(errs, gqlerror.ErrorPosf(value.Position,
								`Field "%s.%s" must be non-null.`, value.Definition.Name, value.Children[0].Name))
						} else if fieldValue.Kind == ast.Variable && fieldValue.VariableDefinition != nil && !fieldValue.VariableDefinition.Type.NonNull {
							errs = append(errs, gqlerror.ErrorPosf(fieldValue.Position,
								`Variable "%s" must be non-nullable to be used for OneOf Input Object "%s".`,
								fieldValue.VariableDefinition.Variable, value.Definition.Name))
						}
					}
				}
			}
		}
	}
	WalkAllValues(doc, checkValue)
	return errs
}

func valuesTypeError(v *ast.Value) *gqlerror.Error {
	switch v.ExpectedType.String() {
	case "Int", "Int!":
		if _, err := strconv.ParseInt(v.Raw, 10, 32); err != nil {
			return gqlerror.ErrorPosf(v.Position, `Int cannot represent non 32-bit signed integer value: %s`, v.String())
		}
		return gqlerror.ErrorPosf(v.Position, `Int cannot represent non-integer value: %s`, v.String())
	case "String", "String!", "[String]":
		return gqlerror.ErrorPosf(v.Position, `String cannot represent a non string value: %s`, v.String())
	case "Boolean", "Boolean!":
		return gqlerror.ErrorPosf(v.Position, `Boolean cannot represent a non boolean value: %s`, v.String())
	case "Float", "Float!":
		return gqlerror.ErrorPosf(v.Position, `Float cannot represent non numeric value: %s`, v.String())
	case "ID", "ID!":
		return gqlerror.ErrorPosf(v.Position, `ID cannot represent a non-string and non-integer value: %s`, v.String())
	default:
		if v.Definition.Kind == ast.Enum {
			return gqlerror.ErrorPosf(v.Position, `Enum "%s" cannot represent non-enum value: %s.`, v.ExpectedType.String(), v.String())
		}
		return gqlerror.ErrorPosf(v.Position, `Expected value of type "%s", found %s.`, v.ExpectedType.String(), v.String())
	}
}
