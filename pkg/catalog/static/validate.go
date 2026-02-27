package static

import (
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// ValidateSchema checks that all type references in the schema are resolvable.
// Returns a list of validation errors. An empty slice means the schema is valid.
// This is intended for use in tests, not in production runtime.
func (p *Provider) ValidateSchema() []error {
	var errs []error
	for name, def := range p.schema.Types {
		if isBuiltinType(name) {
			continue
		}
		for _, f := range def.Fields {
			typeName := f.Type.Name()
			if !isKnownType(p.schema, typeName) {
				errs = append(errs, fmt.Errorf(
					"type %s: field %s references unknown type %s", name, f.Name, typeName))
			}
			for _, arg := range f.Arguments {
				argTypeName := arg.Type.Name()
				if !isKnownType(p.schema, argTypeName) {
					errs = append(errs, fmt.Errorf(
						"type %s: field %s: argument %s references unknown type %s",
						name, f.Name, arg.Name, argTypeName))
				}
			}
		}
		for _, iface := range def.Interfaces {
			if _, ok := p.schema.Types[iface]; !ok {
				errs = append(errs, fmt.Errorf(
					"type %s: implements unknown interface %s", name, iface))
			}
		}
		for _, member := range def.Types {
			if _, ok := p.schema.Types[member]; !ok {
				errs = append(errs, fmt.Errorf(
					"type %s: union member %s is unknown", name, member))
			}
		}
	}
	return errs
}

// isKnownType checks if a type name is resolvable — either in the schema or as a registered scalar.
func isKnownType(schema *ast.Schema, name string) bool {
	if _, ok := schema.Types[name]; ok {
		return true
	}
	return types.IsScalar(name)
}

// isBuiltinType returns true for GraphQL spec built-in types that don't need validation.
func isBuiltinType(name string) bool {
	switch name {
	case "__Schema", "__Type", "__Field", "__InputValue", "__EnumValue", "__Directive",
		"__DirectiveLocation", "__TypeKind",
		"String", "Int", "Float", "Boolean", "ID":
		return true
	}
	return false
}
