package compiler

import (
	"slices"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func validateSource(source *ast.SchemaDocument) error {
	for _, def := range source.Definitions {
		if IsSystemType(def) {
			return ErrorPosf(def.Position, "system type %s shouldn't be defined", def.Name)
		}
		switch def.Kind {
		case ast.Object:
		case ast.InputObject:
		case ast.Union:
		default:
			return ErrorPosf(def.Position, "unsupported extended definition kind %s", def.Kind)
		}
	}
	return nil
}

func validateSourceSchema(source *ast.SchemaDocument, opt *Options) error {
	errs := applyExtension(source, opt)
	// validate functions
	if def := source.Definitions.ForName(base.FunctionTypeName); def != nil {
		err := validateFunctions(source.Definitions, def, opt)
		if err != nil {
			errs = append(errs, gqlerror.WrapIfUnwrapped(err))
		}
	}
	if def := source.Definitions.ForName(base.FunctionMutationTypeName); def != nil {
		if !opt.ReadOnly {
			err := validateFunctions(source.Definitions, def, opt)
			if err != nil {
				errs = append(errs, gqlerror.WrapIfUnwrapped(err))
			}
		}
		if opt.ReadOnly {
			source.Definitions = slices.DeleteFunc(source.Definitions, func(def *ast.Definition) bool {
				return def.Name == base.FunctionMutationTypeName
			})
		}
	}
	// assign function by modules
	err := assignFunctionByModules(source)
	if err != nil {
		errs = append(errs, gqlerror.WrapIfUnwrapped(err))
	}

	for _, def := range source.Definitions {
		if IsSystemType(def) {
			continue
		}
		if def.Directives.ForName(moduleRootDirectiveName) != nil {
			continue
		}
		err := validateDefinition(source.Definitions, def, opt)
		if err != nil {
			errs = append(errs, gqlerror.WrapIfUnwrapped(err))
		}
	}
	if len(errs) != 0 {
		return &errs
	}
	return nil
}

func applyExtension(source *ast.SchemaDocument, opt *Options) (errs gqlerror.List) {
	for _, def := range source.Definitions {
		if IsDataObject(def) {
			def.Directives = append(def.Directives, opt.catalog)
		}
	}
	var err error
	for _, def := range source.Extensions {
		origin := source.Definitions.ForName(def.Name)
		if origin == nil && def.Name == base.FunctionTypeName {
			origin, err = rootType(source, ModuleFunction)
			if err != nil {
				errs = append(errs, ErrorPosf(def.Position, "extended definition %s not found", def.Name))
				continue
			}
		}
		if origin == nil && def.Name == base.FunctionMutationTypeName {
			origin, err = rootType(source, ModuleMutationFunction)
			if err != nil {
				errs = append(errs, ErrorPosf(def.Position, "extended definition %s not found", def.Name))
				continue
			}
		}
		if origin == nil {
			errs = append(errs, ErrorPosf(def.Position, "extended definition %s not found", def.Name))
			continue
		}
		if origin.Kind != def.Kind {
			errs = append(errs, ErrorPosf(def.Position, "extended definition %s kind mismatch", def.Name))
			continue
		}
		if len(def.Directives) != 0 {
			errs = append(errs, ErrorPosf(def.Position, "extended definition %s shouldn't have any directive", def.Name))
			continue
		}
		for _, field := range def.Fields {
			originField := origin.Fields.ForName(field.Name)
			if originField == nil {
				origin.Fields = append(origin.Fields, field)
				continue
			}
			originField.Type = field.Type
			originField.Directives = append(originField.Directives, field.Directives...)
			originField.Position = field.Position
			originField.Arguments = field.Arguments
			originField.DefaultValue = field.DefaultValue
		}
	}
	source.Extensions = nil
	return errs
}
