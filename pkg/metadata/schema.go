package metadata

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	schemaQuery = "__schema"
)

var (
	ErrInvalidSchema = errors.New("invalid schema")
	ErrTypeNotFound  = errors.New("type not found")
)

func processSchemaQuery(ctx context.Context, schema *ast.Schema, field *ast.Field, maxDepth int) (map[string]any, error) {
	return processSelectionSet(ctx, field.SelectionSet, map[string]fieldResolverFunc{
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return schema.Description, nil
		},
		"queryType": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			data, err := typeResolver(ctx, schema, ast.NamedType(schema.Query.Name, schema.Query.Position), field.SelectionSet, maxDepth)
			if err != nil {
				return nil, err
			}
			return data, nil
		},
		"mutationType": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if schema.Mutation == nil {
				return nil, nil
			}
			data, err := typeResolver(ctx, schema, ast.NamedType(schema.Mutation.Name, schema.Query.Position), field.SelectionSet, maxDepth)
			if errors.Is(err, ErrTypeNotFound) {
				return nil, nil
			}
			return data, err
		},
		"subscriptionType": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if schema.Subscription == nil {
				return nil, nil
			}
			data, err := typeResolver(ctx, schema, ast.NamedType(schema.Subscription.Name, schema.Query.Position), field.SelectionSet, maxDepth)
			if errors.Is(err, ErrTypeNotFound) {
				return nil, nil
			}
			return data, err
		},
		"types": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			var res []map[string]any
			for _, t := range schema.Types {

				data, err := typeResolver(ctx, schema, ast.NamedType(t.Name, &ast.Position{}), field.SelectionSet, maxDepth)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"directives": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			var res []map[string]any
			for _, dn := range base.QuerySideDirectives() {
				d, ok := schema.Directives[dn]
				if !ok {
					continue
				}
				data, err := directiveResolver(ctx, schema, d, field.SelectionSet, maxDepth)
				if err != nil {
					return nil, nil
				}
				res = append(res, data)
			}
			return res, nil
		},
		"__typename": typeNameResolver,
	}, schemaQuery)
}

func typeNameResolver(ctx context.Context, field *ast.Field, onType string) (any, error) {
	if onType != "" {
		return onType, nil
	}
	return field.ObjectDefinition.Name, nil
}

func typeResolver(ctx context.Context, schema *ast.Schema, typeDef *ast.Type, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	def, ok := schema.Types[typeDef.Name()]
	if !ok {
		return nil, ErrTypeNotFound
	}

	if maxDepth <= 0 {
		return nil, errors.New("max depth exceeded")
	}

	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"kind": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NonNull {
				return "NON_NULL", nil
			}
			if typeDef.NamedType == "" {
				return "LIST", nil
			}
			return def.Kind, nil
		},
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			return def.Name, nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			return def.Description, nil
		},
		"fields": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			if def.Kind != ast.Object && def.Kind != ast.Interface {
				return nil, nil
			}
			includeDeprecated := false
			if a := field.Arguments.ForName("includeDeprecated"); a != nil {
				includeDeprecated = a.Value.Raw == "true"
			}
			res := []map[string]any{}
			for _, f := range def.Fields {
				if strings.HasPrefix(f.Name, "__") {
					continue
				}
				di := compiler.FieldDeprecatedInfo(f)
				if !includeDeprecated && di.IsDeprecated {
					continue
				}
				if p := perm.PermissionsFromCtx(ctx); p != nil {
					if _, ok := p.Visible(def.Name, f.Name); !ok {
						continue
					}
				}
				data, err := fieldResolver(ctx, schema, f, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"interfaces": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			if def.Kind != ast.Interface && def.Kind != ast.Object {
				return nil, nil
			}
			res := []map[string]any{}
			for _, i := range def.Interfaces {
				data, err := typeResolver(ctx, schema, ast.NamedType(i, &ast.Position{}), field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"possibleTypes": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			if def.Kind != ast.Interface && def.Kind != ast.Union {
				return nil, nil
			}
			res := []map[string]any{}
			for _, t := range def.Types {
				data, err := typeResolver(ctx, schema, ast.NamedType(t, &ast.Position{}), field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"enumValues": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			if def.Kind != ast.Enum {
				return nil, nil
			}
			includeDeprecated := false
			if a := field.Arguments.ForName("includeDeprecated"); a != nil {
				includeDeprecated = a.Value.Raw == "true"
			}
			var res []map[string]any
			for _, ev := range def.EnumValues {
				di := compiler.EnumDeprecatedInfo(ev)
				if !includeDeprecated && di.IsDeprecated {
					continue
				}
				data, err := enumValueResolver(ctx, ev, field.SelectionSet)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"inputFields": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			if def.Kind != ast.InputObject {
				return nil, nil
			}
			res := []map[string]any{}
			for _, f := range def.Fields {
				data, err := inputValueResolver(ctx, schema, f, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"ofType": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NonNull {
				return typeResolver(ctx, schema, &ast.Type{
					NamedType: typeDef.NamedType,
					Elem:      typeDef.Elem,
					NonNull:   false,
					Position:  typeDef.Position,
				}, field.SelectionSet, maxDepth-1)
			}
			if typeDef.NamedType == "" {
				return typeResolver(ctx, schema, typeDef.Elem, field.SelectionSet, maxDepth-1)
			}
			return nil, nil
		},
		"specifiedByURL": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if typeDef.NamedType == "" || typeDef.NonNull {
				return nil, nil
			}
			if def.Kind != ast.Scalar {
				return nil, nil
			}
			url := compiler.SpecifiedByURL(def)
			if url == "" {
				return nil, nil
			}
			return url, nil
		},
		"hugr_type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if mi := compiler.ModuleRootInfo(def); mi != nil {
				return base.HugrTypeModule, nil
			}
			switch compiler.DataObjectType(def) {
			case compiler.Table:
				return base.HugrTypeTable, nil
			case compiler.View:
				return base.HugrTypeView, nil
			}
			switch {
			case def.Kind == ast.InputObject && def.Directives.ForName(compiler.FilterInputDirectiveName) != nil:
				return base.HugrTypeFilter, nil
			case def.Kind == ast.InputObject && def.Directives.ForName(compiler.FilterListInputDirectiveName) != nil:
				return base.HugrTypeFilterList, nil
			case def.Kind == ast.InputObject && def.Directives.ForName(compiler.DataInputDirectiveName) != nil:
				return base.HugrTypeDataInput, nil
			case def.Kind == ast.Object && def.Name == base.QueryTimeJoinsTypeName:
				return base.HugrTypeJoin, nil
			case def.Kind == ast.Object && def.Name == base.QueryTimeSpatialTypeName:
				return base.HugrTypeSpatial, nil
			case def.Kind == ast.Object && def.Name == base.H3QueryTypeName:
				return base.HugrTypeH3Agg, nil
			case def.Kind == ast.Object && def.Name == base.H3DataQueryTypeName:
				return base.HugrTypeH3Data, nil
			}
			return "", nil
		},
		"module": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if mi := compiler.ModuleRootInfo(def); mi != nil {
				return mi.Name, nil
			}
			if compiler.IsDataObject(def) {
				return compiler.ObjectModule(def), nil
			}
			return "", nil
		},
		"catalog": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if compiler.IsDataObject(def) {
				info := compiler.DataObjectInfo(def)
				if info != nil {
					return info.Catalog, nil
				}
				return "", nil
			}
			return "", nil
		},
	}, "__Type")
}

func fieldResolver(ctx context.Context, schema *ast.Schema, def *ast.FieldDefinition, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	di := compiler.FieldDeprecatedInfo(def)
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Name, nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Description, nil
		},
		"args": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			res := []map[string]any{}
			for _, a := range def.Arguments {
				data, err := argumentResolver(ctx, schema, a, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if maxDepth <= 2 {
				return typeResolver(ctx, schema, ast.NamedType(compiler.JSONTypeName, &ast.Position{}), field.SelectionSet, maxDepth-1)
			}
			return typeResolver(ctx, schema, def.Type, field.SelectionSet, maxDepth-1)
		},
		"isDeprecated": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return di.IsDeprecated, nil
		},
		"deprecationReason": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if di.Reason == "" {
				return nil, nil
			}
			return di.Reason, nil
		},
		"hugr_type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			td := schema.Types[def.Type.Name()]
			if td == nil {
				return "", nil
			}
			if mi := compiler.ModuleRootInfo(td); mi != nil && def.Type.NamedType != "" {
				return base.HugrTypeFieldSubmodule, nil
			}
			switch {
			case compiler.IsAggregateQueryDefinition(def):
				return base.HugrTypeFieldAgg, nil
			case compiler.IsSelectOneQueryDefinition(def):
				return base.HugrTypeFieldSelectOne, nil
			case compiler.IsSelectQueryDefinition(def):
				return base.HugrTypeFieldSelect, nil
			case compiler.IsAggregateQueryDefinition(def):
				return base.HugrTypeFieldAgg, nil
			case compiler.IsBucketAggregateQueryDefinition(def):
				return base.HugrTypeFieldBucketAgg, nil
			case compiler.IsFunctionCall(def):
				return base.HugrTypeFieldFunction, nil
			case compiler.IsFunction(def):
				return base.HugrTypeFieldFunction, nil
			case compiler.IsJoinSubqueryDefinition(def):
				return base.HugrTypeFieldSelect, nil
			case compiler.IsReferencesSubquery(def):
				return base.HugrTypeFieldSelect, nil
			case compiler.IsInsertQueryDefinition(def):
				return base.HugrTypeFieldMutationInsert, nil
			case compiler.IsUpdateQueryDefinition(def):
				return base.HugrTypeFieldMutationUpdate, nil
			case compiler.IsDeleteQueryDefinition(def):
				return base.HugrTypeFieldMutationDelete, nil
			}
			if def.Name == base.QueryTimeJoinsFieldName && td.Name == base.QueryBaseName {
				return base.HugrTypeFieldJoin, nil
			}
			if def.Name == base.QueryTimeSpatialFieldName && td.Name == base.QueryBaseName {
				return base.HugrTypeFieldSpatial, nil
			}
			if def.Name == compiler.JQTransformQueryName && td.Name == base.QueryBaseName {
				return base.HugrTypeFieldJQ, nil
			}
			if def.Name == base.H3QueryFieldName && td.Name == base.H3QueryTypeName {
				return base.HugrTypeFieldH3Agg, nil
			}
			return "", nil
		},
		"catalog": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if compiler.IsFunction(def) {
				info, err := compiler.FunctionInfo(def)
				if err != nil {
					return nil, err
				}
				return info.Catalog, nil
			}
			return "", nil
		},
		"mcp_exclude": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Directives.ForName(base.FieldExcludeMCPDirectiveName) != nil, nil
		},
	}, "__Field")
}

func enumValueResolver(ctx context.Context, def *ast.EnumValueDefinition, ss ast.SelectionSet) (map[string]any, error) {
	di := compiler.EnumDeprecatedInfo(def)
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Name, nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Description, nil
		},
		"isDeprecated": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return di.IsDeprecated, nil
		},
		"deprecationReason": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if di.Reason == "" {
				return nil, nil
			}
			return di.Reason, nil
		},
	}, "__EnumValue")
}

func argumentResolver(ctx context.Context, schema *ast.Schema, def *ast.ArgumentDefinition, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Name, nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Description, nil
		},
		"type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if maxDepth <= 2 {
				return typeResolver(ctx, schema, ast.NamedType(compiler.JSONTypeName, &ast.Position{}), field.SelectionSet, maxDepth-1)
			}
			return typeResolver(ctx, schema, def.Type, field.SelectionSet, maxDepth-1)
		},
		"defaultValue": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if def.DefaultValue == nil {
				return nil, nil
			}
			if def.DefaultValue.Kind == ast.StringValue {
				if def.DefaultValue.Raw == "" {
					return strconv.Quote("\"\""), nil
				}
				return strconv.Quote(def.DefaultValue.Raw), nil
			}
			return def.DefaultValue.Raw, nil
		},
		"hugr_type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return "test", nil
		},
	}, "__InputValue")
}

func inputValueResolver(ctx context.Context, schema *ast.Schema, def *ast.FieldDefinition, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Name, nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Description, nil
		},
		"type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if maxDepth <= 2 {
				return typeResolver(ctx, schema, ast.NamedType(compiler.JSONTypeName, &ast.Position{}), field.SelectionSet, maxDepth-1)
			}
			return typeResolver(ctx, schema, def.Type, field.SelectionSet, maxDepth-1)
		},
		"defaultValue": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if def.DefaultValue == nil {
				return nil, nil
			}
			return def.DefaultValue.Raw, nil
		},
		"hugr_type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return "test", nil
		},
	}, "__InputValue")
}

func directiveResolver(ctx context.Context, schema *ast.Schema, def *ast.DirectiveDefinition, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Name, nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Description, nil
		},
		"locations": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Locations, nil
		},
		"args": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			res := []map[string]any{}
			for _, a := range def.Arguments {
				data, err := argumentResolver(ctx, schema, a, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"isRepeatable": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.IsRepeatable, nil
		},
	}, "__Directive")
}
