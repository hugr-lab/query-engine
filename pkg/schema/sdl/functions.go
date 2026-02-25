package sdl

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

type Function struct {
	Module       string
	Catalog      string
	Name         string
	ReturnsTable bool
	JsonCast     bool
	SkipNullArg  bool

	sql   string
	field *ast.FieldDefinition
}

func FunctionInfo(field *ast.FieldDefinition) (*Function, error) {
	d := field.Directives.ForName(base.FunctionDirectiveName)
	if d == nil {
		return nil, ErrorPosf(field.Position, "field %s should have a directive @%s", field.Name, base.FunctionDirectiveName)
	}
	catalog := fieldDirectiveArgValue(field, base.CatalogDirectiveName, "name")
	isTable := fieldDirectiveArgValue(field, base.FunctionDirectiveName, "is_table") == "true" ||
		!IsScalarType(field.Type.Name()) && field.Type.NamedType == ""

	return &Function{
		Module:       fieldDirectiveArgValue(field, base.ModuleDirectiveName, "name"),
		Catalog:      catalog,
		Name:         directiveArgValue(d, "name"),
		SkipNullArg:  directiveArgValue(d, "skip_null_arg") == "true" && len(field.Arguments) == 1,
		JsonCast:     !isTable && fieldDirectiveArgValue(field, base.FunctionDirectiveName, "json_cast") == "true",
		ReturnsTable: isTable,
		sql:          directiveArgValue(d, "sql"),
		field:        field,
	}, nil
}

func (f *Function) Definition() *ast.FieldDefinition {
	return f.field
}

func (f *Function) SQL() string {
	sql := f.sql
	if sql != "" {
		sql = strings.ReplaceAll(sql, "["+base.CatalogSystemVariableName+"]", "'"+f.Catalog+"'")
	}
	if sql == "" {
		d := f.field.Directives.ForName(base.FunctionDirectiveName)
		name := directiveArgValue(d, "name")
		sql = name + "("
		for i, arg := range f.field.Arguments {
			if i > 0 {
				sql += ", "
			}
			sql += "[" + arg.Name + "]"
		}
		sql += ")"
	}
	return sql
}

func (f *Function) ArgumentByName(name string) *ast.ArgumentDefinition {
	return f.field.Arguments.ForName(name)
}

func (f *Function) ResultAggregationType(ctx context.Context, defs base.DefinitionsSource) string {
	if f == nil {
		return ""
	}
	name := buildObjectAggregationTypeName(f.field.Type.Name(), false, false)
	if defs.ForName(ctx, name) == nil {
		return ""
	}
	return name
}

func (f *Function) ResultSubAggregationType(ctx context.Context, defs base.DefinitionsSource) string {
	if f == nil {
		return ""
	}
	name := buildObjectAggregationTypeName(f.field.Type.Name(), true, false)
	if defs.ForName(ctx, name) == nil {
		return ""
	}
	return name
}

func (f *Function) ResultBucketAggregationType(ctx context.Context, defs base.DefinitionsSource) string {
	if f == nil {
		return ""
	}
	name := buildObjectAggregationTypeName(f.field.Type.Name(), false, true)
	if defs.ForName(ctx, name) == nil {
		return ""
	}
	return name
}

func NewFunction(module, name, sql string, t *ast.Type, isTable, jsonCast bool, args ast.ArgumentDefinitionList, pos *ast.Position) *ast.FieldDefinition {
	def := &ast.FieldDefinition{
		Name:      name,
		Type:      t,
		Arguments: args,
		Directives: ast.DirectiveList{
			{
				Name: base.FunctionDirectiveName,
				Arguments: ast.ArgumentList{
					{
						Name:     "name",
						Value:    &ast.Value{Kind: ast.StringValue, Raw: name, Position: pos},
						Position: pos,
					},
					{
						Name:     "sql",
						Value:    &ast.Value{Kind: ast.StringValue, Raw: sql, Position: pos},
						Position: pos,
					},
					{
						Name:     "is_table",
						Value:    &ast.Value{Kind: ast.BooleanValue, Raw: fmt.Sprint(isTable), Position: pos},
						Position: pos,
					},
					{
						Name:     "json_cast",
						Value:    &ast.Value{Kind: ast.BooleanValue, Raw: fmt.Sprint(jsonCast), Position: pos},
						Position: pos,
					},
				},
			},
		},
	}
	if module != "" {
		def.Directives = append(def.Directives, base.ModuleDirective(module, pos))
	}
	return def
}

type FunctionCall struct {
	ReferencesName   string
	argumentMap      map[string]string
	IsTableFuncJoin  bool
	sourceFields     []string
	referencesFields []string
	sql              string
	Module           string

	query     *ast.Field
	directive *ast.Directive
}

func FunctionCallInfo(field *ast.Field) *FunctionCall {
	if d := field.Definition.Directives.ForName(base.FunctionDirectiveName); d != nil {
		return &FunctionCall{
			ReferencesName: field.Name,
			query:          field,
			Module:         FunctionModule(field.Definition),
		}
	}
	var directive *ast.Directive
	for _, n := range []string{base.FunctionCallDirectiveName, base.FunctionCallTableJoinDirectiveName} {
		if d := field.Definition.Directives.ForName(n); d != nil {
			directive = d
			break
		}
	}
	if directive == nil {
		return nil
	}
	info := parseFunctionCallDirective(directive)
	info.query = field
	return info
}

func FunctionCallDefinitionInfo(def *ast.FieldDefinition) *FunctionCall {
	d := def.Directives.ForName(base.FunctionCallDirectiveName)
	if d != nil {
		return parseFunctionCallDirective(d)
	}
	d = def.Directives.ForName(base.FunctionCallTableJoinDirectiveName)
	if d != nil {
		return parseFunctionCallDirective(d)
	}
	return nil
}

func parseFunctionCallDirective(def *ast.Directive) *FunctionCall {
	if def == nil {
		return nil
	}
	if def.Name != base.FunctionCallDirectiveName &&
		def.Name != base.FunctionCallTableJoinDirectiveName {
		return nil
	}
	ref := &FunctionCall{
		argumentMap: make(map[string]string),
		IsTableFuncJoin: def.Name == base.FunctionCallTableJoinDirectiveName ||
			directiveArgValue(def, "is_table_func_join") == "true",
		ReferencesName:   directiveArgValue(def, "references_name"),
		sql:              directiveArgValue(def, "sql"),
		sourceFields:     directiveArgChildValues(def, "source_fields"),
		referencesFields: directiveArgChildValues(def, "references_fields"),
		Module:           directiveArgValue(def, "module"),
		directive:        def,
	}
	if a := def.Arguments.ForName("args"); a != nil {
		for _, f := range a.Value.Children {
			ref.argumentMap[f.Name] = f.Value.Raw
		}
	}
	return ref
}

func (f *FunctionCall) FunctionInfo(ctx context.Context, defs base.DefinitionsSource) (*Function, error) {
	if f.query != nil && f.query.Definition.Directives.ForName(base.FunctionDirectiveName) != nil {
		return FunctionInfo(f.query.Definition)
	}
	module := defs.ForName(ctx, ModuleTypeName(f.Module, ModuleFunction))
	if module == nil {
		return nil, ErrorPosf(f.directive.Position, "module root object %s for function is not defined", f.Module)
	}
	function := module.Fields.ForName(f.ReferencesName)
	if function != nil {
		return FunctionInfo(function)
	}
	return nil, ErrorPosf(nil, "unknown function %s", f.ReferencesName)
}

func (f *FunctionCall) ArgumentMap() map[string]string {
	out := make(map[string]string, len(f.argumentMap))
	for k, v := range f.argumentMap {
		out[k] = v
	}
	return out
}

func (f *FunctionCall) SourceFields() ([]string, error) {
	fields := make([]string, 0, len(f.sourceFields))
	for _, fn := range f.argumentMap {
		fields = append(fields, fn)
	}
	if !f.IsTableFuncJoin {
		return fields, nil
	}
	fields = append(fields, f.sourceFields...)
	for _, field := range ExtractFieldsFromSQL(f.sql) {
		parts := strings.SplitN(field, ".", 2)
		if len(parts) != 2 {
			return nil, ErrorPosf(f.query.Position, "invalid field %s in SQL", field)
		}
		if base.JoinSourceFieldPrefix == parts[0] {
			fields = append(fields, parts[1])
		}
	}
	fields = append(fields, ExtractFieldsFromSQL(f.sql)...)
	return RemoveFieldsDuplicates(fields), nil
}

func (f *FunctionCall) ReferencesFields() []string {
	if !f.IsTableFuncJoin {
		return nil
	}
	fields := append([]string(nil), f.referencesFields...)
	for _, field := range ExtractFieldsFromSQL(f.sql) {
		parts := strings.SplitN(field, ".", 2)
		if len(parts) != 2 {
			continue
		}
		if base.JoinRefFieldPrefix == parts[0] {
			fields = append(fields, parts[1])
		}
	}
	return RemoveFieldsDuplicates(fields)
}

func (f *FunctionCall) JoinConditionsTemplate() string {
	conditions := make([]string, 0, len(f.sourceFields))
	for i, sfn := range f.sourceFields {
		conditions = append(conditions, fmt.Sprintf(
			"[%s.%s] = [%s.%s]",
			base.JoinSourceFieldPrefix, sfn,
			base.JoinRefFieldPrefix, f.referencesFields[i],
		))
	}
	if f.sql != "" {
		conditions = append(conditions, f.sql)
	}
	return strings.Join(conditions, " AND ")
}

func (f *FunctionCall) ArgumentValues(ctx context.Context, defs base.DefinitionsSource, vars map[string]any) (FieldQueryArguments, error) {
	args := make([]FieldQueryArgument, 0, len(f.query.Definition.Arguments))
	for _, def := range f.query.Definition.Arguments {
		arg := f.query.Arguments.ForName(def.Name)
		if arg == nil && def.DefaultValue == nil {
			if def.Type.NonNull {
				return nil, ErrorPosf(f.query.Position, "argument %s is required", def.Name)
			}
			args = append(args, FieldQueryArgument{
				Name:  def.Name,
				Type:  def.Type,
				Value: nil,
			})
			continue
		}
		if arg == nil {
			arg = &ast.Argument{
				Name:     def.Name,
				Value:    def.DefaultValue,
				Position: base.CompiledPos(""),
			}
		}
		value := arg.Value
		if value == nil {
			value = def.DefaultValue
		}
		v, err := ParseArgumentValue(ctx, defs, def, value, vars, false)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		args = append(args, FieldQueryArgument{
			Name:  arg.Name,
			Type:  def.Type,
			Value: v,
		})
	}
	return args, nil
}
