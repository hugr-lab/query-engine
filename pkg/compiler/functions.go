package compiler

import (
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	functionDirectiveName              = "function"
	functionCallDirectiveName          = "function_call"
	functionCallTableJoinDirectiveName = "table_function_call_join"
)

func addFunctionsPrefix(defs Definitions, def *ast.Definition, opt *Options) {
	for _, field := range def.Fields {
		if checkSystemDirective(field.Directives) {
			continue
		}

		field.Directives = append(field.Directives, base.OriginalNameDirective(field.Name))
		if !opt.AsModule {
			field.Name = opt.Prefix + "_" + field.Name
		}

		// rename arguments types
		for _, arg := range field.Arguments {
			arg.Type = typeWithPrefix(defs, arg.Type, opt.Prefix+"_")
		}

		// rename output type
		field.Type = typeWithPrefix(defs, field.Type, opt.Prefix+"_")
	}
}

func typeWithPrefix(defs Definitions, t *ast.Type, prefix string) *ast.Type {
	if t == nil {
		return nil
	}
	if IsScalarType(t.Name()) || prefix == "" {
		return t
	}
	ot := defs.ForName(t.Name())
	if ot != nil && ot.Directives.ForName(base.OriginalNameDirectiveName) != nil {
		return t
	}
	if t.NamedType != "" {
		return &ast.Type{
			NamedType: prefix + t.NamedType,
			NonNull:   t.NonNull,
		}
	}
	return &ast.Type{
		NonNull: t.NonNull,
		Elem:    typeWithPrefix(defs, t.Elem, prefix),
	}
}

func validateFunctions(defs Definitions, def *ast.Definition, opt *Options) error {
	if def.Name != base.FunctionTypeName && def.Name != base.FunctionMutationTypeName {
		return nil
	}
	for _, field := range def.Fields {
		info, err := FunctionInfo(field)
		if err != nil {
			return err
		}
		err = info.validate(defs, opt)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateFunctionCall(defs Definitions, def *ast.Definition, field *ast.FieldDefinition, checkArgsMap bool) error {
	info := functionCallInfo(field.Directives.ForName(functionCallDirectiveName))
	if info == nil {
		info = functionCallInfo(field.Directives.ForName(functionCallTableJoinDirectiveName))
	}
	if info == nil {
		return ErrorPosf(field.Position, "field %s should have a directive @%s or @%s",
			field.Name, functionCallDirectiveName, functionCallTableJoinDirectiveName,
		)
	}

	return info.validate(defs, def, field, checkArgsMap)
}

func assignFunctionByModules(schema *ast.SchemaDocument) {
	function := schema.Definitions.ForName(base.FunctionTypeName)
	if function != nil {
		for _, field := range function.Fields {
			module := functionModule(field)
			if module == "" {
				continue
			}
			moduleObject := moduleType(schema, module, ModuleFunction)
			moduleObject.Fields = append(moduleObject.Fields, field)
			addModuleToFunctionCalls(schema, field)
		}
		// remove function from the root query
		var fields []*ast.FieldDefinition
		for _, field := range function.Fields {
			if field.Directives.ForName(moduleRootDirectiveName) != nil ||
				functionModule(field) != "" {
				continue
			}
			fields = append(fields, field)
		}
		function.Fields = fields
	}

	mutation := schema.Definitions.ForName(base.FunctionMutationTypeName)
	if mutation != nil {
		for _, field := range mutation.Fields {
			module := functionModule(field)
			if module == "" {
				continue
			}
			moduleObject := moduleType(schema, module, ModuleMutationFunction)
			moduleObject.Fields = append(moduleObject.Fields, field)
		}
		var fields []*ast.FieldDefinition
		for _, field := range mutation.Fields {
			if field.Directives.ForName(moduleRootDirectiveName) != nil ||
				functionModule(field) != "" {
				continue
			}
			fields = append(fields, field)
		}
		mutation.Fields = fields
	}
}

func addModuleToFunctionCalls(schema *ast.SchemaDocument, function *ast.FieldDefinition) {
	module := functionModule(function)
	if module == "" {
		return
	}
	for _, def := range schema.Definitions {
		if def.Kind != ast.Object || !IsDataObject(def) {
			continue
		}
		for _, field := range def.Fields {
			if !IsFunctionCall(field) {
				continue
			}
			d := field.Directives.ForName(functionCallDirectiveName)
			if d == nil {
				d = field.Directives.ForName(functionCallTableJoinDirectiveName)
			}
			if d == nil {
				continue
			}
			if directiveArgValue(d, "references_name") != function.Name {
				continue
			}
			if a := d.Arguments.ForName("module"); a != nil {
				a.Value = &ast.Value{
					Kind:     ast.StringValue,
					Raw:      module,
					Position: d.Position,
				}
			} else {
				d.Arguments = append(d.Arguments, &ast.Argument{
					Name:     "module",
					Value:    &ast.Value{Kind: ast.StringValue, Raw: module, Position: d.Position},
					Position: d.Position,
				})
			}
		}
	}
}

type FunctionCall struct {
	referencesName   string
	argumentMap      map[string]string
	IsTableFuncJoin  bool
	sourceFields     []string
	referencesFields []string
	sql              string
	module           string

	query     *ast.Field
	directive *ast.Directive
}

func FunctionCallInfo(field *ast.Field) *FunctionCall {
	if d := field.Definition.Directives.ForName(functionDirectiveName); d != nil {
		return &FunctionCall{
			referencesName: field.Name,
			query:          field,
			module:         functionModule(field.Definition),
		}
	}
	var directive *ast.Directive
	for _, n := range []string{functionCallDirectiveName, functionCallTableJoinDirectiveName} {
		if d := field.Definition.Directives.ForName(n); d != nil {
			directive = d
			break
		}
	}
	if directive == nil {
		return nil
	}
	info := functionCallInfo(directive)
	info.query = field
	return info
}

func functionCallInfo(def *ast.Directive) *FunctionCall {
	if def == nil {
		return nil
	}
	if def.Name != functionCallDirectiveName &&
		def.Name != functionCallTableJoinDirectiveName {
		return nil
	}
	ref := &FunctionCall{
		argumentMap: make(map[string]string),
		IsTableFuncJoin: def.Name == functionCallTableJoinDirectiveName ||
			directiveArgValue(def, "is_table_func_join") == "true",
		referencesName:   directiveArgValue(def, "references_name"),
		sql:              directiveArgValue(def, "sql"),
		sourceFields:     directiveArgChildValues(def, "source_fields"),
		referencesFields: directiveArgChildValues(def, "references_fields"),
		module:           directiveArgValue(def, "module"),
		directive:        def,
	}
	if a := def.Arguments.ForName("args"); a != nil {
		for _, f := range a.Value.Children {
			ref.argumentMap[f.Name] = f.Value.Raw
		}
	}

	return ref
}

func (f *FunctionCall) FunctionInfo(defs Definitions) (*Function, error) {
	if f.query != nil && f.query.Definition.Directives.ForName(functionDirectiveName) != nil {
		return FunctionInfo(f.query.Definition)
	}
	module := defs.ForName(moduleTypeName(f.module, ModuleFunction))
	if module == nil {
		return nil, ErrorPosf(f.directive.Position, "module root object %s for function is not defined", f.module)
	}

	function := module.Fields.ForName(f.referencesName)
	if function != nil {
		return FunctionInfo(function)
	}

	return nil, ErrorPosf(nil, "unknown function %s", f.referencesName)
}

func (f *FunctionCall) ArgumentMap() map[string]string {
	out := make(map[string]string, len(f.argumentMap))
	for k, v := range f.argumentMap {
		out[k] = v
	}
	return out
}

func (f *FunctionCall) validate(defs Definitions, def *ast.Definition, field *ast.FieldDefinition, checkArgsMap bool) error {
	// check arguments
	function, err := f.FunctionInfo(defs)
	if err != nil {
		return err
	}

	// check types
	if !f.IsTableFuncJoin && !IsEqualTypes(function.field.Type, field.Type) {
		return ErrorPosf(field.Position, "function %s return type should be %s the same as in the function definition", f.referencesName, field.Type.Name())
	}
	if f.IsTableFuncJoin && (function.field.Type.Name() != field.Type.Name() || function.field.Type.NamedType != "") {
		return ErrorPosf(field.Position, "function %s return type should be %s the same as in the function definition", f.referencesName, field.Type.Name())
	}

	// check catalog
	fieldCatalog := base.FieldCatalogName(field)
	funcCatalog := function.field.Directives.ForName(base.CatalogDirectiveName)

	if fieldCatalog == "" && funcCatalog != nil { // add catalog directive to function call field
		field.Directives = append(field.Directives, funcCatalog)
	}

	// check arguments
	usedArgs := map[string]struct{}{}
	for _, arg := range field.Arguments {
		a := function.field.Arguments.ForName(arg.Name)
		if a == nil {
			return ErrorPosf(field.Position, "function %s doesn't have argument %s", f.referencesName, arg.Name)
		}
		if _, ok := f.argumentMap[arg.Name]; ok {
			return ErrorPosf(field.Position, "function argument %s is redefined in args", arg.Name)
		}
		// check types
		if !IsEqualTypes(a.Type, arg.Type) {
			return ErrorPosf(field.Position, "function %s argument %s type should be %s the same as in the function definition", f.referencesName, arg.Name, arg.Type.Name())
		}
		usedArgs[arg.Name] = struct{}{}
	}
	for an, fn := range f.argumentMap {
		if function.field.Arguments.ForName(an) == nil {
			return ErrorPosf(field.Position, "function %s doesn't have argument %s", f.referencesName, an)
		}
		if checkArgsMap {
			fv := objectFieldByPath(defs, def.Name, fn, true, true)
			if fv == nil {
				return ErrorPosf(field.Position, "function %s argument %s is not used", f.referencesName, an)
			}
			if !IsEqualTypes(fv.Type, function.field.Arguments.ForName(an).Type) {
				return ErrorPosf(field.Position, "function %s argument %s type should be %s the same as in the function definition", f.referencesName, an, fv.Type.Name())
			}
		}
		usedArgs[an] = struct{}{}
	}

	// check that all required function arguments are used
	for _, arg := range function.field.Arguments {
		if _, ok := usedArgs[arg.Name]; !ok && arg.DefaultValue == nil {
			return ErrorPosf(field.Position, "function %s argument %s is not used", f.referencesName, arg.Name)
		}
	}
	return nil
}

func (f *FunctionCall) SourceFields() ([]string, error) {
	fields := make([]string, 0, len(f.sourceFields))
	// add argument map fields
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
		if JoinSourceFieldPrefix == parts[0] {
			fields = append(fields, parts[1])
		}
	}

	// add fields from SQL
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
		if JoinRefFieldPrefix == parts[0] {
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
			JoinSourceFieldPrefix, sfn,
			JoinRefFieldPrefix, f.referencesFields[i],
		))
	}
	if f.sql != "" {
		conditions = append(conditions, f.sql)
	}
	return strings.Join(conditions, " AND ")
}

func (f *FunctionCall) ArgumentValues(defs Definitions, vars map[string]any) (FieldQueryArguments, error) {
	// go by function arguments
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
				Position: compiledPos(),
			}
		}
		value := arg.Value
		if value == nil { // get value from default value
			value = def.DefaultValue
		}
		v, err := ParseArgumentValue(defs, def, value, vars, false)
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

func IsFunctionCall(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(functionCallDirectiveName) != nil ||
		field.Directives.ForName(functionCallTableJoinDirectiveName) != nil
}

func IsTableFuncJoin(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(functionCallTableJoinDirectiveName) != nil
}

func IsFunction(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(functionDirectiveName) != nil
}

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
	d := field.Directives.ForName(functionDirectiveName)
	if d == nil {
		return nil, ErrorPosf(field.Position, "field %s should have a directive @%s", field.Name, functionDirectiveName)
	}
	catalog := fieldDirectiveArgValue(field, base.CatalogDirectiveName, "name")
	isTable := fieldDirectiveArgValue(field, functionDirectiveName, "is_table") == "true" ||
		!IsScalarType(field.Type.Name()) && field.Type.NamedType == ""

	return &Function{
		Module:       fieldDirectiveArgValue(field, base.ModuleDirectiveName, "name"),
		Catalog:      catalog,
		Name:         directiveArgValue(d, "name"),
		SkipNullArg:  directiveArgValue(d, "skip_null_arg") == "true" && len(field.Arguments) == 1,
		JsonCast:     !isTable && fieldDirectiveArgValue(field, functionDirectiveName, "json_cast") == "true",
		ReturnsTable: isTable,
		sql:          directiveArgValue(d, "sql"),
		field:        field,
	}, nil
}

func (f *Function) SQL() string {
	sql := f.sql
	if sql != "" {
		sql = strings.ReplaceAll(sql, "["+catalogSystemVariableName+"]", "'"+f.Catalog+"'")
	}
	if sql == "" {
		// create sql to call the function
		d := f.field.Directives.ForName(functionDirectiveName)
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

func NewFunction(module, name, sql string, t *ast.Type, isTable, jsonCast bool, args ast.ArgumentDefinitionList, pos *ast.Position) *ast.FieldDefinition {
	def := &ast.FieldDefinition{
		Name:      name,
		Type:      t,
		Arguments: args,
		Directives: ast.DirectiveList{
			{
				Name: functionDirectiveName,
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

func (f *Function) ArgumentByName(name string) *ast.ArgumentDefinition {
	return f.field.Arguments.ForName(name)
}

func (f *Function) validate(defs Definitions, opt *Options) error {
	if f.Name == "" {
		return ErrorPosf(nil, "function name is required")
	}
	// check output type
	typeName := f.field.Type.Name()
	if t := defs.ForName(typeName); t == nil && !IsScalarType(typeName) {
		return ErrorPosf(f.field.Position, "unknown function return type %s ", typeName)
	}

	// check arguments
	for _, arg := range f.field.Arguments {
		t := defs.ForName(arg.Type.Name())
		if t == nil {
			return ErrorPosf(f.field.Position, "unknown function argument type %s ", arg.Type.Name())
		}
		if t.Kind != ast.InputObject && t.Kind != ast.Scalar { // only scalar or input object can be used as an argument for functions
			return ErrorPosf(f.field.Position, "function argument type %s should be an input object or scalar value", arg.Type.Name())
		}
	}

	f.field.Directives = append(f.field.Directives, opt.catalog)

	// if catalog is compiled as module, add module directive
	if opt.AsModule {
		if d := f.field.Directives.ForName(base.ModuleDirectiveName); d != nil {
			if a := d.Arguments.ForName("name"); a != nil {
				if a.Value.Raw == "" {
					a.Value.Raw = opt.Name
				} else {
					a.Value.Raw = opt.Name + "." + a.Value.Raw
				}
				f.Module = a.Value.Raw
			}
			f.Module = opt.Name
			f.field.Directives = append(f.field.Directives, base.ModuleDirective(opt.Name, f.field.Position))
		}
	}

	return nil
}

type FieldQueryArgument struct {
	Name  string
	Type  *ast.Type
	Value any
}

type FieldQueryArguments []FieldQueryArgument

func (a FieldQueryArguments) ForName(name string) *FieldQueryArgument {
	for _, arg := range a {
		if arg.Name == name {
			return &arg
		}
	}
	return nil
}
