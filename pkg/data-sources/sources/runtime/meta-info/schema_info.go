package metainfo

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

type SchemaInfo struct {
	QueryRoot    string           `json:"query_root"`
	MutationRoot string           `json:"mutation_root"`
	DataSources  []DataSourceInfo `json:"data_sources"`
	RootModule   ModuleInfo       `json:"modules"`
}

func (s *SchemaInfo) DataSource(name string) *DataSourceInfo {
	for _, ds := range s.DataSources {
		if ds.Name == name {
			return &ds
		}
	}
	return nil
}

func (s *SchemaInfo) Modules() []*ModuleInfo {
	return s.RootModule.Modules()
}

func (s *SchemaInfo) Module(path string) *ModuleInfo {
	if path == "" {
		return &s.RootModule
	}
	return s.RootModule.SubModule(path)
}

func (s *SchemaInfo) DataObjects() []*DataObjectInfo {
	return s.RootModule.DataObjects()
}

func (s *SchemaInfo) Table(path string) *DataObjectInfo {
	pp := strings.Split(path, ".")
	if len(pp) == 0 {
		return nil
	}
	tn := pp[len(pp)-1]
	m := s.Module(strings.Join(pp[:len(pp)-1], "."))
	if m == nil {
		return nil
	}
	for i := range m.Tables {
		if m.Tables[i].Name == tn {
			return &m.Tables[i]
		}
	}
	return nil
}

func (s *SchemaInfo) View(path string) *DataObjectInfo {
	pp := strings.Split(path, ".")
	if len(pp) == 0 {
		return nil
	}
	tn := pp[len(pp)-1]
	m := s.Module(strings.Join(pp[:len(pp)-1], "."))
	if m == nil {
		return nil
	}
	for i := range m.Views {
		if m.Views[i].Name == tn {
			return &m.Views[i]
		}
	}
	return nil
}

func (s *SchemaInfo) Functions() []*FunctionInfo {
	return s.RootModule.AllFunctions()
}

func (s *SchemaInfo) MutationFunctions() []*FunctionInfo {
	return s.RootModule.AllMutationFunctions()
}

func (s *SchemaInfo) Function(path string) *FunctionInfo {
	pp := strings.Split(path, ".")
	if len(pp) == 0 {
		return nil
	}
	fn := pp[len(pp)-1]
	m := s.Module(strings.Join(pp[:len(pp)-1], "."))
	if m == nil {
		return nil
	}
	for i := range m.Functions {
		if m.Functions[i].Name == fn {
			return &m.Functions[i]
		}
	}
	return nil
}

func (s *SchemaInfo) MutateFunction(path string) *FunctionInfo {
	pp := strings.Split(path, ".")
	if len(pp) == 0 {
		return nil
	}
	fn := pp[len(pp)-1]
	m := s.Module(strings.Join(pp[:len(pp)-1], "."))
	if m == nil {
		return nil
	}
	for i := range m.MutateFunctions {
		if m.MutateFunctions[i].Name == fn {
			return &m.MutateFunctions[i]
		}
	}
	return nil
}

func (s *SchemaInfo) SpecialQuery(path string) *QueryInfo {
	pp := strings.Split(path, ".")
	if len(pp) == 0 {
		return nil
	}
	qn := pp[len(pp)-1]
	m := s.Module(strings.Join(pp[:len(pp)-1], "."))
	if m == nil {
		return nil
	}
	for i := range m.SpecialQueries {
		if m.SpecialQueries[i].Name == qn {
			return &m.SpecialQueries[i]
		}
	}
	return nil
}

type DataSourceInfo struct {
	Name        string `json:"name"`
	Prefix      string `json:"prefix"`
	Type        string `json:"type"`
	Description string `json:"description"`
	ReadOnly    bool   `json:"read_only"`
	AsModule    bool   `json:"as_module"`
}

type ModuleInfo struct {
	Name                 string           `json:"name"`
	Description          string           `json:"description"`
	QueryType            string           `json:"query_type,omitempty"`
	FunctionType         string           `json:"function_type,omitempty"`
	MutationType         string           `json:"mutation_type,omitempty"`
	MutationFunctionType string           `json:"mutation_function_type,omitempty"`
	SubModules           []ModuleInfo     `json:"sub_modules,omitempty"`
	Tables               []DataObjectInfo `json:"tables,omitempty"`
	Views                []DataObjectInfo `json:"views,omitempty"`
	Functions            []FunctionInfo   `json:"functions,omitempty"`
	MutateFunctions      []FunctionInfo   `json:"mutate_functions,omitempty"`
	SpecialQueries       []QueryInfo      `json:"special_queries,omitempty"`
}

func (m *ModuleInfo) SubModule(path string) *ModuleInfo {
	for i := range m.SubModules {
		if m.SubModules[i].Name == path {
			return &m.SubModules[i]
		}
		if strings.HasPrefix(path, m.SubModules[i].Name+".") {
			return m.SubModules[i].SubModule(path)
		}
	}
	return nil
}

func (m *ModuleInfo) Modules() []*ModuleInfo {
	var out []*ModuleInfo
	out = append(out, m)
	for i := range m.SubModules {
		out = append(out, m.SubModules[i].Modules()...)
	}
	return out
}

func (m *ModuleInfo) DataObjects() []*DataObjectInfo {
	var out []*DataObjectInfo
	for i := range m.SubModules {
		out = append(out, m.SubModules[i].DataObjects()...)
	}
	for i := range m.Tables {
		out = append(out, &m.Tables[i])
	}
	for i := range m.Views {
		out = append(out, &m.Views[i])
	}
	return out
}

func (m *ModuleInfo) DataObject(name string) *DataObjectInfo {
	for i := range m.Tables {
		if m.Tables[i].Name == name {
			return &m.Tables[i]
		}
	}
	for i := range m.Views {
		if m.Views[i].Name == name {
			return &m.Views[i]
		}
	}
	return nil
}

func (m *ModuleInfo) AllFunctions() []*FunctionInfo {
	var out []*FunctionInfo
	for i := range m.SubModules {
		out = append(out, m.SubModules[i].AllFunctions()...)
	}
	for i := range m.Functions {
		out = append(out, &m.Functions[i])
	}
	return out
}

func (m *ModuleInfo) AllMutationFunctions() []*FunctionInfo {
	var out []*FunctionInfo
	for i := range m.SubModules {
		out = append(out, m.SubModules[i].AllMutationFunctions()...)
	}
	for i := range m.MutateFunctions {
		out = append(out, &m.MutateFunctions[i])
	}
	return out
}

func (m *ModuleInfo) Function(name string) *FunctionInfo {
	for i := range m.Functions {
		if m.Functions[i].Name == name {
			return &m.Functions[i]
		}
	}
	return nil
}

func (m *ModuleInfo) MutationFunction(name string) *FunctionInfo {
	for i := range m.MutateFunctions {
		if m.MutateFunctions[i].Name == name {
			return &m.MutateFunctions[i]
		}
	}
	return nil
}

type DataObjectInfo struct {
	Name          string             `json:"name"`
	Module        string             `json:"module"`
	DataSource    string             `json:"data_source"`
	Type          DataObjectType     `json:"type"`
	Description   string             `json:"description"`
	HasPrimaryKey bool               `json:"has_primary_key"`
	HasGeometry   bool               `json:"has_geometry"`
	IsM2M         bool               `json:"is_m2m"`
	IsCube        bool               `json:"is_cube"`
	IsHypertable  bool               `json:"is_hypertable"`
	Columns       []FieldInfo        `json:"columns"`
	References    []SubqueryInfo     `json:"relations,omitempty"`
	Subqueries    []SubqueryInfo     `json:"subqueries,omitempty"`
	FunctionCalls []FunctionCallInfo `json:"function_calls,omitempty"`

	AggregationType       string        `json:"aggregation_type,omitempty"`
	SubAggregationType    string        `json:"sub_aggregation_type,omitempty"`
	BucketAggregationType string        `json:"bucket_aggregation_type,omitempty"`
	FilterType            string        `json:"filter_type"`
	Arguments             *ArgumentInfo `json:"arguments,omitempty"`

	Queries   []QueryInfo   `json:"queries"`
	Mutations *MutationInfo `json:"mutations,omitempty"`
}

type DataObjectType string

const (
	DataObjectTypeTable DataObjectType = compiler.Table
	DataObjectTypeView  DataObjectType = compiler.View
)

type ReferenceType string

const (
	ReferenceTypeOneToMany  ReferenceType = "one_to_many"
	ReferenceTypeManyToOne  ReferenceType = "many_to_one"
	ReferenceTypeManyToMany ReferenceType = "many_to_many"
)

type SubqueryInfo struct {
	Name                string        `json:"name"`
	Type                ReferenceType `json:"type"`
	M2MTable            string        `json:"m2m_table,omitempty"`
	DataSource          string        `json:"data_source"`
	DataObject          string        `json:"data_object"`
	Module              string        `json:"module"`
	FieldDataQuery      string        `json:"field_data_query,omitempty"`
	FieldDataType       string        `json:"field_data_type,omitempty"`
	FieldAggQuery       string        `json:"field_agg_query,omitempty"`
	FieldAggDataType    string        `json:"field_agg_data_type,omitempty"`
	FieldBucketAggQuery string        `json:"field_bucket_agg_query,omitempty"`
	FieldBucketAggType  string        `json:"field_bucket_agg_type,omitempty"`
	Description         string        `json:"description"`
	DataObjectFields    []FieldInfo   `json:"fields,omitempty"`
}

type FunctionCallInfo struct {
	Name            string         `json:"name"`
	Module          string         `json:"module"`
	DataSource      string         `json:"data_source"`
	FieldName       string         `json:"field_name"`
	ReturnType      string         `json:"result_type"`
	IsScalarFunc    bool           `json:"is_scalar"`
	ReturnsArray    bool           `json:"returns_array"`
	IsTableFuncJoin bool           `json:"is_table_func_join"`
	Description     string         `json:"description"`
	Arguments       []ArgumentInfo `json:"arguments,omitempty"`
}

type QueryInfo struct {
	Name             string         `json:"name"`
	Path             string         `json:"path"`
	Type             QueryType      `json:"type"`
	Description      string         `json:"description"`
	Arguments        []ArgumentInfo `json:"arguments,omitempty"`
	ReturnedTypeName string         `json:"returned_type"`
	IsSingleRow      bool           `json:"is_single_row"`
	ReturnTypeFields []FieldInfo    `json:"return_type_fields,omitempty"`
}

type QueryType string

const (
	QueryTypeJQ              QueryType = "jq"
	QueryTypeH3              QueryType = "h3"
	QueryTypeSelect          QueryType = "select"
	QueryTypeSelectOne       QueryType = "select_one"
	QueryTypeAggregate       QueryType = "aggregate"
	QueryTypeAggregateBucket QueryType = "bucket_aggregate"
)

type ArgumentInfo struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	Description  string      `json:"description"`
	IsScalarType bool        `json:"is_scalar_type"`
	IsRequired   bool        `json:"is_required"`
	IsArray      bool        `json:"is_array"`
	NestedFields []FieldInfo `json:"nested_fields,omitempty"`
}

type MutationInfo struct {
	InsertMutation string `json:"insert_mutation"`
	InsertDataType string `json:"insert_data_type"`
	UpdateMutation string `json:"update_mutation"`
	UpdateDataType string `json:"update_data_type"`
	DeleteMutation string `json:"delete_mutation"`
}

type FunctionInfo struct {
	Name                  string         `json:"name"`
	Description           string         `json:"description"`
	Module                string         `json:"module"`
	DataSource            string         `json:"data_source"`
	Arguments             []ArgumentInfo `json:"arguments,omitempty"`
	ReturnType            string         `json:"return_type"`
	ReturnsArray          bool           `json:"returns_array"`
	ReturnTypeFields      []FieldInfo    `json:"return_type_fields,omitempty"`
	AggregationType       string         `json:"aggregation_type,omitempty"`
	SubAggregationType    string         `json:"sub_aggregation_type,omitempty"`
	BucketAggregationType string         `json:"bucket_aggregation_type,omitempty"`
}

type FieldInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`

	ReturnsArray bool `json:"returns_array"`
	IsPrimaryKey bool `json:"is_primary_key"`
	Nullable     bool `json:"nullable"`
	IsCalculated bool `json:"is_calculated"`

	Arguments []ArgumentInfo `json:"arguments,omitempty"`

	NestedFields []FieldInfo `json:"nested_fields,omitempty"`
	ExtraFields  []FieldInfo `json:"extra_fields,omitempty"`
}

// Function to get summary structured information about schema.
func (s *Source) Summary(ctx context.Context) (*SchemaInfo, error) {
	schema := s.qe.Schema()
	if schema == nil {
		return nil, fmt.Errorf("schema is not available")
	}

	si := &SchemaInfo{
		QueryRoot:    schema.Query.Name,
		MutationRoot: schema.Mutation.Name,
	}

	// 1. DataSources
	dss, err := s.schemaDataSources(ctx)
	if err != nil {
		return nil, err
	}
	si.DataSources = dss

	// 2.Modules
	var rootQueryModule, rootMutationModule *ModuleInfo
	if schema.Query != nil {
		rootQueryModule, err = s.moduleInfo(schema, si, schema.Query)
		if err != nil {
			return nil, err
		}
	}
	if schema.Mutation != nil {
		rootMutationModule, err = s.moduleInfo(schema, si, schema.Mutation)
		if err != nil {
			return nil, err
		}
	}
	rootModule := mergeModules(rootQueryModule, rootMutationModule)
	if rootModule != nil {
		si.RootModule = *rootModule
	}

	return si, nil
}

func mergeModules(rootQueryModule, rootMutationModule *ModuleInfo) *ModuleInfo {
	if rootQueryModule == nil {
		return nil
	}
	merged := &ModuleInfo{}

	merged.QueryType = rootQueryModule.QueryType
	var frm *ModuleInfo
	for _, subModule := range rootQueryModule.SubModules {
		if subModule.FunctionType == base.FunctionTypeName {
			frm = &subModule
			continue
		}
		merged.SubModules = append(merged.SubModules, subModule)
	}
	if frm != nil {
		mergeSubModules(merged, frm)
	}

	if rootMutationModule == nil {
		return merged
	}

	merged.MutationType = rootMutationModule.MutationType
	for _, subModule := range rootMutationModule.SubModules {
		if subModule.MutationFunctionType == base.FunctionMutationTypeName {
			mergeSubModules(merged, &subModule)
			continue
		}
		m := merged.SubModule(subModule.Name)
		if m == nil {
			merged.SubModules = append(merged.SubModules, subModule)
			continue
		}
		mergeSubModules(m, &subModule)
	}

	return merged
}

func mergeSubModules(rootModule, addModule *ModuleInfo) {
	if addModule == nil {
		return
	}
	if rootModule.QueryType == "" {
		rootModule.QueryType = addModule.QueryType
	}
	if rootModule.MutationType == "" {
		rootModule.MutationType = addModule.MutationType
	}
	if rootModule.FunctionType == "" {
		rootModule.FunctionType = addModule.FunctionType
	}
	if rootModule.MutationFunctionType == "" {
		rootModule.MutationFunctionType = addModule.MutationFunctionType
	}
	for _, f := range addModule.Functions {
		rootModule.Functions = append(rootModule.Functions, f)
	}
	for _, mf := range addModule.MutateFunctions {
		rootModule.MutateFunctions = append(rootModule.MutateFunctions, mf)
	}
	for _, t := range addModule.Tables {
		rootModule.Tables = append(rootModule.Tables, t)
	}
	for _, v := range addModule.Views {
		rootModule.Views = append(rootModule.Views, v)
	}
	for _, addSub := range addModule.SubModules {
		mainModule := rootModule.SubModule(addSub.Name)
		if mainModule == nil {
			rootModule.SubModules = append(rootModule.SubModules, addSub)
			continue
		}
		mergeSubModules(mainModule, &addSub)
	}
}

func (s *Source) schemaDataSources(ctx context.Context) ([]DataSourceInfo, error) {
	var dss []types.DataSource
	res, err := s.qe.Query(ctx, `query {
		core{
			data_sources {
				name
				type
				prefix
				description
				read_only
				as_module
			}
		}
	}`, nil)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	err = res.ScanData("core.data_sources", &dss)
	if err != nil {
		return nil, err
	}
	res.Close()
	var infos []DataSourceInfo
	for _, ds := range dss {
		infos = append(infos, DataSourceInfo{
			Name:        ds.Name,
			Type:        string(ds.Type),
			Prefix:      ds.Prefix,
			Description: ds.Description,
			ReadOnly:    ds.ReadOnly,
			AsModule:    ds.AsModule,
		})
	}
	return infos, nil
}

func (s *Source) schemaModulesInfo(schema *ast.Schema, si *SchemaInfo, rootName string) ([]ModuleInfo, error) {
	var modules []ModuleInfo
	for _, field := range schema.Types[rootName].Fields {
		if field.Type.Name() == schema.Query.Name ||
			field.Type.Name() == schema.Mutation.Name {
			continue
		}
		def := schema.Types[field.Type.Name()]
		info := compiler.ModuleRootInfo(def)
		if info == nil {
			continue
		}
		module, err := s.moduleInfo(schema, si, def)
		if err != nil {
			return nil, err
		}
		if module == nil {
			continue
		}

		modules = append(modules, *module)
	}
	return modules, nil
}

func (s *Source) moduleInfo(schema *ast.Schema, si *SchemaInfo, def *ast.Definition) (*ModuleInfo, error) {
	info := compiler.ModuleRootInfo(def)
	if info == nil {
		return nil, fmt.Errorf("module root info not found for %s", def.Name)
	}
	module := ModuleInfo{
		Name:        info.Name,
		Description: def.Description,
	}
	// name of the graphql module type
	switch info.Type {
	case compiler.ModuleQuery:
		module.QueryType = def.Name
	case compiler.ModuleMutation:
		module.MutationType = def.Name
	case compiler.ModuleFunction:
		module.FunctionType = def.Name
	case compiler.ModuleMutationFunction:
		module.MutationFunctionType = def.Name
	}
	// submodules
	sm, err := s.schemaModulesInfo(schema, si, def.Name)
	if err != nil {
		return nil, err
	}
	module.SubModules = sm

	if info.Type == compiler.ModuleMutation {
		return &module, nil
	}

	// tables and views
	for _, mf := range def.Fields {
		// special queries
		if mf.Name == compiler.JQTransformQueryName && mf.Type.NamedType == schema.Query.Name ||
			mf.Name == base.H3QueryFieldName && mf.Type.NamedType == base.H3QueryTypeName {
			// Handle special query
			special := QueryInfo{
				Name:             mf.Name,
				Description:      mf.Description,
				ReturnedTypeName: mf.Type.Name(),
				IsSingleRow:      mf.Type.NamedType != "",
			}
			if mf.Name == compiler.JQTransformQueryName {
				special.Type = QueryTypeJQ
			}
			if mf.Name == base.H3QueryFieldName {
				special.Type = QueryTypeH3
			}
			spDef := schema.Types[mf.Type.Name()]
			if spDef != nil {
				special.ReturnTypeFields = objectFieldsInfo(schema, spDef, 3, true)
			}
			for _, arg := range mf.Arguments {
				special.Arguments = append(special.Arguments, argumentInfo(schema, arg, 2))
			}
			module.SpecialQueries = append(module.SpecialQueries, special)
		}
		if compiler.IsFunction(mf) {
			// Handle function fields
			funcInfo, err := compiler.FunctionInfo(mf)
			if err != nil {
				return nil, err
			}

			function := FunctionInfo{
				Name:                  mf.Name,
				Description:           mf.Description,
				Module:                module.Name,
				DataSource:            funcInfo.Catalog,
				ReturnType:            mf.Type.Name(),
				ReturnsArray:          mf.Type.NamedType == "",
				AggregationType:       funcInfo.ResultAggregationType(compiler.SchemaDefs(schema)),
				SubAggregationType:    funcInfo.ResultSubAggregationType(compiler.SchemaDefs(schema)),
				BucketAggregationType: funcInfo.ResultBucketAggregationType(compiler.SchemaDefs(schema)),
			}
			if !compiler.IsScalarType(mf.Type.Name()) {
				def := schema.Types[mf.Type.Name()]
				if def == nil {
					return nil, fmt.Errorf("type %s not found", mf.Type.Name())
				}
				function.ReturnTypeFields = objectFieldsInfo(schema, def, 0, true)
			}
			// function arguments
			for _, arg := range mf.Arguments {
				argInfo := ArgumentInfo{
					Name:         arg.Name,
					Description:  arg.Description,
					Type:         arg.Type.Name(),
					IsRequired:   arg.Type.NonNull,
					IsScalarType: compiler.IsScalarType(arg.Type.Name()),
					IsArray:      arg.Type.NamedType == "",
				}
				function.Arguments = append(function.Arguments, argInfo)
			}
			if info.Type == compiler.ModuleMutationFunction {
				module.MutateFunctions = append(module.MutateFunctions, function)
			}
			if info.Type == compiler.ModuleFunction {
				module.Functions = append(module.Functions, function)
			}
			continue
		}
		if !compiler.IsSelectQueryDefinition(mf) {
			continue
		}
		dtDef := schema.Types[mf.Type.Name()]
		dtInfo := compiler.DataObjectInfo(dtDef)
		if dtInfo == nil {
			return nil, fmt.Errorf("data object info not found for %s", dtDef.Name)
		}
		dataObject := DataObjectInfo{
			Name:                  mf.Type.Name(),
			Module:                module.Name,
			Description:           dtDef.Description,
			DataSource:            dtInfo.Catalog,
			IsM2M:                 dtInfo.IsM2M,
			IsCube:                dtInfo.IsCube,
			IsHypertable:          dtInfo.IsHypertable,
			FilterType:            dtInfo.InputFilterName(),
			AggregationType:       dtInfo.AggregationTypeName(compiler.SchemaDefs(schema)),
			SubAggregationType:    dtInfo.SubAggregationTypeName(compiler.SchemaDefs(schema)),
			BucketAggregationType: dtInfo.BucketAggregationTypeName(compiler.SchemaDefs(schema)),
		}

		for _, df := range dtDef.Fields {
			switch {
			case compiler.IsReferencesSubquery(df):
				// Handle references subquery
				reference := referenceInfo(dtInfo, schema, df, dtDef)
				if reference == nil {
					continue
				}
				dataObject.References = append(dataObject.References, *reference)
			case compiler.IsJoinSubqueryDefinition(df):
				// Handle join subquery
				sq, err := subQueryInfo(df, schema, dtDef)
				if err != nil {
					return nil, err
				}
				if sq == nil {
					continue
				}
				dataObject.Subqueries = append(dataObject.Subqueries, *sq)
			case compiler.IsFunctionCallSubqueryDefinition(df) ||
				compiler.IsTableFuncJoinSubqueryDefinition(df):
				// Handle function call subquery
				function, err := functionCallInfo(df, schema)
				if err != nil {
					return nil, err
				}
				if function == nil {
					continue
				}
				dataObject.FunctionCalls = append(dataObject.FunctionCalls, *function)
			case compiler.IsAggregateQueryDefinition(df) ||
				compiler.IsBucketAggregateQueryDefinition(df) ||
				strings.HasPrefix(df.Name, "_"):
				// skip extrafields and aggregation subquery
			default:
				// handle field (not subquery)
				fi := fieldInfo(df, dtDef, schema, 0, true)
				if fi == nil {
					continue
				}
				dataObject.Columns = append(dataObject.Columns, *fi)
				if fi.IsPrimaryKey {
					dataObject.HasPrimaryKey = true
				}
				if fi.Type == compiler.GeometryTypeName && !fi.ReturnsArray {
					dataObject.HasGeometry = true
				}
			}
		}

		// queries
		for _, q := range dtInfo.Queries() {
			var qt QueryType
			switch q.Type {
			case compiler.QueryTypeSelect:
				qt = QueryTypeSelect
			case compiler.QueryTypeSelectOne:
				qt = QueryTypeSelectOne
			case compiler.QueryTypeAggregate:
				qt = QueryTypeAggregate
			case compiler.QueryTypeAggregateBucket:
				qt = QueryTypeAggregateBucket
			default:
				continue
			}
			qf := def.Fields.ForName(q.Name)
			if qf == nil {
				return nil, fmt.Errorf("query field %s not found in type %s", q.Name, dtDef.Name)
			}
			query := QueryInfo{
				Name:             q.Name,
				Type:             qt,
				Path:             module.Name,
				ReturnedTypeName: qf.Type.Name(),
				IsSingleRow:      qf.Type.NamedType != "",
			}
			for _, arg := range qf.Arguments {
				query.Arguments = append(query.Arguments, argumentInfo(schema, arg, 1))
			}
			if def := schema.Types[qf.Type.Name()]; def != nil {
				query.ReturnTypeFields = objectFieldsInfo(schema, def, 3, true)
			}
			dataObject.Queries = append(dataObject.Queries, query)
		}

		if dtInfo.Type == compiler.Table {
			// mutation info
			dataObject.Type = DataObjectTypeTable
			dataObject.Mutations = &MutationInfo{
				InsertMutation: dtInfo.InsertMutationName(),
				InsertDataType: dtInfo.InputInsertDataName(),
				UpdateMutation: dtInfo.UpdateMutationName(),
				UpdateDataType: dtInfo.InputUpdateDataName(),
				DeleteMutation: dtInfo.DeleteMutationName(),
			}
			module.Tables = append(module.Tables, dataObject)
		}
		if dtInfo.Type == compiler.View {
			dataObject.Type = DataObjectTypeView
			if dtInfo.HasArguments() {
				argsType := schema.Types[dtInfo.InputArgsName]
				if argsType == nil {
					return nil, fmt.Errorf("arguments type not found for %s", dtInfo.InputArgsName)
				}
				dataObject.Arguments = &ArgumentInfo{
					Name:         "args",
					Description:  argsType.Description,
					Type:         dtInfo.InputArgsName,
					IsRequired:   dtInfo.RequiredArgs,
					NestedFields: objectFieldsInfo(schema, argsType, 1, true),
				}
			}
			module.Views = append(module.Views, dataObject)
		}
	}
	return &module, nil
}

func referenceInfo(dtInfo *compiler.Object, schema *ast.Schema, df *ast.FieldDefinition, dtDef *ast.Definition) *SubqueryInfo {
	refInfo := dtInfo.ReferencesQueryInfo(
		compiler.SchemaDefs(schema),
		df.Name,
	)
	reference := SubqueryInfo{
		Name:           df.Name,
		M2MTable:       refInfo.M2MName,
		DataSource:     dtInfo.Catalog,
		DataObject:     df.Type.Name(),
		FieldDataQuery: df.Name,
		FieldDataType:  df.Type.Name(),
		Description:    df.Description,
	}
	def := schema.Types[refInfo.ReferencesName]
	if def != nil {
		reference.Module = compiler.ObjectModule(def)
	}

	reference.DataObjectFields = objectFieldsInfo(schema, def, 3, true)

	// define reference type
	switch {
	case refInfo.IsM2M:
		reference.Type = ReferenceTypeManyToMany
	case df.Type.NamedType == "":
		reference.Type = ReferenceTypeOneToMany
	default:
		reference.Type = ReferenceTypeManyToOne
	}
	// aggregation queries for the reference
	for _, sf := range dtDef.Fields {
		if !compiler.IsAggregateQueryDefinition(sf) &&
			!compiler.IsBucketAggregateQueryDefinition(sf) {
			continue
		}
		fieldName, isBucket := compiler.AggregatedQueryFieldName(sf)
		if fieldName != df.Name {
			continue
		}
		if isBucket {
			reference.FieldBucketAggQuery = fieldName
			reference.FieldBucketAggType = sf.Type.Name()
			continue
		}
		reference.FieldAggQuery = sf.Name
		reference.FieldAggDataType = sf.Type.Name()
	}
	return &reference
}

func subQueryInfo(df *ast.FieldDefinition, schema *ast.Schema, dtDef *ast.Definition) (*SubqueryInfo, error) {
	joinInfo := compiler.JoinDefinitionInfo(df)
	if joinInfo == nil {
		return nil, fmt.Errorf("join info not found for %s", df.Name)
	}
	jt := schema.Types[joinInfo.ReferencesName]
	if jt == nil {
		return nil, fmt.Errorf("join target type not found for %s", joinInfo.ReferencesName)
	}
	jtInfo := compiler.DataObjectInfo(jt)
	if jtInfo == nil {
		return nil, fmt.Errorf("join target type info not found for %s", joinInfo.ReferencesName)
	}
	sq := SubqueryInfo{
		Name:             df.Name,
		DataObject:       joinInfo.ReferencesName,
		FieldDataQuery:   df.Name,
		Description:      df.Description,
		DataSource:       jtInfo.Catalog,
		Module:           compiler.ObjectModule(jt),
		DataObjectFields: objectFieldsInfo(schema, jt, 3, true),
	}
	// aggregation queries for the join subquery
	for _, sf := range dtDef.Fields {
		if !compiler.IsAggregateQueryDefinition(sf) &&
			!compiler.IsBucketAggregateQueryDefinition(sf) {
			continue
		}
		fieldName, isBucket := compiler.AggregatedQueryFieldName(sf)
		if fieldName != df.Name {
			continue
		}
		if isBucket {
			sq.FieldBucketAggQuery = fieldName
			sq.FieldBucketAggType = sf.Type.Name()
			continue
		}
		sq.FieldAggQuery = sf.Name
	}
	return &sq, nil
}

func functionCallInfo(df *ast.FieldDefinition, schema *ast.Schema) (*FunctionCallInfo, error) {
	fcInfo := compiler.FunctionCallDefinitionInfo(df)
	if fcInfo == nil {
		return nil, fmt.Errorf("function call info not found for %s", df.Name)
	}
	funcDef, err := fcInfo.FunctionInfo(compiler.SchemaDefs(schema))
	if err != nil {
		return nil, fmt.Errorf("function info not found for %s: %w", df.Name, err)
	}
	if funcDef == nil {
		return nil, fmt.Errorf("function definition not found for %s", df.Name)
	}

	function := FunctionCallInfo{
		Name:            funcDef.Name,
		Description:     df.Description,
		FieldName:       df.Name,
		Module:          fcInfo.Module,
		DataSource:      funcDef.Catalog,
		ReturnType:      df.Type.Name(),
		ReturnsArray:    df.Type.NamedType == "",
		IsScalarFunc:    compiler.IsScalarType(df.Type.Name()),
		IsTableFuncJoin: fcInfo.IsTableFuncJoin,
	}
	// function arguments
	for _, arg := range df.Arguments {
		function.Arguments = append(function.Arguments, argumentInfo(schema, arg, 2))
	}
	return &function, nil
}

const maxNestedFieldLevels = 3

func objectFieldsInfo(schema *ast.Schema, def *ast.Definition, level int, skipSubqueries bool) []FieldInfo {
	if level > maxNestedFieldLevels {
		return nil
	}
	var fields []FieldInfo
	for _, field := range def.Fields {
		if strings.HasPrefix(field.Name, "_") ||
			compiler.IsAggregateQueryDefinition(field) ||
			compiler.IsBucketAggregateQueryDefinition(field) {
			// skip system fields
			continue
		}
		if skipSubqueries && (compiler.IsJoinSubqueryDefinition(field) ||
			compiler.IsReferencesSubquery(field) ||
			compiler.IsFunctionCallSubqueryDefinition(field) ||
			compiler.IsTableFuncJoinSubqueryDefinition(field)) {
			continue
		}
		fi := fieldInfo(field, def, schema, level, skipSubqueries)
		if fi == nil {
			continue
		}
		fields = append(fields, *fi)
	}
	return fields
}

func fieldInfo(field *ast.FieldDefinition, def *ast.Definition, schema *ast.Schema, level int, skipSubqueries bool) *FieldInfo {
	info := compiler.FieldDefinitionInfo(field, def)
	if info == nil {
		return nil
	}
	fi := FieldInfo{
		Name:         field.Name,
		Description:  field.Description,
		Type:         field.Type.Name(),
		IsPrimaryKey: field.Directives.ForName(base.FieldPrimaryKeyDirectiveName) != nil,
		IsCalculated: info.IsCalcField(),
		ReturnsArray: field.Type.NamedType == "",
		Nullable:     !field.Type.NonNull,
	}
	for _, arg := range field.Arguments {
		argInfo := ArgumentInfo{
			Name:         arg.Name,
			Description:  arg.Description,
			Type:         arg.Type.Name(),
			IsRequired:   arg.Type.NonNull,
			IsScalarType: compiler.IsScalarType(arg.Type.Name()),
			IsArray:      arg.Type.NamedType == "",
		}
		fi.Arguments = append(fi.Arguments, argInfo)
	}
	if !compiler.IsScalarType(field.Type.Name()) {
		// If the field is an object type, recurse into its fields
		def := schema.Types[field.Type.Name()]
		if def != nil {
			fi.NestedFields = objectFieldsInfo(schema, def, level+1, skipSubqueries)
		}
	}
	if compiler.IsScalarType(field.Type.Name()) {
		// find extra fields
		for _, ef := range def.Fields {
			ed := ef.Directives.ForName(base.FieldExtraFieldDirectiveName)
			if ed == nil ||
				ed.Arguments.ForName("base_field") == nil ||
				ed.Arguments.ForName("base_field").Value.Raw != field.Name {
				continue
			}
			extraFieldInfo := FieldInfo{
				Name:         ef.Name,
				Description:  ef.Description,
				Type:         ef.Type.Name(),
				ReturnsArray: ef.Type.NamedType == "",
				IsCalculated: true,
			}
			for _, arg := range ef.Arguments {
				extraFieldInfo.Arguments = append(extraFieldInfo.Arguments, argumentInfo(schema, arg, 2))
			}
			if !compiler.IsScalarType(ef.Type.Name()) {
				// If the extra field is an object type, recurse into its fields
				def := schema.Types[ef.Type.Name()]
				if def != nil {
					extraFieldInfo.NestedFields = objectFieldsInfo(schema, def, 1, skipSubqueries)
				}
			}
			fi.ExtraFields = append(fi.ExtraFields, extraFieldInfo)
		}
	}
	return &fi
}

func argumentInfo(schema *ast.Schema, arg *ast.ArgumentDefinition, maxNestedLevel int) ArgumentInfo {
	if maxNestedLevel <= 0 && maxNestedLevel+1 > maxNestedFieldLevels {
		maxNestedLevel = maxNestedFieldLevels
	}
	maxNestedLevel = maxNestedFieldLevels - maxNestedLevel + 1
	argInfo := ArgumentInfo{
		Name:         arg.Name,
		Description:  arg.Description,
		Type:         arg.Type.Name(),
		IsRequired:   arg.Type.NonNull,
		IsScalarType: compiler.IsScalarType(arg.Type.Name()),
		IsArray:      arg.Type.NamedType == "",
	}
	if !compiler.IsScalarType(arg.Type.Name()) {
		// If the argument is an object type, recurse into its fields
		def := schema.Types[arg.Type.Name()]
		if def != nil {
			argInfo.NestedFields = objectFieldsInfo(schema, def, maxNestedLevel, true)
		}
	}
	return argInfo
}
