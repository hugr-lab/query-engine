package http

import (
	"context"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestLoadSpecs(t *testing.T) {
	tests := []struct {
		name        string
		Path        string
		queryParams map[string]string
		wantParams  httpSourceParams
		wantErr     bool
	}{
		{
			name:    "Invalid spec path",
			Path:    "invalid-url",
			wantErr: true,
		},
		{
			name: "Valid spec path with no query params",
			Path: "./open-api-test.yaml",
			wantParams: httpSourceParams{
				serverURL: "http://localhost:8080",
				specPath:  "./open-api-test.yaml",
				hasSpec:   true,
				isFile:    true,
			},
		},
		{
			name: "Valid spec path with server url param",
			Path: "./open-api-test.yaml",
			queryParams: map[string]string{
				serverParamKey: "http://localhost:8081",
			},
			wantParams: httpSourceParams{
				serverURL: "http://localhost:8081",
				specPath:  "./open-api-test.yaml",
				hasSpec:   true,
				isFile:    true,
			},
		},
		{
			name: "Valid spec path with security params",
			Path: "https://example.com",
			queryParams: map[string]string{
				securityParamsKey: `{"type":"apiKey","api_key":"12345","name":"api_key","in":"header"}`,
				specPathParamKey:  "./open-api-test.yaml",
			},
			wantParams: httpSourceParams{
				serverURL: "https://example.com",
				specPath:  "./open-api-test.yaml",
				hasSpec:   true,
				isFile:    true,
				securityParams: httpSecurityParams{
					Type:   "apiKey",
					ApiKey: "12345",
					Name:   "api_key",
					In:     "header",
				},
			},
		},
		{
			name: "Valid spec path with security params",
			Path: "https://example.com",
			queryParams: map[string]string{
				securityParamsKey: `{"schema_name":"oauth2","type":"oauth2","username":"test","password":"test","flow_name":"password"}`,
				specPathParamKey:  "./open-api-test.yaml",
			},
			wantParams: httpSourceParams{
				serverURL: "https://example.com",
				specPath:  "./open-api-test.yaml",
				hasSpec:   true,
				isFile:    true,
				securityParams: httpSecurityParams{
					SchemaName: "oauth2",
					Type:       "oauth2",
					Username:   "test",
					Password:   "test",
					FlowName:   "password",
					Flows:      &openapi3.OAuthFlows{},
				},
			},
		},
		{
			name: "Valid spec path with security params",
			Path: "https://example.com",
			queryParams: map[string]string{
				securityParamsKey: `{"type":"apiKey","api_key":"12345","name":"api_key","in":"header"}`,
				specUrlParamKey:   "https://example.com",
			},
			wantErr: true,
		},
		{
			name: "Valid spec path with server param",
			Path: "https://example.com",
			queryParams: map[string]string{
				serverParamKey: "https://api.example.com",
			},
			wantParams: httpSourceParams{
				serverURL: "https://api.example.com",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			path := tt.Path
			if len(tt.queryParams) != 0 {
				u, err := url.Parse(tt.Path)
				if err != nil {
					t.Fatalf("failed to parse spec path: %v", err)
				}
				q := u.Query()
				for k, v := range tt.queryParams {
					q.Set(k, v)
				}
				u.RawQuery = q.Encode()
				path = u.String()
			}
			sp, err := sourceParamsFromPath(path)
			if err != nil {
				t.Fatalf("failed to get source params: %v", err)
			}
			source := &Source{
				ds:     types.DataSource{Path: path},
				params: sp,
			}

			err = source.loadSpecs(ctx)
			if tt.wantErr != (err != nil) {
				t.Fatalf("expected error but got: %v", err)
			}
			if err != nil {
				return
			}
			flows := source.params.securityParams.Flows
			source.params.securityParams.Flows = tt.wantParams.securityParams.Flows
			if !reflect.DeepEqual(source.params, tt.wantParams) ||
				(flows != nil) != (tt.wantParams.securityParams.Flows != nil) {
				t.Errorf("expected params to be %+v but got %+v", tt.wantParams, source.params)
			}
		})
	}
}

func TestPrintSpec(t *testing.T) {
	source := &Source{
		ds: types.DataSource{Path: "../../internal/fixture/open-api/openapi.json?" + serverParamKey + "=http://localhost:8081"},
	}
	err := source.loadSpecs(context.Background())
	if err != nil {
		t.Fatalf("failed to load spec: %v", err)
	}

	var defs ast.DefinitionList
	var gt *ast.Type
	pos := compiler.CompiledPosName("http-openapi-test")

	var functions []openApiFunction
	if source.spec != nil && source.spec.Paths != nil {
		for path, item := range source.spec.Paths.Map() {
			t.Logf("Path: %s", path)
			for method, op := range item.Operations() {
				opName := op.OperationID
				if opName == "" {
					opName = path
				}
				opName = openAPINameToGraphQLFuncName(opName)
				if len(item.Operations()) > 1 {
					opName += method
				}
				f := openApiFunction{
					Name:         opName,
					OperationId:  op.OperationID,
					Path:         path,
					Method:       method,
					Parameters:   make(map[string]openApiFuncParam),
					RequestBody:  make(map[string]*ast.Type),
					ResponseBody: make(map[string]map[string]*ast.Type),
				}
				t.Logf("\tMethod: %s, OperationId: %s, OperationName: %s", method, op.OperationID, opName)
				baseParent := op.OperationID
				if baseParent == "" {
					baseParent = path
				}
				baseParent = openAPINameToGraphQLName(baseParent)
				if op.Parameters != nil {
					for i, p := range op.Parameters {
						if p.Value == nil {
							t.Errorf("\t\tParameter: %d, skip", i)
							continue
						}
						t.Logf("\t\tParameter: %s, in: %s", p.Value.Name, p.Value.In)
						printSchemaRef(t, p.Value.Schema, 3)
						parentName := baseParent + "Param" + openAPINameToGraphQLName(p.Value.Name)
						gt, defs, err = openApiSchemaRefToGraphQL(defs, p.Value.Schema, parentName, pos, true)
						if err != nil {
							t.Errorf("failed to convert schema to graphql: %v", err)
							continue
						}
						if p.Value.Required {
							gt.NonNull = true
						}
						f.Parameters[p.Value.Name] = openApiFuncParam{
							Name:        p.Value.Name,
							In:          p.Value.In,
							Description: p.Value.Description,
							Type:        gt,
						}
					}
				}
				if op.RequestBody != nil {
					for md, content := range op.RequestBody.Value.Content {
						parentName, ok := typeNameForMediaType(baseParent, md)
						if !ok {
							t.Logf("\t\tRequest Body: Media Type: %s, skip", md)
							continue
						}
						t.Logf("\t\tRequest Body: Media Type: %s", md)
						parentName += "RequestBody"
						printSchemaRef(t, content.Schema, 3)
						gt, defs, err = openApiSchemaRefToGraphQL(defs, content.Schema, parentName, pos, true)
						if err != nil {
							t.Errorf("failed to convert schema to graphql: %v", err)
							continue
						}
						f.RequestBody[md] = gt
					}
				}
				for code, resp := range op.Responses.Map() {
					t.Logf("\t\tResponse: %s", code)
					f.ResponseBody[code] = make(map[string]*ast.Type)
					for md, content := range resp.Value.Content {
						parentName, ok := typeNameForMediaType(baseParent, md)
						if !ok {
							t.Logf("\t\t\tResponse Body: Media Type: %s, skip", md)
							continue
						}
						parentName += "ResponseBody"
						t.Logf("\t\t\tResponse Body: Media Type: %s", md)
						printSchemaRef(t, content.Schema, 4)
						gt, defs, err = openApiSchemaRefToGraphQL(defs, content.Schema, parentName, pos, false)
						if err != nil {
							t.Errorf("failed to convert schema to graphql: %v", err)
							continue
						}
						f.ResponseBody[code][md] = gt
					}
				}
				functions = append(functions, f)
			}
		}
	}

	t.Log("GraphQL Functions:")
	for _, f := range functions {
		t.Logf("\tFunction: %s, operationId: %s, Path: %s, Method: %s", f.Name, f.OperationId, f.Path, f.Method)
		t.Logf("\tParameters:")
		for _, p := range f.Parameters {
			t.Logf("\t\tParameter: %s, type: %s, is_array: %v, required: %v", p.Name, p.Type.Name(), p.Type.NamedType == "", p.Type.NonNull)
		}
		t.Logf("\tRequest Body:")
		for md, gt := range f.RequestBody {
			t.Logf("\t\tMedia Type: %s, type: %s, is_array: %v", md, gt.Name(), gt.NamedType == "")
		}
		t.Logf("\tResponse Body:")
		for code, m := range f.ResponseBody {
			t.Logf("\t\tResponse Code: %s", code)
			for md, gt := range m {
				t.Logf("\t\t\tMedia Type: %s, type: %s, is_array: %v", md, gt.Name(), gt.NamedType == "")
			}
		}
	}
	t.Log("GraphQL Definitions:")
	for _, def := range defs {
		t.Logf("\tDefinition: %s, kind: %v", def.Name, def.Kind)
		t.Logf("\tFields:")
		for _, f := range def.Fields {
			t.Logf("\t\tField: %s, type: %s, is_array: %v, required: %v", f.Name, f.Type.Name(), f.Type.NamedType == "", f.Type.NonNull)
		}
	}
}

func TestOpenAPISchemaToGraphQL(t *testing.T) {
	tests := []struct {
		name       string
		schema     *openapi3.Schema
		parentName string
		asInput    bool
		wantType   *ast.Type
		wantDefs   ast.DefinitionList
		wantErr    bool
	}{
		{
			name:       "String type",
			schema:     openapi3.NewStringSchema(),
			parentName: "TestString",
			wantType:   ast.NamedType("String", nil),
		},
		{
			name:       "Integer type",
			schema:     openapi3.NewIntegerSchema(),
			parentName: "TestInt",
			wantType:   ast.NamedType("Int", nil),
		},
		{
			name:       "Number type",
			schema:     openapi3.NewFloat64Schema(),
			parentName: "TestFloat",
			wantType:   ast.NamedType("Float", nil),
		},
		{
			name:       "Boolean type",
			schema:     openapi3.NewBoolSchema(),
			parentName: "TestBoolean",
			wantType:   ast.NamedType("Boolean", nil),
		},
		{
			name:       "Array type",
			schema:     openapi3.NewArraySchema().WithItems(openapi3.NewStringSchema()),
			parentName: "TestArray",
			wantType:   ast.ListType(ast.NamedType("String", nil), nil),
		},
		{
			name: "Object type",
			schema: openapi3.NewObjectSchema().
				WithProperties(map[string]*openapi3.Schema{
					"field1": openapi3.NewStringSchema(),
					"field2": openapi3.NewIntegerSchema(),
				}).
				WithRequired([]string{"field1"}),
			parentName: "TestObject",
			wantType:   ast.NamedType("TestObject", nil),
			wantDefs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "TestObject",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "field1",
							Type: &ast.Type{
								NamedType: "String",
								NonNull:   true,
							},
						},
						&ast.FieldDefinition{
							Name: "field2",
							Type: &ast.Type{
								NamedType: "Int",
							},
						},
					},
				},
			},
		},
		{
			name: "Object type with input",
			schema: openapi3.NewObjectSchema().
				WithProperties(map[string]*openapi3.Schema{
					"field1": openapi3.NewStringSchema(),
					"field2": openapi3.NewIntegerSchema(),
				}).
				WithRequired([]string{"field1"}),
			parentName: "TestObject",
			asInput:    true,
			wantType:   ast.NamedType("TestObject", nil),
			wantDefs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.InputObject,
					Name: "TestObject",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "field1",
							Type: &ast.Type{
								NamedType: "String",
								NonNull:   true,
							},
						},
						&ast.FieldDefinition{
							Name: "field2",
							Type: &ast.Type{
								NamedType: "Int",
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDefs, err := openAPISchemaToGraphQL(nil, tt.schema, tt.parentName, nil, tt.asInput)
			if (err != nil) != tt.wantErr {
				t.Errorf("openAPISchemaToGraphQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotType, tt.wantType) {
				t.Errorf("openAPISchemaToGraphQL() gotType = %v, want %v", gotType, tt.wantType)
			}
			checkEqualDefs(t, gotDefs, tt.wantDefs)
		})
	}
}

func TestFunctionFields(t *testing.T) {
	tests := []struct {
		name    string
		funcs   []openApiFunction
		defs    ast.DefinitionList
		want    []*ast.FieldDefinition
		wantErr bool
	}{
		{
			name: "Single function with query parameter",
			funcs: []openApiFunction{
				{
					Name:   "getUser",
					Path:   "/user",
					Method: "GET",
					Parameters: map[string]openApiFuncParam{
						"id": {
							Name: "id",
							In:   "query",
							Type: ast.NamedType("String", nil),
						},
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "id",
							Type: ast.NamedType("String", nil),
						},
					},
				},
			},
			want: []*ast.FieldDefinition{
				compiler.NewFunction("http.test", "getUser", "http_data_source_request_scalar([$catalog], '/user', 'GET', '{}'::JSON, {id: [id]}::JSON, '{}'::JSON, '')", ast.NamedType("User", nil), false, true, ast.ArgumentDefinitionList{
					{
						Name: "id",
						Type: ast.NamedType("String", nil),
					},
				}, nil),
			},
		},
		{
			name: "Function with query and header parameters",
			funcs: []openApiFunction{
				{
					Name:   "getUser",
					Path:   "/user",
					Method: "GET",
					Parameters: map[string]openApiFuncParam{
						"id": {
							Name: "id",
							In:   "query",
							Type: ast.NamedType("String", nil),
						},
						"auth": {
							Name: "auth",
							In:   "header",
							Type: ast.NamedType("String", nil),
						},
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: []*ast.FieldDefinition{
						{Name: "id", Type: ast.NamedType("String", nil)},
					},
				},
			},
			want: []*ast.FieldDefinition{
				compiler.NewFunction("http.test", "getUser", "http_data_source_request_scalar([$catalog], '/user', 'GET', {auth: [auth]}::JSON, {id: [id]}::JSON, '{}'::JSON, '')", ast.NamedType("User", nil), false, true, ast.ArgumentDefinitionList{
					{
						Name: "id",
						Type: ast.NamedType("String", nil),
					},
				}, nil),
			},
		},
		{
			name: "Function with request body",
			funcs: []openApiFunction{
				{
					Name:   "createUser",
					Path:   "/user",
					Method: "POST",
					RequestBody: map[string]*ast.Type{
						"application/json": ast.NamedType("CreateUserInput", nil),
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.InputObject,
					Name: "CreateUserInput",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "name",
							Type: ast.NamedType("String", nil),
						},
					},
				},
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "id",
							Type: ast.NamedType("String", nil),
						},
					},
				},
			},
			want: []*ast.FieldDefinition{
				compiler.NewFunction("http.test", "createUser", "http_data_source_request_scalar([$catalog], '/user', 'POST', '{}'::JSON, '{}'::JSON, {name: [name]}::JSON, '')", ast.NamedType("User", nil), false, true, ast.ArgumentDefinitionList{
					{
						Name: "request_body",
						Type: ast.NamedType("CreateUserInput", nil),
					},
				}, nil),
			},
		},
		{
			name: "Function with headers",
			funcs: []openApiFunction{
				{
					Name:   "getUserWithHeader",
					Path:   "/user",
					Method: "GET",
					Parameters: map[string]openApiFuncParam{
						"Authorization": {
							Name: "Authorization",
							In:   "header",
							Type: ast.NamedType("String", nil),
						},
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "id",
							Type: ast.NamedType("String", nil),
						},
					},
				},
			},
			want: []*ast.FieldDefinition{
				compiler.NewFunction("http.test", "getUserWithHeader", "http_data_source_request_scalar([$catalog], '/user', 'GET', {Authorization: [Authorization]}::JSON, '{}'::JSON, '{}'::JSON, '')", ast.NamedType("User", nil), false, true, ast.ArgumentDefinitionList{
					{
						Name: "Authorization",
						Type: ast.NamedType("String", nil),
					},
				}, nil),
			},
		},
		{
			name: "Function with missing response type",
			funcs: []openApiFunction{
				{
					Name:   "getUser",
					Path:   "/user",
					Method: "GET",
					Parameters: map[string]openApiFuncParam{
						"id": {
							Name: "id",
							In:   "query",
							Type: ast.NamedType("String", nil),
						},
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {},
					},
				},
			},
			defs:    ast.DefinitionList{},
			want:    nil,
			wantErr: false,
		},
		{
			name: "Function with query parameters and request body",
			funcs: []openApiFunction{
				{
					Name:   "createUser",
					Path:   "/user",
					Method: "POST",
					Parameters: map[string]openApiFuncParam{
						"auth": {
							Name: "auth",
							In:   "header",
							Type: ast.NamedType("String", nil),
						},
					},
					RequestBody: map[string]*ast.Type{
						"application/json": ast.NamedType("CreateUserInput", nil),
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: []*ast.FieldDefinition{
						{Name: "id", Type: ast.NamedType("String", nil)},
					},
				},
				&ast.Definition{
					Kind: ast.InputObject,
					Name: "CreateUserInput",
					Fields: []*ast.FieldDefinition{
						{Name: "name", Type: ast.NamedType("String", nil)},
						{Name: "email", Type: ast.NamedType("String", nil)},
					},
				},
			},
			want: []*ast.FieldDefinition{
				compiler.NewFunction("http.test", "createUser", "http_data_source_request_scalar([$catalog], '/user', 'POST', {auth: [auth]}::JSON, '{}'::JSON, COALESCE([request_body]::JSON, '{}'::JSON), '')", ast.NamedType("User", nil), false, true, ast.ArgumentDefinitionList{
					{Name: "auth", Type: ast.NamedType("String", nil)},
					{Name: "request_body", Type: ast.NamedType("CreateUserInput", nil)},
				}, nil),
			},
		},
		{
			name: "Function with missing request body type",
			funcs: []openApiFunction{
				{
					Name:   "createUser",
					Path:   "/user",
					Method: "POST",
					RequestBody: map[string]*ast.Type{
						"application/json": ast.NamedType("NonExistentType", nil),
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "id",
							Type: ast.NamedType("String", nil),
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Function with path parameter",
			funcs: []openApiFunction{
				{
					Name:   "getUserWithPathParam",
					Path:   "/user/{id}/details",
					Method: "GET",
					Parameters: map[string]openApiFuncParam{
						"Authorization": {
							Name: "Authorization",
							In:   "header",
							Type: ast.NamedType("String", nil),
						},
						"id": {
							Name: "id",
							In:   "path",
							Type: ast.NamedType("Int", nil),
						},
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "id",
							Type: ast.NamedType("String", nil),
						},
					},
				},
			},
			want: []*ast.FieldDefinition{
				compiler.NewFunction("http.test", "getUserWithPathParam", "http_data_source_request_scalar([$catalog], '/user/'||[id]||'/details', 'GET', {Authorization: [Authorization]}::JSON, '{}'::JSON, '{}'::JSON, '')", ast.NamedType("User", nil), false, true, ast.ArgumentDefinitionList{
					{
						Name: "Authorization",
						Type: ast.NamedType("String", nil),
					},
				}, nil),
			},
		},
		{
			name: "Function with path parameter",
			funcs: []openApiFunction{
				{
					Name:   "getUserWithPathParam",
					Path:   "/user/{id}",
					Method: "GET",
					Parameters: map[string]openApiFuncParam{
						"Authorization": {
							Name: "Authorization",
							In:   "header",
							Type: ast.NamedType("String", nil),
						},
						"id": {
							Name: "id",
							In:   "path",
							Type: ast.NamedType("Int", nil),
						},
					},
					ResponseBody: map[string]map[string]*ast.Type{
						"200": {
							"application/json": ast.NamedType("User", nil),
						},
					},
				},
			},
			defs: ast.DefinitionList{
				&ast.Definition{
					Kind: ast.Object,
					Name: "User",
					Fields: ast.FieldList{
						&ast.FieldDefinition{
							Name: "id",
							Type: ast.NamedType("String", nil),
						},
					},
				},
			},
			want: []*ast.FieldDefinition{
				compiler.NewFunction("http.test", "getUserWithPathParam", "http_data_source_request_scalar([$catalog], '/user/'||[id], 'GET', {Authorization: [Authorization]}::JSON, '{}'::JSON, '{}'::JSON, '')", ast.NamedType("User", nil), false, true, ast.ArgumentDefinitionList{
					{
						Name: "Authorization",
						Type: ast.NamedType("String", nil),
					},
				}, nil),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &Source{
				ds: types.DataSource{Name: "test"},
			}
			got, err := source.functionFields(tt.defs, tt.funcs, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("functionFields() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			checkEqualFields(t, got, tt.want)
		})
	}
}

func checkEqualDefs(t *testing.T, got, want ast.DefinitionList) {
	if len(got) != len(want) {
		t.Fatalf("expected %d definitions but got %d", len(want), len(got))
	}
	for _, w := range want {
		g := got.ForName(w.Name)
		if g == nil {
			t.Fatalf("definition %s not found", w.Name)
		}
		if g.Kind != w.Kind ||
			g.Description != w.Description ||
			len(g.Fields) != len(w.Fields) ||
			len(g.Directives) != len(w.Directives) {
			t.Fatalf("definition %s mismatch", w.Name)
		}
		checkEqualFields(t, g.Fields, w.Fields)
		checkEqualDirectives(t, g.Directives, w.Directives)
	}
}

func checkEqualFields(t *testing.T, got, want ast.FieldList) {
	for _, wf := range got {
		gf := want.ForName(wf.Name)
		if gf == nil {
			t.Fatalf("field %s of definition not found", wf.Name)
		}
		if gf.Type.Name() != wf.Type.Name() ||
			gf.Type.NonNull != wf.Type.NonNull ||
			gf.Type.NamedType != wf.Type.NamedType {
			t.Fatalf("field %s of definition mismatch", wf.Name)
		}
		checkEqualDirectives(t, gf.Directives, wf.Directives)
	}
}

func checkEqualDirectives(t *testing.T, got, want ast.DirectiveList) {
	for _, wd := range want {
		gd := got.ForName(wd.Name)
		if gd == nil {
			t.Fatalf("directive %s of definition not found", wd.Name)
		}
		if len(gd.Arguments) != len(wd.Arguments) {
			t.Fatalf("directive %s of definition mismatch", wd.Name)
		}
		for _, wa := range wd.Arguments {
			ga := gd.Arguments.ForName(wa.Name)
			if ga == nil {
				t.Fatalf("argument %s of directive %s of definition not found", wa.Name, wd.Name)
			}
			if ga.Value.Raw != wa.Value.Raw {
				t.Fatalf("argument %s of directive %s of definition mismatch:\ngot:%s\nwant:%s", wa.Name, wd.Name, wa.Value.Raw, ga.Value.Raw)
			}
		}
	}
}

func printSchemaRef(t *testing.T, schema *openapi3.SchemaRef, level int) {
	if schema == nil {
		return
	}
	prepend := ""
	if level > 0 {
		prepend = strings.Repeat("\t", level)
	}
	if schema.Ref != "" {
		t.Logf("%sSchema: %s", prepend, schema.Ref)
	}
	if schema.Value == nil {
		return
	}
	printSchema(t, schema.Value, level+1)
}

func printSchema(t *testing.T, schema *openapi3.Schema, level int) {
	if schema == nil {
		return
	}
	prepend := ""
	if level > 0 {
		prepend = strings.Repeat("\t", level)
	}
	switch {
	case schema.Type.Is(openapi3.TypeString):
		t.Logf("%sType: string", prepend)
	case schema.Type.Is(openapi3.TypeInteger):
		t.Logf("%sType: integer", prepend)
	case schema.Type.Is(openapi3.TypeNumber):
		t.Logf("%sType: number", prepend)
	case schema.Type.Is(openapi3.TypeBoolean):
		t.Logf("%sType: boolean", prepend)
	case schema.Type.Is(openapi3.TypeArray):
		t.Logf("%sType: array", prepend)
		if schema.Items != nil {
			t.Logf("%sItems:", prepend)
			printSchemaRef(t, schema.Items, level+1)
		}
	case schema.Type.Is(openapi3.TypeObject):
		t.Logf("%sType: object:", prepend)
		if len(schema.Required) != 0 {
			t.Logf("%sRequired: %v", prepend, schema.Required)
		}
		if schema.Properties != nil {
			for name, prop := range schema.Properties {
				t.Logf("%sProperty: %s", prepend, name)
				printSchemaRef(t, prop, level+1)
			}
		}
	case schema.Type.Is(openapi3.TypeNull):
		t.Logf("%sType: null", prepend)
	default:
		t.Logf("%sType unknown: %s", prepend, schema.Type)
	}
}
