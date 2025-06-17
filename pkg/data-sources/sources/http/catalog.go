package http

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	openApiTypeExtName  = "x-hugr-type"
	openAPIFieldExtName = "x-hugr-name"
)

func (s *Source) CatalogSource(ctx context.Context, db *db.Pool) (sources.Source, error) {
	return s, nil
}

func (s *Source) SchemaDocument(ctx context.Context) (*ast.SchemaDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.document != nil {
		return s.document, nil
	}
	return s.document, s.reloadSchemaDocument(ctx)
}

func (s *Source) Reload(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reloadSchemaDocument(ctx)
}

func (s *Source) reloadSchemaDocument(ctx context.Context) error {
	if !s.params.hasSpec {
		s.document = &ast.SchemaDocument{}
		return nil
	}
	err := s.loadSpecs(ctx)
	if err != nil {
		return err
	}
	// parse the openAPI specs
	document, err := s.schemaFromSpec()
	if err != nil {
		return err
	}
	s.document = document

	return nil
}

func (s *Source) loadSpecs(ctx context.Context) (err error) {
	sp := s.params
	if !sp.hasSpec {
		return nil
	}
	loader := openapi3.NewLoader()
	loader.Context = ctx
	var spec *openapi3.T
	if sp.isFile {
		loader.ReadFromURIFunc = openapi3.ReadFromURIs(openapi3.ReadFromHTTP(http.DefaultClient), openapi3.ReadFromFile)
		spec, err = loader.LoadFromFile(sp.specPath)
	}
	if !sp.isFile {
		url, err := url.ParseRequestURI(sp.specPath)
		if err != nil {
			return err
		}
		spec, err = loader.LoadFromURI(url)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	if spec == nil {
		return fmt.Errorf("failed to load openAPI specs")
	}
	err = spec.Validate(ctx)
	if err != nil {
		return err
	}
	// parse server URL
	if len(spec.Servers) != 0 && sp.serverURL == "" {
		sp.serverURL = spec.Servers[0].URL
		for _, s := range spec.Servers {
			if s.URL == "" || len(s.Extensions) == 0 ||
				s.Extensions["x-hugr-default"] == nil ||
				s.Extensions["x-hugr-default"] == any(false) {
				continue
			}
			sp.serverURL = s.URL
		}
	}
	if sp.serverURL == "" {
		return fmt.Errorf("no server URL found in the openAPI specs")
	}
	// parse security params
	if sp.securityParams.SchemaName != "" &&
		sp.securityParams.Flows == nil &&
		spec.Components != nil &&
		spec.Components.SecuritySchemes != nil {
		securitySchema, ok := spec.Components.SecuritySchemes[sp.securityParams.SchemaName]
		if !ok || securitySchema == nil {
			return fmt.Errorf("security schema %s not found in the openAPI specs", sp.securityParams.SchemaName)
		}
		ss := securitySchema.Value
		if ss == nil {
			return fmt.Errorf("security schema %s not found in the openAPI specs", sp.securityParams.SchemaName)
		}
		sp.securityParams = httpSecurityParams{
			SchemaName:   sp.securityParams.SchemaName,
			Type:         ss.Type,
			Scheme:       ss.Scheme,
			Name:         ss.Name,
			In:           ss.In,
			FlowName:     sp.securityParams.FlowName,
			ApiKey:       sp.securityParams.ApiKey,
			Username:     sp.securityParams.Username,
			Password:     sp.securityParams.Password,
			ClientID:     sp.securityParams.ClientID,
			ClientSecret: sp.securityParams.ClientSecret,
			Flows:        ss.Flows,
		}
	}
	if err = sp.securityParams.validate(); err != nil {
		return fmt.Errorf("security params validation failed: %v", err)
	}
	s.params = sp
	s.spec = spec

	return nil
}

func (s *Source) schemaFromSpec() (doc *ast.SchemaDocument, err error) {
	if !s.params.hasSpec || s.spec == nil {
		return &ast.SchemaDocument{}, nil
	}
	pos := compiler.CompiledPosName("http-openapi-" + s.ds.Name)
	defs := ast.DefinitionList{}
	var funcs, mutFuncs []openApiFunction
	var gt *ast.Type
	for path, item := range s.spec.Paths.Map() {
	NEXT:
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
				Description:  op.Description,
				OperationId:  op.OperationID,
				Path:         path,
				Method:       method,
				Parameters:   make(map[string]openApiFuncParam),
				RequestBody:  make(map[string]*ast.Type),
				ResponseBody: make(map[string]map[string]*ast.Type),
			}
			// 1.1. parameters
			baseParent := op.OperationID
			if baseParent == "" {
				baseParent = path
			}
			baseParent = openAPINameToGraphQLName(baseParent)
			for _, p := range op.Parameters {
				if p.Value == nil {
					continue NEXT
				}
				parentName := baseParent + "Param" + openAPINameToGraphQLName(p.Value.Name)
				gt, defs, err = openApiSchemaRefToGraphQL(defs, p.Value.Schema, parentName, pos, true)
				if err != nil {
					continue NEXT
				}
				if p.Value != nil && gt != nil && p.Value.Required {
					gt.NonNull = true
				}
				f.Parameters[p.Value.Name] = openApiFuncParam{
					Name:        p.Value.Name,
					In:          p.Value.In,
					Description: p.Value.Description,
					Type:        gt,
				}
			}
			// 1.2. request body
			if op.RequestBody != nil {
				for md, content := range op.RequestBody.Value.Content {
					parentName, ok := typeNameForMediaType(baseParent, md)
					if !ok {
						continue
					}
					parentName += "RequestBody"
					gt, defs, err = openApiSchemaRefToGraphQL(defs, content.Schema, parentName, pos, true)
					if err != nil {
						continue
					}
					f.RequestBody[md] = gt
				}
			}
			// 1.3. responses
			for code, resp := range op.Responses.Map() {
				f.ResponseBody[code] = make(map[string]*ast.Type)
				for md, content := range resp.Value.Content {
					parentName, ok := typeNameForMediaType(baseParent, md)
					if !ok {
						continue
					}
					parentName += "ResponseBody"
					gt, defs, err = openApiSchemaRefToGraphQL(defs, content.Schema, parentName, pos, false)
					if err != nil {
						continue
					}
					f.ResponseBody[code][md] = gt
				}
			}
			switch strings.ToUpper(method) {
			case http.MethodGet:
				funcs = append(funcs, f)
			case http.MethodPost: // can be query or mutation
				mutFuncs = append(mutFuncs, f)
				funcs = append(funcs, f)
			case http.MethodPut, http.MethodPatch, http.MethodDelete:
				mutFuncs = append(mutFuncs, f)
			}
		}
	}

	for _, def := range defs {
		if def.Fields == nil {
			return nil, fmt.Errorf("definition %s has no fields", def.Name)
		}
	}

	if len(funcs) == 0 && len(mutFuncs) == 0 {
		return &ast.SchemaDocument{
			Definitions: defs,
		}, nil
	}

	// 2. add function definitions
	var ext ast.DefinitionList
	ff, err := s.functionFields(defs, funcs, pos)
	if err != nil {
		return nil, err
	}
	if len(ff) != 0 {
		ext = append(ext, &ast.Definition{
			Kind:   ast.Object,
			Name:   base.FunctionTypeName,
			Fields: ff,
		})
	}
	ff, err = s.functionFields(defs, mutFuncs, pos)
	if err != nil {
		return nil, err
	}
	if len(ff) != 0 {
		ext = append(ext, &ast.Definition{
			Kind:   ast.Object,
			Name:   base.FunctionMutationTypeName,
			Fields: ff,
		})
	}

	return &ast.SchemaDocument{
		Definitions: defs,
		Extensions:  ext,
	}, nil
}

func (s *Source) functionFields(defs ast.DefinitionList, funcs []openApiFunction, pos *ast.Position) ([]*ast.FieldDefinition, error) {
	var fields []*ast.FieldDefinition
	for _, f := range funcs {
		var resType, reqBody *ast.Type
		for _, resType = range f.ResponseBody["200"] {
			break
		}
		if resType == nil && f.ResponseBody["default"] == nil {
			continue
		}
		for _, reqBody = range f.RequestBody {
			break
		}

		// arguments
		var params, headers []string
		path := f.Path
		var args ast.ArgumentDefinitionList
		for _, p := range f.Parameters {
			args = append(args, &ast.ArgumentDefinition{
				Name:        p.Name,
				Description: p.Description,
				Type:        p.Type,
				Position:    pos,
			})
			sql := "[" + p.Name + "]"
			if p.Type.NamedType == compiler.GeometryTypeName {
				sql = fmt.Sprintf("ST_AsGeoJSON(%s)", sql)
			}
			switch p.In {
			case "query":
				params = append(params, fmt.Sprintf("%s: %s", p.Name, sql))
			case "header":
				headers = append(headers, fmt.Sprintf("%s: %s", p.Name, sql))
			case "path":
				path = strings.ReplaceAll(path, "{"+p.Name+"}", fmt.Sprintf("'||%s||'", sql))
			}
		}
		path = "'" + path + "'"
		if strings.HasSuffix(path, "||''") {
			path = strings.TrimRight(path, "|'")
		}
		bSQL := ""
		if reqBody != nil && (len(args) != 0 ||
			reqBody.NamedType == "" ||
			reqBody.NamedType == compiler.JSONTypeName) {
			args = append(args, &ast.ArgumentDefinition{
				Name:        "request_body",
				Description: "request body",
				Type:        reqBody,
				Position:    pos,
			})
			bSQL = "COALESCE([request_body]::JSON, '{}'::JSON)"
		}
		if reqBody != nil && len(args) == 0 {
			def := defs.ForName(reqBody.Name())
			if def == nil {
				return nil, fmt.Errorf("request body type %s not found", reqBody.Name())
			}
			var bodyStruct []string
			for _, f := range def.Fields {
				args = append(args, &ast.ArgumentDefinition{
					Name:        f.Name,
					Description: f.Description,
					Type:        f.Type,
					Position:    pos,
				})
				sql := "[" + f.Name + "]"
				bodyStruct = append(bodyStruct, fmt.Sprintf("%s: %s", f.Name, sql))
			}
			if len(bodyStruct) != 0 {
				bSQL = fmt.Sprintf("{%s}::JSON", strings.Join(bodyStruct, ", "))
			}
		}
		pSQL := strings.Join(params, ",")
		if pSQL != "" {
			pSQL = "{" + pSQL + "}"
		}
		if pSQL == "" {
			pSQL = "'{}'"
		}
		hSQL := strings.Join(headers, ",")
		if hSQL != "" {
			hSQL = "{" + hSQL + "}"
		}
		if hSQL == "" {
			hSQL = "'{}'"
		}
		if bSQL == "" {
			bSQL = "'{}'::JSON"
		}

		sql := "http_data_source_request_scalar([$catalog], %s, '%s', %s::JSON, %s::JSON, %s, '')"
		sql = fmt.Sprintf(sql,
			path,
			strings.ToUpper(f.Method),
			hSQL,
			pSQL,
			bSQL,
		)
		// directive
		def := compiler.NewFunction("", f.Name, sql, resType, false, true, args, pos)
		fields = append(fields, def)
	}
	return fields, nil
}

type openApiFuncParam struct {
	Name        string
	In          string
	Description string
	Type        *ast.Type
}
type openApiFunction struct {
	Name         string
	Description  string
	OperationId  string
	Path         string
	Method       string
	Parameters   map[string]openApiFuncParam
	RequestBody  map[string]*ast.Type
	ResponseBody map[string]map[string]*ast.Type
}

var mediaTypeNameMap = map[string]string{
	"application/json":   "",
	"application/*+json": "",
	"text/json":          "",
}

func typeNameForMediaType(parentName, mediaType string) (string, bool) {
	name, ok := mediaTypeNameMap[mediaType]
	if !ok {
		return "", false
	}
	if name == "" {
		return parentName, true
	}
	return parentName + "_" + name, true
}

func openApiSchemaRefToGraphQL(defs ast.DefinitionList, t *openapi3.SchemaRef, parentName string, pos *ast.Position, asInput bool) (*ast.Type, ast.DefinitionList, error) {
	if t == nil {
		return ast.NamedType("JSON", pos), defs, nil
	}
	if t.Ref != "" {
		parentName = openAPINameToGraphQLName(t.Ref)
		if asInput {
			parentName += "Input"
		}
		if def := defs.ForName(parentName); def != nil {
			return ast.NamedType(parentName, pos), defs, nil
		}
	}
	if t.Value == nil {
		return ast.NamedType("JSON", pos), defs, nil
	}
	return openAPISchemaToGraphQL(defs, t.Value, parentName, pos, asInput)
}

func openAPISchemaToGraphQL(defs ast.DefinitionList, t *openapi3.Schema, parentName string, pos *ast.Position, asInput bool) (*ast.Type, ast.DefinitionList, error) {
	ext, err := openApiTypeExtension(t)
	if err != nil {
		return nil, nil, err
	}
	switch {
	case ext != nil && !asInput:
		return ast.NamedType(ext.TypeName, pos), defs, nil
	case t.Type.Is(openapi3.TypeString):
		return ast.NamedType("String", pos), defs, nil
	case t.Type.Is(openapi3.TypeInteger):
		return ast.NamedType("Int", pos), defs, nil
	case t.Type.Is(openapi3.TypeNumber):
		return ast.NamedType("Float", pos), defs, nil
	case t.Type.Is(openapi3.TypeBoolean):
		return ast.NamedType("Boolean", pos), defs, nil
	case t.Type.Is(openapi3.TypeArray):
		parentName += "_item"
		nt, dd, err := openApiSchemaRefToGraphQL(defs, t.Items, parentName, pos, asInput)
		if err != nil {
			return nil, nil, err
		}
		defs = dd
		return ast.ListType(nt, pos), defs, nil
	case t.Type.Is(openapi3.TypeObject):
		if len(t.Properties) == 0 {
			return ast.NamedType("JSON", pos), defs, nil
		}
		// create object with properties definition
		def := &ast.Definition{
			Kind:        ast.Object,
			Name:        parentName,
			Description: t.Description,
			Position:    pos,
		}
		if asInput {
			def.Kind = ast.InputObject
		}
		defs = append(defs, def)
		for name, prop := range t.Properties {
			nt, dd, err := openApiSchemaRefToGraphQL(defs, prop, parentName+"_"+openAPINameToGraphQLName(name), pos, asInput)
			if err != nil {
				return nil, nil, err
			}
			if slices.Contains(t.Required, name) {
				nt.NonNull = true
			}
			f := &ast.FieldDefinition{
				Name:     name,
				Type:     nt,
				Position: pos,
			}
			if prop.Value != nil {
				f.Description = prop.Value.Description
				// field source directive if exists
				if prop.Value.Extensions != nil {
					if fn := prop.Value.Extensions[openAPIFieldExtName]; fn != nil {
						fn, ok := fn.(string)
						if !ok {
							return nil, nil, fmt.Errorf("invalid field name extension %T for %s", fn, name)
						}
						if fn != "" && fn != name {
							f.Name = fn
							f.Directives = append(f.Directives, base.FieldSourceDirective(name))
						}
					}
				}
				ext, err := openApiTypeExtension(prop.Value)
				if err != nil {
					return nil, nil, err
				}
				if ext != nil {
					if dd := ext.FieldDirectives(name); len(dd) != 0 {
						f.Directives = append(f.Directives, dd...)
					}
				}
			}
			def.Fields = append(def.Fields, f)
			defs = dd
		}
		return ast.NamedType(parentName, pos), defs, nil
	default:
		return ast.NamedType("JSON", pos), defs, nil
	}
}

func openAPINameToGraphQLFuncName(name string) string {
	name = strings.TrimPrefix(name, "#/components/schemas/")
	name = strings.TrimPrefix(name, "/")
	out := make([]rune, 0, len(name))
	na := []rune(name)
	for i, r := range na {
		switch {
		case r == ' ' || r == '.' || r == '/' || r == '\\' || r == ':' || r == '-' || r == '{' || r == '}':
			out = append(out, '_')
		case isUpperRune(r):
			if i > 0 && !isUpperRune(na[i+1]) {
				out = append(out, '_')
			}
			r = r + 32
			out = append(out, r)
		case !checkRune(r):
			out = append(out, 'X')
		default:
			out = append(out, r)
		}
	}
	return string(out)
}

func openAPINameToGraphQLName(name string) string {
	name = strings.TrimPrefix(name, "#/components/schemas/")
	name = strings.TrimPrefix(name, "/")
	out := make([]rune, 0, len(name))
	skipped := false
	for _, r := range name {
		switch {
		case r == ' ' || r == '.' || r == '/' || r == '\\' || r == ':' || r == '{' || r == '}':
			skipped = true
			continue
		case isUpperRune(r):
			out = append(out, r)
			skipped = false
		case skipped:
			out = append(out, r-32)
		case !checkRune(r):
			out = append(out, 'X')
		default:
			out = append(out, r)
		}
	}
	return string(out)
}

func isUpperRune(r rune) bool {
	return r >= 'A' && r <= 'Z'
}
func checkRune(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
}

type openApiTypeExt struct {
	TypeName      string
	TransformName string
	GeometryInfo  struct {
		GeometryType string
		SRID         int
	}
}

func (t *openApiTypeExt) FieldDirectives(name string) []*ast.Directive {
	var dd ast.DirectiveList
	switch {
	case t.TypeName == compiler.TimestampTypeName:
		sql := "[" + name + "]"
		switch t.TransformName {
		case "FromUnixTime":
			dd = append(dd, base.FieldSqlDirective(fmt.Sprintf("TO_TIMESTAMP(try_cast(%s AS BIGINT))", sql)))
		default:
			dd = append(dd, base.FieldSqlDirective(fmt.Sprintf("try_cast(%s AS VARCHAR)::TIMESTAMP_TZ", sql)))
		}
	case t.TypeName == compiler.GeometryTypeName:
		if t.GeometryInfo.GeometryType != "" && t.GeometryInfo.SRID != 0 {
			dd = append(dd, base.FieldGeometryInfoDirective(t.GeometryInfo.GeometryType, t.GeometryInfo.SRID))
		}
		switch t.TransformName {
		case "StringAsGeoJson":
			dd = append(dd, base.FieldSqlDirective(fmt.Sprintf("ST_GeomFromGeoJSON([%s])", name)))
		}
	}
	return dd
}

func openApiTypeExtension(t *openapi3.Schema) (*openApiTypeExt, error) {
	if t.Extensions == nil {
		return nil, nil
	}
	ext := openApiTypeExt{}
	et, ok := t.Extensions[openApiTypeExtName]
	if !ok || et == nil {
		return nil, nil // no extension found
	}
	etm, ok := et.(map[string]any)
	if !ok || etm == nil {
		return nil, fmt.Errorf("invalid extension type %T", et)
	}
	etName, ok := etm["type"]
	if !ok {
		return nil, fmt.Errorf("extension type name not found")
	}
	ext.TypeName, ok = etName.(string)
	if !ok {
		return nil, fmt.Errorf("invalid type extension type")
	}
	if v, ok := etm["transform"]; ok {
		ext.TransformName, ok = v.(string)
		if !ok {
			return nil, fmt.Errorf("invalid extension transform")
		}
	}
	if v, ok := etm["geometry_info"]; ok {
		vm, ok := v.(map[string]any)
		if !ok || vm == nil {
			return nil, fmt.Errorf("invalid extension geometry_info")
		}
		if v, ok := vm["geometry_type"]; ok {
			ext.GeometryInfo.GeometryType, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("invalid extension geometry_type")
			}
		}
		if v, ok := vm["srid"]; ok {
			fi, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("invalid extension srid")
			}
			ext.GeometryInfo.SRID = int(fi)
		}
	}
	return &ext, nil
}
