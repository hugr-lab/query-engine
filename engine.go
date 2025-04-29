package hugr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	adminui "github.com/hugr-lab/query-engine/pkg/admin-ui"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/cache"
	"github.com/hugr-lab/query-engine/pkg/catalogs"
	datasources "github.com/hugr-lab/query-engine/pkg/data-sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/storage"
	"github.com/hugr-lab/query-engine/pkg/db"
	permissions "github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/types"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/validator"
	"github.com/vektah/gqlparser/v2/validator/rules"
	"golang.org/x/sync/errgroup"
)

func init() {
	// remove unused variable GraphQL validation rule
	validator.RemoveRule(rules.NoUnusedVariablesRule.Name)
}

type Request struct {
	Query         string                 `json:"query"`
	Variables     map[string]interface{} `json:"variables"`
	OperationName string                 `json:"operationName,omitempty"`
}

type Service struct {
	config Config

	router  *http.ServeMux
	adminUI http.HandlerFunc
	catalog *catalogs.Service
	ds      *datasources.Service
	planner *planner.Service
	db      *db.Pool
	perm    permissions.Store
	cache   *cache.Service
	s3      *storage.Source
}

type Config struct {
	DB               db.Config
	AdminUI          bool
	AdminUIFetchPath string
	Debug            bool

	AllowParallel      bool
	MaxParallelQueries int
	MaxDepth           int

	CoreDB *coredb.Source
	Auth   *auth.Config
	Cache  cache.Config
}

func New(config Config) *Service {
	return &Service{
		config:  config,
		router:  http.NewServeMux(),
		catalog: catalogs.New(),
		cache:   cache.New(config.Cache),
		s3:      storage.New(),
	}
}

func (s *Service) Init(ctx context.Context) (err error) {
	s.db, err = db.Connect(ctx, s.config.DB)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	s.ds = datasources.New(s.db, s.catalog)
	err = s.ds.RegisterUDF(ctx)
	if err != nil {
		return fmt.Errorf("register udf: %w", err)
	}

	// load core-db runtime data sources
	// if core-db is not provided, it will be created with default config (in-memory)
	cdb := s.config.CoreDB
	if cdb == nil {
		cdb = coredb.New(coredb.Config{})
	}
	err = s.ds.AttachRuntimeSource(ctx, cdb)
	if err != nil {
		return fmt.Errorf("attach runtime source: %w", err)
	}

	// init cache
	err = s.cache.Init(ctx)
	if err != nil {
		return fmt.Errorf("init cache: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, s.cache)
	if err != nil {
		return fmt.Errorf("attach cache source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, s.s3)
	if err != nil {
		return fmt.Errorf("attach s3 source: %w", err)
	}

	s.planner = planner.New(s.catalog)

	if s.config.AdminUIFetchPath == "" {
		s.config.AdminUIFetchPath = "/query"
	}

	if s.config.AdminUI {
		s.adminUI, err = adminui.AdminUIHandler(adminui.Config{
			Path: s.config.AdminUIFetchPath,
		})
		if err != nil {
			return err
		}
	}

	s.endpoints()
	if s.config.MaxDepth == 0 {
		s.config.MaxDepth = 7
	}

	s.perm = permissions.New(s)

	// load stored data sources
	if s.config.CoreDB == nil {
		return nil
	}

	return s.loadDataSources(ctx)
}

func (s *Service) AttachRuntimeSource(ctx context.Context, source sources.RuntimeSource) error {
	return s.ds.AttachRuntimeSource(ctx, source)
}

func (s *Service) Close() error {
	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

func (s *Service) Schema() *ast.Schema {
	return s.catalog.Schema()
}

func (s *Service) endpoints() {
	// register http endpoints
	mw := s.middlewares()

	s.router.Handle("/query", mw(http.HandlerFunc(s.queryHandler)))
	s.router.Handle("/jq-query", mw(http.HandlerFunc(s.jqHandler)))
	s.router.Handle("/ipc", mw(http.HandlerFunc(s.ipcHandler)))
	s.router.Handle("/{catalog}/query", mw(http.HandlerFunc(s.queryHandler)))

	if s.config.AdminUI {
		s.router.Handle("/admin", mw(http.HandlerFunc(s.adminUI)))
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Service) queryHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	req, err := s.parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	catalog := r.URL.Query().Get("catalog")

	res := s.ProcessQuery(r.Context(), catalog, req)

	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Service) parseRequest(r *http.Request) (req Request, err error) {
	switch r.Method {
	case http.MethodGet:
		query := r.URL.Query()
		req = Request{
			Query:         query.Get("query"),
			Variables:     make(map[string]any),
			OperationName: query.Get("operationName"),
		}
		vars := query.Get("variables")
		if vars != "" {
			err = json.Unmarshal([]byte(vars), &req.Variables)
			if err != nil {
				return Request{}, fmt.Errorf("unmarshal variables: %w", err)
			}
		}
	case http.MethodPost:
		err = json.NewDecoder(r.Body).Decode(&req)
	default:
		err = fmt.Errorf("unsupported method: %s", r.Method)
	}
	return req, err
}

func (s *Service) ProcessQuery(ctx context.Context, catalog string, req Request) types.Response {
	// add permissions to context
	ctx, err := s.perm.ContextWithPermissions(ctx)
	if err != nil {
		return types.ErrResponse(err)
	}
	schema, err := s.catalog.CatalogSchema(catalog)
	if err != nil {
		return types.ErrResponse(err)
	}

	qd, errs := gqlparser.LoadQuery(schema, req.Query)
	if len(errs) > 0 {
		return types.ErrResponse(errs)
	}

	if len(qd.Operations) == 0 {
		return types.Response{Errors: gqlerror.List{gqlerror.Errorf("no operations found")}}
	}

	var res types.Response
	if len(qd.Operations) == 1 {
		data, ext, err := s.processOperation(ctx, schema, qd.Operations[0], req.Variables)
		if err != nil {
			return types.ErrResponse(err)
		}
		if data != nil {
			res.Data = data
		}
		if ext != nil {
			res.Extensions = ext
		}
		return res
	}

	data := make(map[string]any, len(qd.Operations))
	extensions := make(map[string]any)
	for _, op := range qd.Operations {
		data[op.Name] = nil
		extensions[op.Name] = nil
	}
	eg, ctx := errgroup.WithContext(ctx)
	for _, op := range qd.Operations {
		op := op
		eg.Go(func() error {
			d, ext, err := s.processOperation(ctx, schema, op, req.Variables)
			data[op.Name] = d
			if ext != nil {
				extensions[op.Name] = ext
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		return types.ErrResponse(err)
	}
	res.Data = data
	res.Extensions = extensions
	return res
}

func (s *Service) processOperation(ctx context.Context, schema *ast.Schema, op *ast.OperationDefinition, vars map[string]any) (map[string]any, map[string]any, error) {
	switch op.Operation {
	case ast.Query, ast.Mutation:
		return s.processQuery(ctx, schema, op, vars)
	case ast.Subscription:
		// operation should be handled by another endpoint (websocket)
		return nil, nil, gqlerror.ErrorPosf(op.Position, "operation %s not supported", op.Operation)
	default:
		return nil, nil, gqlerror.ErrorPosf(op.Position, "operation %s not supported", op.Operation)
	}
}

func (s *Service) Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error) {
	return s.QueryCatalog(ctx, "", query, vars)
}

func (s *Service) QueryCatalog(ctx context.Context, catalog, query string, vars map[string]any) (*types.Response, error) {
	res := s.ProcessQuery(ctx, catalog, Request{
		Query:     query,
		Variables: vars,
	})
	if len(res.Errors) > 0 {
		return nil, res.Errors
	}
	return &res, nil
}

func (s *Service) ContextWithTx(ctx context.Context) (context.Context, error) {
	return s.db.WithTx(ctx)
}

func (s *Service) Commit(ctx context.Context) error {
	return s.db.Commit(ctx)
}

func (s *Service) Rollback(ctx context.Context) error {
	return s.db.Rollback(ctx)
}
