package hugr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	adminui "github.com/hugr-lab/query-engine/pkg/admin-ui"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/cache"
	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	datasources "github.com/hugr-lab/query-engine/pkg/data-sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/storage"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/gis"
	permissions "github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/schema"
	schemacatalogs "github.com/hugr-lab/query-engine/pkg/schema/catalogs"
	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/hugr-lab/query-engine/pkg/types"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type Service struct {
	config Config

	router  *http.ServeMux
	adminUI http.HandlerFunc
	catalog schema.Manager
	schema  *schema.Service
	ds      *datasources.Service
	planner *planner.Service
	db      *db.Pool
	perm    permissions.Store
	cache   *cache.Service
	s3      *storage.Source
	gis     *gis.Service
}

type Config struct {
	DB               db.Config
	AdminUI          bool
	AdminUIFetchPath string
	Debug            bool
	Profiling        bool

	AllowParallel      bool
	MaxParallelQueries int
	MaxDepth           int

	CoreDB *coredb.Source
	Auth   *auth.Config
	Cache  cache.Config

	// UseNewCompiler selects the new rule-based compiler and catalog service
	// instead of the legacy catalogs.Service. Default false (legacy).
	UseNewCompiler bool
}

type Info struct {
	AdminUI bool `json:"admin_ui"`
	Debug   bool `json:"debug"`

	AllowParallel      bool `json:"allow_parallel"`
	MaxParallelQueries int  `json:"max_parallel_queries"`
	MaxDepth           int  `json:"max_depth"`

	DuckDB db.Config           `json:"duckdb"`
	CoreDB coredb.Info         `json:"coredb"`
	Auth   []auth.ProviderInfo `json:"auth"`
	Cache  cache.Config        `json:"cache"`
}

func New(config Config) *Service {
	// Initialize schema service with an empty schema (queries won't validate
	// until the first rebuildSchema populates it via SetProvider).
	ss := schema.NewService(static.New(&ast.Schema{
		Types:      make(map[string]*ast.Definition),
		Directives: make(map[string]*ast.DirectiveDefinition),
	}))
	var cat schema.Manager = ss
	if !config.UseNewCompiler {
		cat = schemacatalogs.NewAdapter(catalogs.New(ss))
	}

	return &Service{
		config:  config,
		router:  http.NewServeMux(),
		schema:  ss,
		catalog: cat,
		cache:   cache.New(config.Cache),
		s3:      storage.New(),
	}
}

func (s *Service) Init(ctx context.Context) (err error) {
	s.db, err = db.Connect(ctx, s.config.DB)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	s.ds = datasources.New(s, s.db, s.catalog)
	err = s.ds.RegisterUDF(ctx)
	if err != nil {
		return fmt.Errorf("register udf: %w", err)
	}

	// load core-db runtime data sources
	// if core-db is not provided, it will be created with default config (in-memory)
	if s.config.CoreDB == nil {
		s.config.CoreDB = coredb.New(coredb.Config{})
	}
	err = s.ds.AttachRuntimeSource(ctx, s.config.CoreDB)
	if err != nil {
		return fmt.Errorf("attach runtime source: %w", err)
	}

	// init cache
	err = s.cache.Init(ctx)
	if err != nil {
		return fmt.Errorf("init cache: %w", err)
	}
	err = s.attachRuntimeSources(ctx)
	if err != nil {
		return fmt.Errorf("attach runtime sources: %w", err)
	}

	s.schema.SetVariableTransformer(schema.NewJQVariableTransformer(s))
	s.planner = planner.New(s.catalog, s)

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
	s.perm = permissions.New(s)

	s.gis = gis.New(gis.Config{
		Querier: s,
		Schema:  s.schema,
	})

	s.endpoints()
	if s.config.MaxDepth == 0 {
		s.config.MaxDepth = 7
	}

	// load stored data sources
	if s.config.CoreDB != nil {
		err = s.loadDataSources(ctx)
		if err != nil {
			return fmt.Errorf("load data sources: %w", err)
		}
	}

	err = s.gis.Init()
	if err != nil {
		return fmt.Errorf("init GIS service: %w", err)
	}
	return nil
}

func (s *Service) Info() Info {
	return Info{
		AdminUI:            s.config.AdminUI,
		Debug:              s.config.Debug,
		AllowParallel:      s.config.AllowParallel,
		MaxParallelQueries: s.config.MaxParallelQueries,
		MaxDepth:           s.config.MaxDepth,
		DuckDB:             s.config.DB,
		CoreDB:             s.CoreDBVersion(),
		Auth:               s.config.Auth.Info(),
		Cache:              s.config.Cache,
	}
}

func (s *Service) CoreDBVersion() coredb.Info {
	if s.config.CoreDB == nil {
		return coredb.Info{
			Version: coredb.Version,
		}
	}
	return s.config.CoreDB.Info()
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

func (s *Service) SchemaProvider() schema.Provider {
	return s.schema.Provider()
}

func (s *Service) endpoints() {
	// register http endpoints
	mw := s.middlewares()

	s.router.Handle("/query", mw(http.HandlerFunc(s.queryHandler)))
	s.router.Handle("/jq-query", mw(http.HandlerFunc(s.jqHandler)))
	s.router.Handle("/ipc", mw(http.HandlerFunc(s.ipcHandler)))

	// s.router.Handle("/schema", mw(http.HandlerFunc(s.schemaHandler))) // disabled: schemaHandler blocked on gqlparser requiring *ast.Schema

	if s.config.AdminUI {
		s.router.Handle("/admin", mw(http.HandlerFunc(s.adminUI)))
	}
	if s.config.Profiling {
		s.router.Handle("/debug/profile", mw(http.HandlerFunc(pprof.Profile)))
		s.router.Handle("/debug/pprof/", mw(http.HandlerFunc(pprof.Index)))
		s.router.Handle("/debug/pprof/cmdline", mw(http.HandlerFunc(pprof.Cmdline)))
		s.router.Handle("/debug/pprof/symbol", mw(http.HandlerFunc(pprof.Symbol)))
		s.router.Handle("/debug/pprof/trace", mw(http.HandlerFunc(pprof.Trace)))
		s.router.Handle("/debug/pprof/goroutine", mw(http.HandlerFunc(pprof.Handler("goroutine").ServeHTTP)))
		s.router.Handle("/debug/pprof/heap", mw(http.HandlerFunc(pprof.Handler("heap").ServeHTTP)))
		s.router.Handle("/debug/pprof/block", mw(http.HandlerFunc(pprof.Handler("block").ServeHTTP)))
		s.router.Handle("/debug/pprof/threadcreate", mw(http.HandlerFunc(pprof.Handler("threadcreate").ServeHTTP)))
		s.router.Handle("/debug/pprof/mutex", mw(http.HandlerFunc(pprof.Handler("mutex").ServeHTTP)))
	}

	if s.gis != nil {
		s.router.Handle("/gis/", mw(http.StripPrefix("/gis", s.gis)))
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
	defer res.Close()

	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Service) parseRequest(r *http.Request) (req types.Request, err error) {
	switch r.Method {
	case http.MethodGet:
		query := r.URL.Query()
		req = types.Request{
			Query:         query.Get("query"),
			Variables:     make(map[string]any),
			OperationName: query.Get("operationName"),
			ValidateOnly:  query.Has("validate_only") && query.Get("validate_only") == "true",
		}
		vars := query.Get("variables")
		if vars != "" {
			err = json.Unmarshal([]byte(vars), &req.Variables)
			if err != nil {
				return types.Request{}, fmt.Errorf("unmarshal variables: %w", err)
			}
		}
	case http.MethodPost:
		err = json.NewDecoder(r.Body).Decode(&req)
	default:
		err = fmt.Errorf("unsupported method: %s", r.Method)
	}
	return req, err
}

func (s *Service) ProcessQuery(ctx context.Context, _ string, req types.Request) types.Response {
	start := time.Now()
	op, err := s.schema.ParseQuery(ctx, req.Query, req.Variables, req.OperationName)
	if err != nil {
		return types.ErrResponse(err)
	}
	parseDuration := time.Since(start)

	if req.ValidateOnly {
		ctx = types.ContextWithValidateOnly(ctx)
	}

	provider := s.schema.Provider()
	data, ext, err := s.ProcessOperation(ctx, provider, op)
	if err != nil {
		types.DataClose(data)
		return types.ErrResponse(err)
	}
	var res types.Response
	if data != nil {
		res.Data = data
	}
	if ext != nil {
		if op.Definition.Directives.ForName(base.StatsDirectiveName) != nil {
			opStats, ok := ext["stats"].(map[string]any)
			if !ok {
				opStats = make(map[string]any)
			}
			opStats["parse_time"] = parseDuration.String()
			opStats["total_time"] = time.Since(start).String()
			ext["stats"] = opStats
		}
		res.Extensions = ext
	}
	return res
}

func (s *Service) ProcessOperation(ctx context.Context, provider schema.Provider, op *schema.Operation) (map[string]any, map[string]any, error) {
	switch op.Definition.Operation {
	case ast.Query, ast.Mutation:
		return s.processQuery(ctx, provider, op)
	case ast.Subscription:
		// operation should be handled by another endpoint (websocket)
		return nil, nil, gqlerror.ErrorPosf(op.Definition.Position, "operation %s not supported", op.Definition.Operation)
	default:
		return nil, nil, gqlerror.ErrorPosf(op.Definition.Position, "operation %s not supported", op.Definition.Operation)
	}
}

func (s *Service) Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error) {
	return s.QueryCatalog(ctx, "", query, vars)
}

func (s *Service) QueryCatalog(ctx context.Context, catalog, query string, vars map[string]any) (*types.Response, error) {
	res := s.ProcessQuery(ctx, catalog, types.Request{
		Query:     query,
		Variables: vars,
	})
	if len(res.Errors) > 0 {
		types.DataClose(res.Data)
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
