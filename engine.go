package hugr

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	adminui "github.com/hugr-lab/query-engine/pkg/admin-ui"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/cache"
	"github.com/hugr-lab/query-engine/pkg/cluster"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	catalogdb "github.com/hugr-lab/query-engine/pkg/catalog/db"
	datasources "github.com/hugr-lab/query-engine/pkg/data-sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/embedding"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/storage"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/gis"
	mcpserver "github.com/hugr-lab/query-engine/pkg/mcp"
	permissions "github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/types"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type Service struct {
	config Config

	router   *http.ServeMux
	adminUI  http.HandlerFunc
	catalog  catalog.Manager
	schema   *catalog.Service
	ds       *datasources.Service
	planner  *planner.Service
	db       *db.Pool
	perm     permissions.Store
	cache    *cache.Service
	s3       *storage.Source
	gis      *gis.Service
	embedder *embedding.Source
	cluster  *cluster.Source

	dbProvider *catalogdb.Provider

	pendingSources []sources.RuntimeSource
	initialized    bool
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

	SchemaCacheMaxEntries int           // LRU cache max entries (0 = default 10000)
	SchemaCacheTTL        time.Duration // LRU cache TTL (0 = default 10m)

	MCPEnabled bool // Enable MCP endpoint on /mcp

	CoreDB   *coredb.Source
	Auth     *auth.Config
	Cache    cache.Config
	Embedder EmbedderConfig
	Cluster  cluster.ClusterConfig
}

type EmbedderConfig struct {
	URL        string // Full URL with query params: model, api_key, api_key_header, timeout
	VectorSize int    // Optional vector size override (default: 0 - no vectorization)
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

func New(config Config) (*Service, error) {
	return &Service{
		config: config,
		router: http.NewServeMux(),
		cache:  cache.New(config.Cache),
		s3:     storage.New(),
	}, nil
}

func (s *Service) Init(ctx context.Context) (err error) {
	// 0. Validate MCP config — fail fast before expensive operations.
	if s.config.MCPEnabled && s.config.Embedder.URL == "" {
		return fmt.Errorf("MCP endpoint requires system embedder (EMBEDDER_URL)")
	}

	// 1. Connect to DuckDB.
	s.db, err = db.Connect(ctx, s.config.DB)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}

	// 2. Ensure CoreDB source exists (default: in-memory).
	if s.config.CoreDB == nil {
		s.config.CoreDB = coredb.New(coredb.Config{})
	}

	// 3. Attach CoreDB early — creates _schema_* tables on first start.
	//    The DB must exist before creating db.Provider.
	err = s.config.CoreDB.Attach(ctx, s.db)
	if err != nil {
		return fmt.Errorf("attach core db: %w", err)
	}

	// 3b. Create system embedder from config (if EMBEDDER_URL set).
	var embedder catalogdb.Embedder
	if s.config.Embedder.URL != "" {
		src, err := embedding.New(types.DataSource{
			Name: "_system_embedder",
			Type: sources.Embedding,
			Path: s.config.Embedder.URL,
		}, false)
		if err != nil {
			return fmt.Errorf("create system embedder: %w", err)
		}
		if err := src.Attach(ctx, s.db); err != nil {
			return fmt.Errorf("attach system embedder: %w", err)
		}
		s.embedder = src
		embedder = src
	}

	// 4. Create db.Provider with compiler for CatalogManager support.
	isPostgres := s.config.CoreDB.Info().Type == sources.Postgres
	tablePrefix := "core."
	c := compiler.New(compiler.GlobalRules()...)
	cacheConfig := catalogdb.CacheConfig{
		MaxEntries: s.config.SchemaCacheMaxEntries,
		TTL:        s.config.SchemaCacheTTL,
	}
	if cacheConfig.MaxEntries == 0 && cacheConfig.TTL == 0 {
		cacheConfig = catalogdb.DefaultCacheConfig()
	}
	isReadonly := s.config.CoreDB.IsReadonly()
	dbProvider, err := catalogdb.NewWithCompiler(ctx, s.db, catalogdb.Config{
		TablePrefix: tablePrefix,
		Cache:       cacheConfig,
		IsPostgres:  isPostgres,
		IsReadonly:  isReadonly,
		VecSize:     s.config.Embedder.VectorSize,
	}, embedder, c)
	if err != nil {
		return fmt.Errorf("create db provider: %w", err)
	}

	s.dbProvider = dbProvider

	// 5. Persist system types (version-checked, skips if unchanged on restart).
	//    Skip in read-only mode — system types were persisted by the writer node.
	if !isReadonly {
		err = dbProvider.InitSystemTypes(ctx)
		if err != nil {
			return fmt.Errorf("init system types: %w", err)
		}
	}

	// 6. Create catalog.Service (auto-detects CatalogManager on dbProvider).
	ss := catalog.NewService(dbProvider)
	s.schema = ss
	s.catalog = ss

	// 6b. Register schema management UDFs (description updates, hard remove, reindex).
	if err := dbProvider.RegisterUDFs(ctx, ss); err != nil {
		return fmt.Errorf("register schema UDFs: %w", err)
	}

	// 7. Create datasources service and register UDFs.
	s.ds = datasources.New(s, s.db, s.catalog)
	// In read-only or cluster worker mode, skip schema DB writes
	// on Attach/Detach — schemas are managed by the writer/management node.
	if isReadonly || s.config.Cluster.IsWorker() {
		s.ds.SetSkipCatalogOps(true)
	}
	if !isReadonly {
		err = s.ds.RegisterUDF(ctx)
		if err != nil {
			return fmt.Errorf("register udf: %w", err)
		}
	}

	// 7b. Register system embedder as data-source.
	if s.embedder != nil {
		if err := s.ds.Register(ctx, "_system_embedder", s.embedder); err != nil {
			return fmt.Errorf("register system embedder: %w", err)
		}
	}

	// 8. Attach CoreDB runtime source — Attach is idempotent (skips since
	//    already attached), but compiles CoreDB's GraphQL schema into the
	//    catalog via AddCatalog (version-checked, skips if unchanged).
	//    Skip in read-only mode — schema was compiled by the writer node.
	if !isReadonly {
		err = s.ds.AttachRuntimeSource(ctx, s.config.CoreDB)
		if err != nil {
			return fmt.Errorf("attach core db catalog: %w", err)
		}
	} else {
		// Schema already persisted by writer; just register engine for routing.
		s.schema.RegisterEngine(s.config.CoreDB.Name(), s.config.CoreDB.Engine())
	}

	// 9. Init cache.
	err = s.cache.Init(ctx)
	if err != nil {
		return fmt.Errorf("init cache: %w", err)
	}

	// 10. Attach other runtime sources.
	err = s.attachRuntimeSources(ctx, isReadonly)
	if err != nil {
		return fmt.Errorf("attach runtime sources: %w", err)
	}

	// 11. Process pending runtime sources registered before Init().
	for _, source := range s.pendingSources {
		if isReadonly {
			if err := s.attachRuntimeSourceReadonly(ctx, source); err != nil {
				return fmt.Errorf("attach pending runtime source %q: %w", source.Name(), err)
			}
		} else {
			if err := s.ds.AttachRuntimeSource(ctx, source); err != nil {
				return fmt.Errorf("attach pending runtime source %q: %w", source.Name(), err)
			}
		}
	}
	s.pendingSources = nil
	s.initialized = true

	// 12. Setup services.
	s.schema.SetVariableTransformer(catalog.NewJQVariableTransformer(s))
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

	// 13. Clean orphaned catalogs (standalone/management mode only).
	// All runtime catalogs are now registered in dbProvider.catalogs;
	// only data-source catalogs missing from the data_sources table are removed.
	// Skip on read-only CoreDB and cluster workers (management handles cleanup).
	if !isReadonly && !s.config.Cluster.IsWorker() {
		if err := s.dbProvider.CleanOrphanedCatalogs(ctx); err != nil {
			slog.Error("failed to clean orphaned catalogs", "error", err)
		}
	}

	// 14. Load stored data sources.
	err = s.loadDataSources(ctx)
	if err != nil {
		return fmt.Errorf("load data sources: %w", err)
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
	if s.initialized {
		return fmt.Errorf("engine already initialized: AttachRuntimeSource must be called before Init()")
	}
	s.pendingSources = append(s.pendingSources, source)
	return nil
}

// ClusterSource returns the cluster source, or nil if cluster mode is disabled.
func (s *Service) ClusterSource() *cluster.Source {
	return s.cluster
}

func (s *Service) Close() error {
	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

func (s *Service) SchemaProvider() catalog.Provider {
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

	if s.config.MCPEnabled {
		mcpSrv := mcpserver.New(s, s.config.Debug)
		s.router.Handle("/mcp", mw(mcpSrv.Handler()))
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Service) queryHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	req, err := s.parseRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(types.ErrResponse(err))
		return
	}

	res := s.ProcessQuery(r.Context(), req)
	defer res.Close()

	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(types.ErrResponse(err))
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

func (s *Service) ProcessQuery(ctx context.Context, req types.Request) types.Response {
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

func (s *Service) ProcessOperation(ctx context.Context, provider catalog.Provider, op *catalog.Operation) (map[string]any, map[string]any, error) {
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
	res := s.ProcessQuery(ctx, types.Request{
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
