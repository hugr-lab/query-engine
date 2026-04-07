package datasources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/airport"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/duckdb"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/ducklake"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/embedding"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/extension"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/http"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/hugrapp"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/iceberg"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/llm"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/mssql"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/mysql"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/postgres"
	redissrc "github.com/hugr-lab/query-engine/pkg/data-sources/sources/redis"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/hugr-lab/query-engine/types"

	//lint:ignore ST1001 "github.com/hugr-lab/query-engine/pkg/data-sources/sources" is a valid package name
	. "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

type Service struct {
	mu          sync.RWMutex
	dataSources map[string]Source

	// runtimeSources tracks attached runtime sources by name for subscription
	// and other lookups that need the source instance (e.g. SubscriptionSource).
	runtimeSources map[string]RuntimeSource

	db       *db.Pool
	qe       types.Querier
	catalogs catalog.Manager

	// skipCatalogOps disables schema DB writes (AddCatalog/RemoveCatalog)
	// on Attach/Detach. Set for read-only CoreDB and cluster worker nodes
	// where schema state is managed by the writer/management node.
	skipCatalogOps   bool
	embedderSettings EmbedderSettings
	heartbeatConfig  HeartbeatConfig
}

type EmbedderSettings struct {
	IsEnabled  bool
	Name       string
	Model      string
	Dimensions int
}

func New(qe types.Querier, db *db.Pool, cs catalog.Manager, embederSettings EmbedderSettings, hbConfig ...HeartbeatConfig) *Service {
	var cfg HeartbeatConfig
	if len(hbConfig) > 0 {
		cfg = hbConfig[0]
	}
	return &Service{
		dataSources:      make(map[string]Source),
		runtimeSources:   make(map[string]RuntimeSource),
		catalogs:         cs,
		db:               db,
		qe:               qe,
		embedderSettings: embederSettings,
		heartbeatConfig:  cfg,
	}
}

// SetSkipCatalogOps disables schema DB writes on Attach/Detach.
// Used for read-only CoreDB and cluster worker nodes.
func (s *Service) SetSkipCatalogOps(skip bool) {
	s.skipCatalogOps = skip
}

func (s *Service) AttachRuntimeSource(ctx context.Context, source RuntimeSource) error {
	if sq, ok := source.(RuntimeSourceQuerier); ok {
		sq.QueryEngineSetup(s.qe)
	}
	if du, ok := source.(RuntimeSourceDataSourceUser); ok {
		du.DataSourceServiceSetup(s)
	}

	err := source.Attach(ctx, s.db)
	if err != nil && !errors.Is(err, ErrDataSourceAttached) {
		return err
	}

	// Track runtime source for subscription and other lookups.
	s.mu.Lock()
	s.runtimeSources[source.Name()] = source
	s.mu.Unlock()

	cat, err := source.Catalog(ctx)
	if err != nil {
		return err
	}
	return s.catalogs.AddCatalog(ctx, source.Name(), cat)
}

func (s *Service) Engine(name string) (engines.Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return nil, ErrDataSourceNotFound
	}
	return ds.Engine(), nil
}

func (s *Service) Register(ctx context.Context, name string, ds Source) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.dataSources[name]
	if ok {
		return ErrDataSourceExists
	}
	s.dataSources[name] = ds
	return nil
}

func (s *Service) Unregister(ctx context.Context, name string) error {
	isAttached := s.IsAttached(name)
	if isAttached {
		return ErrDataSourceAttached
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.dataSources[name]
	if !ok {
		return ErrDataSourceNotFound
	}

	delete(s.dataSources, name)

	return nil
}

func (s *Service) Attach(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, ok := s.dataSources[name]
	if !ok {
		return ErrDataSourceNotFound
	}

	if ds.IsAttached() {
		return ErrDataSourceAttached
	}

	err := ds.Attach(ctx, s.db)
	if err != nil {
		return err
	}

	// Inject resolver for sources that need access to other data sources (e.g., rate limiting).
	if du, ok := ds.(SourceDataSourceUser); ok {
		du.SetDataSourceResolver(s)
	}

	// Provision external resources (app databases) if source supports it.
	// Unlock mutex during provisioning — it makes GraphQL queries via Querier
	// which may need to access the data source service (deadlock otherwise).
	if p, ok := ds.(Provisioner); ok && s.qe != nil {
		s.mu.Unlock()
		provErr := p.Provision(ctx, s.qe)
		s.mu.Lock()
		if provErr != nil {
			slog.Error("provisioning failed", "source", name, "error", provErr)
			if err := ds.Detach(ctx, s.db); err != nil {
				slog.Error("failed to rollback attach after provisioning failure", "source", name, "error", err)
			}
			return fmt.Errorf("provisioning failed: %w", provErr)
		}
	}

	// Skip schema compilation in readonly/worker mode — schemas
	// are already persisted by the writer/management node.
	if s.skipCatalogOps {
		s.catalogs.RegisterEngine(name, ds.Engine())
		return nil
	}

	if e, ok := ds.(ExtensionSource); ok && e.IsExtension() {
		cat, err := s.catalogSource(ctx, ds, false)
		if err != nil {
			return err
		}
		def := ds.Definition()
		return s.catalogs.AddCatalog(ctx, def.Name, cat)
	}

	cat, err := s.catalogSource(ctx, ds, false)
	if err != nil {
		return err
	}
	if cat == nil {
		return nil
	}
	def := ds.Definition()
	if err := s.catalogs.AddCatalog(ctx, def.Name, cat); err != nil {
		return err
	}

	// Start heartbeat if the source supports it.
	if hb, ok := ds.(Heartbeater); ok {
		name := ds.Name()
		hb.StartHeartbeat(s.heartbeatConfig,
			func(ctx context.Context, n string) error {
				return s.catalogs.SuspendCatalog(ctx, n)
			},
			func(ctx context.Context, n string) error {
				cat, err := s.catalogSource(ctx, ds, false)
				if err != nil {
					return err
				}
				if cat == nil {
					return nil
				}
				if err := s.catalogs.ReactivateCatalog(ctx, n, cat); err != nil {
					return err
				}
				if p, ok := ds.(Provisioner); ok && s.qe != nil {
					if err := p.Provision(ctx, s.qe); err != nil {
						slog.Error("re-provision after recovery failed", "app", name, "error", err)
					}
				}
				return nil
			},
		)
	}

	return nil
}

func (s *Service) Detach(ctx context.Context, name string, db *db.Pool, hard bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return ErrDataSourceNotFound
	}

	if !ds.IsAttached() {
		return ErrDataSourceNotAttached
	}

	// Stop heartbeat if the source supports it.
	if hb, ok := ds.(Heartbeater); ok {
		hb.StopHeartbeat()
	}

	// Skip schema removal in readonly/worker mode.
	if !s.skipCatalogOps {
		var err error
		if hard {
			err = s.catalogs.RemoveCatalog(ctx, name)
		} else {
			err = s.catalogs.SuspendCatalog(ctx, name)
		}
		if !errors.Is(err, catalog.ErrCatalogNotFound) && err != nil {
			return err
		}
	}

	return ds.Detach(ctx, db)
}

func (s *Service) IsAttached(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return false
	}

	return ds.IsAttached()
}

func (s *Service) DataSource(name string) (Source, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	return ds, nil
}

// RuntimeSourceByName returns a runtime source by name.
func (s *Service) RuntimeSourceByName(name string) (RuntimeSource, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs, ok := s.runtimeSources[name]
	return rs, ok
}

// Resolve implements DataSourceResolver.
func (s *Service) Resolve(name string) (Source, error) {
	return s.DataSource(name)
}

// ResolveAll implements DataSourceResolver — returns all registered data sources.
func (s *Service) ResolveAll() []Source {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]Source, 0, len(s.dataSources))
	for _, ds := range s.dataSources {
		result = append(result, ds)
	}
	return result
}

func NewDataSource(ctx context.Context, ds types.DataSource, attached bool) (Source, error) {
	switch ds.Type {
	case Postgres:
		return postgres.New(ds, attached)
	case DuckDB:
		return duckdb.New(ds, attached)
	case MySQL:
		return mysql.New(ds, attached)
	case MSSQL:
		return mssql.New(ds, attached)
	case Http:
		return http.New(ds, attached)
	case Airport:
		return airport.New(ds, attached)
	case Extension:
		return extension.New(ds, attached)
	case Embedding:
		return embedding.New(ds, attached)
	case DuckLake:
		return ducklake.New(ds, attached)
	case Iceberg:
		return iceberg.New(ds, attached)
	case HugrApp:
		return hugrapp.New(ds, attached)
	case LLMOpenAI:
		return llm.NewOpenAI(ds, attached)
	case LLMAnthropic:
		return llm.NewAnthropic(ds, attached)
	case LLMGemini:
		return llm.NewGemini(ds, attached)
	case Redis:
		return redissrc.New(ds, attached)
	default:
		return nil, ErrUnknownDataSourceType
	}
}

func (s *Service) HttpRequest(ctx context.Context, source, path, method, headers, params, body, jqq string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.dataSources[source]
	if !ok {
		return nil, ErrDataSourceNotFound
	}
	if !ds.IsAttached() {
		return nil, ErrDataSourceNotAttached
	}
	httpDs, ok := ds.(*http.Source)
	if !ok {
		return nil, errors.New("data source is not http source")
	}
	res, err := httpDs.Request(ctx, path, method, headers, params, body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == 204 {
		return nil, nil
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("request failed with status code %d:%s", res.StatusCode, res.Status)
	}
	if res.Body == nil {
		return nil, errors.New("response body is nil")
	}
	var data any
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return nil, err
	}
	if jqq != "" {
		transform, err := jq.NewTransformer(db.ClearTxContext(ctx), jqq, jq.WithQuerier(s.qe))
		if err != nil {
			return nil, fmt.Errorf("failed to create jq transformer: %v", err)
		}
		data, err = transform.Transform(ctx, data, nil)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (s *Service) CreateEmbedding(ctx context.Context, source, input string) (*EmbeddingResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.dataSources[source]
	if !ok {
		return nil, ErrDataSourceNotFound
	}
	if !ds.IsAttached() {
		return nil, ErrDataSourceNotAttached
	}
	embeddingDs, ok := ds.(EmbeddingSource)
	if !ok {
		return nil, errors.New("data source is not embedding source")
	}
	return embeddingDs.CreateEmbedding(ctx, input)
}

func (s *Service) CreateEmbeddings(ctx context.Context, source string, input []string) (*EmbeddingsResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.dataSources[source]
	if !ok {
		return nil, ErrDataSourceNotFound
	}
	if !ds.IsAttached() {
		return nil, ErrDataSourceNotAttached
	}
	embeddingDs, ok := ds.(EmbeddingSource)
	if !ok {
		return nil, errors.New("data source is not embedding source")
	}
	return embeddingDs.CreateEmbeddings(ctx, input)
}
