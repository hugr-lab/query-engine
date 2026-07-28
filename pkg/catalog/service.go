package catalog

import (
	"context"
	"errors"
	"iter"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/hugr-lab/query-engine/pkg/catalog/validator/rules"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	ErrCatalogNotFound = errors.New("catalog not found")
	// ErrCatalogNotManaged is returned by the lifecycle methods when the
	// Service sits over a provider that is not a CatalogManager — a static
	// schema, or a read-only view of one someone else writes.
	ErrCatalogNotManaged = errors.New("catalog provider does not manage catalogs")
)

// Service holds dependencies for query parsing: Provider, Validator, VariableTransformer.
// Thread-safe (Provider and Validator are immutable after creation).
type Service struct {
	validator      *validator.Validator
	varTransformer VariableTransformer

	mu       sync.RWMutex
	provider Provider
	manager  CatalogManager
	engines  map[string]engines.Engine
}

// ServiceOption configures a Service.
type ServiceOption func(*Service)

// WithServiceValidator sets the validator for the service.
func WithServiceValidator(v *validator.Validator) ServiceOption {
	return func(s *Service) {
		s.validator = v
	}
}

// WithServiceVarTransformer sets the variable transformer for the service.
func WithServiceVarTransformer(t VariableTransformer) ServiceOption {
	return func(s *Service) {
		s.varTransformer = t
	}
}

// NewService creates a Service with the given provider and options.
//
// The provider doubles as the catalog manager when it is one — the catalog
// storage is. Over a provider that is not (a static schema in a test, a
// cluster worker's read-only view), the lifecycle methods report
// ErrCatalogNotManaged and everything on the read path works unchanged.
func NewService(p Provider, opts ...ServiceOption) *Service {
	m, _ := p.(CatalogManager)
	s := &Service{
		provider: p,
		manager:  m,
		engines:  make(map[string]engines.Engine),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.validator == nil {
		s.validator = validator.New(rules.DefaultRules()...)
	}

	return s
}

// ParseQuery parses a query using the service's dependencies.
func (s *Service) ParseQuery(ctx context.Context, query string, vars map[string]any, operationName string) (*Operation, error) {
	return parseQuery(ctx, s.Provider(), s.validator, s.varTransformer, operationName, query, vars)
}

// ValidateQuery parses and validates/enriches a query, returning the full QueryDocument.
// Unlike ParseQuery it does not select an operation or classify queries.
// Useful when the caller needs to inspect/filter the enriched AST before execution.
func (s *Service) ValidateQuery(ctx context.Context, query string) (*ast.QueryDocument, error) {
	return validateQuery(ctx, s.Provider(), s.validator, query)
}

// SetProvider replaces the Provider (e.g. on catalog change).
// NOT thread-safe — call under external lock.
func (s *Service) SetProvider(p Provider) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.provider = p
}

// Provider returns the current Provider.
func (s *Service) Provider() Provider {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.provider
}

// SetVariableTransformer sets the variable transformer.
func (s *Service) SetVariableTransformer(t VariableTransformer) {
	s.varTransformer = t
}

// --- Provider interface delegation ---
// Service itself implements Provider by delegating to the injected provider.
// This allows consumers to use *Service directly where Provider is expected.

var _ Provider = (*Service)(nil)

func (s *Service) ForName(ctx context.Context, name string) *ast.Definition {
	return s.Provider().ForName(ctx, name)
}
func (s *Service) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return s.Provider().DirectiveForName(ctx, name)
}

func (s *Service) QueryType(ctx context.Context) *ast.Definition {
	return s.Provider().QueryType(ctx)
}

func (s *Service) MutationType(ctx context.Context) *ast.Definition {
	return s.Provider().MutationType(ctx)
}

func (s *Service) SubscriptionType(ctx context.Context) *ast.Definition {
	return s.Provider().SubscriptionType(ctx)
}

func (s *Service) PossibleTypes(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return s.Provider().PossibleTypes(ctx, name)
}

func (s *Service) Implements(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return s.Provider().Implements(ctx, name)
}

func (s *Service) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return s.Provider().Definitions(ctx)
}

func (s *Service) Types(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return s.Provider().Types(ctx)
}

func (s *Service) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return s.Provider().DirectiveDefinitions(ctx)
}

func (s *Service) Description(ctx context.Context) string {
	return s.Provider().Description(ctx)
}

func (s *Service) Engine(name string) (engines.Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if engine, ok := s.engines[name]; ok {
		return engine, nil
	}

	return nil, ErrCatalogNotFound
}

// RegisterEngine adds an engine reference for planner routing without
// going through catalog compilation. Used in read-only mode where
// schemas are already persisted by the writer node.
func (s *Service) RegisterEngine(name string, engine engines.Engine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[name] = engine
}

func (s *Service) AddCatalog(ctx context.Context, name string, catalog Catalog) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.manager == nil {
		return ErrCatalogNotManaged
	}
	s.engines[name] = catalog.Engine()
	return s.manager.AddCatalog(ctx, name, catalog)
}

func (s *Service) RemoveCatalog(ctx context.Context, name string) error {
	if !s.ExistsCatalog(name) {
		return ErrCatalogNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.engines, name)
	return s.manager.RemoveCatalog(ctx, name)
}

// ReplacesCatalogOnAdd forwards the underlying manager's answer (false when it
// does not claim the property), so callers staging a reload can ask through
// the Manager interface — see ReplacingCatalogManager for what it decides.
func (s *Service) ReplacesCatalogOnAdd() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.manager.(ReplacingCatalogManager)
	return ok && r.ReplacesCatalogOnAdd()
}

func (s *Service) ExistsCatalog(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.manager != nil && s.manager.ExistsCatalog(name)
}

func (s *Service) ReloadCatalog(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.manager == nil {
		return ErrCatalogNotManaged
	}
	return s.manager.ReloadCatalog(ctx, name)
}

func (s *Service) SuspendCatalog(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.manager == nil {
		return ErrCatalogNotManaged
	}
	return s.manager.SuspendCatalog(ctx, name)
}

func (s *Service) ReactivateCatalog(ctx context.Context, name string, catalog Catalog) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.manager == nil {
		return ErrCatalogNotManaged
	}
	return s.manager.ReactivateCatalog(ctx, name, catalog)
}

func (s *Service) IsSuspended(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.manager != nil && s.manager.IsSuspended(name)
}

// Schema access
func (s *Service) SchemaProvider() Provider {
	return s
}
