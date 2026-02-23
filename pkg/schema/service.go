package schema

import (
	"context"
	"iter"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/hugr-lab/query-engine/pkg/schema/validator/rules"
	"github.com/vektah/gqlparser/v2/ast"
)

// Service holds dependencies for query parsing: Provider, Validator, VariableTransformer.
// Thread-safe (Provider and Validator are immutable after creation).
type Service struct {
	querier        Querier
	validator      *validator.Validator
	varTransformer VariableTransformer

	mu       sync.RWMutex
	provider Provider
	manager  CatalogManager
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
func NewService(p Provider, opts ...ServiceOption) *Service {
	m, ok := p.(CatalogManager)
	if !ok {
		m = newMemoryCatalogManager(p)
	}
	s := &Service{
		provider: p,
		manager:  m,
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
	if m, ok := s.manager.(*memoryCatalog); ok {
		m.SetProvider(p)
	}
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
