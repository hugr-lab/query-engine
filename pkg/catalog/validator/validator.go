package validator

import (
	"context"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// Validator validates and enriches GraphQL query documents.
// Rules are injected via the constructor.
type Validator struct {
	inlineRules []InlineRule
	postRules   []PostRule
}

// Option configures a Validator.
type Option func(*Validator)

// WithInlineRule adds an inline rule to the validator.
func WithInlineRule(r InlineRule) Option {
	return func(v *Validator) {
		v.inlineRules = append(v.inlineRules, r)
	}
}

// WithPostRule adds a post-walk rule to the validator.
func WithPostRule(r PostRule) Option {
	return func(v *Validator) {
		v.postRules = append(v.postRules, r)
	}
}

// New creates a Validator with the given rules.
// Without options the validator only performs enrichment (no validation checks).
// For standard usage: New(DefaultRules()...)
func New(opts ...Option) *Validator {
	v := &Validator{}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// Validate walks the QueryDocument, enriches the AST, and applies validation rules.
// The document is mutated: field.Definition, field.ObjectDefinition, etc. are filled in.
// Returns a list of errors (empty = valid).
func (v *Validator) Validate(ctx context.Context, provider TypeResolver, document *ast.QueryDocument) gqlerror.List {
	wCtx := &WalkContext{
		Context:  ctx,
		Provider: provider,
		Document: document,
	}
	w := &walker{
		wCtx:        wCtx,
		inlineRules: v.inlineRules,
	}

	// Walk + enrichment + inline rules
	w.walk()

	// Collect inline errors
	errs := w.errs

	// Post-walk rules
	for _, rule := range v.postRules {
		errs = append(errs, rule.Validate(wCtx, document)...)
	}

	return errs
}
