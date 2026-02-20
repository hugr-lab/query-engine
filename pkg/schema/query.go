package schema

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/hugr-lab/query-engine/pkg/schema/validator/rules"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
)

// ParseOption configures ParseQuery.
type ParseOption func(*parseConfig)

type parseConfig struct {
	validator      *validator.Validator
	varTransformer VariableTransformer
	operationName  string
}

// WithValidator sets the validator for ParseQuery.
// Default: validator.New(rules.DefaultRules()...).
func WithValidator(v *validator.Validator) ParseOption {
	return func(cfg *parseConfig) {
		cfg.validator = v
	}
}

// WithVariableTransformer sets the variable transformer.
// Default: nil (no transformation).
func WithVariableTransformer(t VariableTransformer) ParseOption {
	return func(cfg *parseConfig) {
		cfg.varTransformer = t
	}
}

// WithOperationName sets the operation name to select from the document.
// Default: "" (single operation).
func WithOperationName(name string) ParseOption {
	return func(cfg *parseConfig) {
		cfg.operationName = name
	}
}

// ParseQuery is a universal pipeline for parsing and validating GraphQL queries.
//
// Pipeline:
//  1. VariableTransformer.TransformVariables(ctx, vars) — if set
//  2. parser.ParseQuery(query) — gqlparser lexer + parser
//  3. validator.Validate(ctx, provider, doc) — our Walker: enrichment + validation + permissions
//  4. selectOperation(operations, operationName) — select operation
//  5. QueryRequestInfo(op.SelectionSet) — classify queries
//
// Returns: enriched Operation with filled field.Definition, field.ObjectDefinition etc.
func ParseQuery(ctx context.Context, p Provider, query string, vars map[string]any, opts ...ParseOption) (*Operation, error) {
	cfg := &parseConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// Default validator
	if cfg.validator == nil {
		cfg.validator = validator.New(rules.DefaultRules()...)
	}

	return parseQuery(ctx, p, cfg.validator, cfg.varTransformer, cfg.operationName, query, vars)
}

func validateQuery(ctx context.Context, p Provider, val *validator.Validator, query string) (*ast.QueryDocument, error) {
	doc, parseErr := parser.ParseQuery(&ast.Source{Input: query})
	if parseErr != nil {
		return nil, gqlerror.List{gqlerror.WrapIfUnwrapped(parseErr)}
	}

	errs := val.Validate(ctx, p, doc)
	if len(errs) > 0 {
		return nil, errs
	}

	return doc, nil
}

func parseQuery(ctx context.Context, p Provider, val *validator.Validator, vt VariableTransformer, operationName string, query string, vars map[string]any) (*Operation, error) {
	// 1. Transform variables
	if vt != nil && len(vars) > 0 {
		transformed, err := vt.TransformVariables(ctx, vars)
		if err != nil {
			return nil, fmt.Errorf("variable transformation: %w", err)
		}
		vars = transformed
	}

	// 2. Parse (lexer + parser, without gqlparser validation)
	doc, parseErr := parser.ParseQuery(&ast.Source{Input: query})
	if parseErr != nil {
		return nil, gqlerror.List{gqlerror.WrapIfUnwrapped(parseErr)}
	}

	// 3. Validate + enrich (our Validator)
	errs := val.Validate(ctx, p, doc)
	if len(errs) > 0 {
		return nil, errs
	}

	// 4. Select operation by name
	op, err := selectOperation(doc.Operations, operationName)
	if err != nil {
		return nil, err
	}

	// 5. Classify queries
	queries, qtt := QueryRequestInfo(op.SelectionSet)

	return &Operation{
		Definition: op,
		Fragments:  doc.Fragments,
		Variables:  vars,
		Queries:    queries,
		QueryType:  qtt,
	}, nil
}

func selectOperation(ops ast.OperationList, name string) (*ast.OperationDefinition, error) {
	if len(ops) == 0 {
		return nil, gqlerror.List{gqlerror.Errorf("no operations found")}
	}
	if name == "" {
		return ops[0], nil
	}
	for _, op := range ops {
		if op.Name == name {
			return op, nil
		}
	}
	return nil, gqlerror.List{gqlerror.Errorf("operation %q not found", name)}
}
