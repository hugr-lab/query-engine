package jq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/itchyny/gojq"
)

var ErrInvalidHugrQueryParam = errors.New("invalid hugr query parameter")

type options struct {
	qe          types.Querier
	varNames    []string
	vars        map[string]any
	collectStat bool
}

func (o *options) compilerOptions(ctx context.Context) []gojq.CompilerOption {
	var opts []gojq.CompilerOption
	if o.qe != nil {
		opts = append(opts, gojq.WithFunction("queryHugr", 1, 2, func(arg any, args []any) any {
			ctx := db.ClearTxContext(ctx)
			if len(args) < 1 {
				return ErrInvalidHugrQueryParam
			}
			query, ok := args[0].(string)
			if !ok {
				return ErrInvalidHugrQueryParam
			}
			if len(args) > 2 {
				return ErrInvalidHugrQueryParam
			}
			var vars map[string]any
			if len(args) == 2 {
				v, ok := args[1].(map[string]any)
				if !ok {
					return ErrInvalidHugrQueryParam
				}
				vars = v
			}
			res, err := o.qe.Query(ctx, query, vars)
			if err != nil {
				return fmt.Errorf("failed to execute query: %w", err)
			}
			defer res.Close()
			if len(res.Errors) > 0 {
				return fmt.Errorf("failed to execute hugr query: %w", res.Errors)
			}
			var data any
			b, err := json.Marshal(res.Data)
			if err != nil {
				return fmt.Errorf("failed to marshal result: %w", err)
			}
			err = json.Unmarshal(b, &data)
			if err != nil {
				return fmt.Errorf("failed to unmarshal result: %w", err)
			}
			return data
		}))
	}
	if len(o.varNames) > 0 {
		opts = append(opts, gojq.WithVariables(o.varNames))
	}
	return opts
}

type Option func(*options)

func WithQuerier(qe types.Querier) Option {
	return func(opts *options) {
		opts.qe = qe
	}
}

func WithVariables(vars map[string]any) Option {
	return func(opts *options) {
		opts.varNames = make([]string, 0, len(vars))
		for k := range vars {
			opts.varNames = append(opts.varNames, k)
		}
		opts.vars = vars
	}
}

func WithCollectStat() Option {
	return func(opts *options) {
		opts.collectStat = true
	}
}
