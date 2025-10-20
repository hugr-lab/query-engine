package jq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/itchyny/gojq"
)

type Transformer struct {
	code *gojq.Code
	opt  options

	mu   sync.Mutex
	stat Stat
}

type Stat struct {
	CompilerTime      time.Duration `json:"compiler_time"`
	SerializationTime time.Duration `json:"serialization_time"`
	ExecutionTime     time.Duration `json:"execution"`
	Runs              int           `json:"runs"`
	Transformed       int           `json:"transformed"`
}

func NewTransformer(ctx context.Context, query string, opts ...Option) (*Transformer, error) {
	start := time.Now()
	q, err := gojq.Parse(query)
	if err != nil {
		return nil, err
	}

	transformOptions := &options{}
	for _, opt := range opts {
		opt(transformOptions)
	}

	c, err := gojq.Compile(q, transformOptions.compilerOptions(ctx)...)
	if err != nil {
		return nil, err
	}

	return &Transformer{
		code: c,
		opt:  *transformOptions,
		stat: Stat{
			CompilerTime: time.Since(start),
		},
	}, nil
}

func (t *Transformer) Transform(ctx context.Context, in any, vars map[string]any) (interface{}, error) {
	start := time.Now()
	// serialize data to json and back to interface{} to avoid issues with gojq
	b, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("jq: json marshal error: %v", err)
	}
	var data any
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, fmt.Errorf("jq: json unmarshal results error: %v", err)
	}
	st := time.Since(start)

	start = time.Now()
	var vv []any
	for _, name := range t.opt.varNames {
		if vars == nil {
			vv = append(vv, t.opt.vars[strings.TrimPrefix(name, "$")])
			continue
		}
		vv = append(vv, vars[strings.TrimPrefix(name, "$")])
	}
	iter := t.code.RunWithContext(ctx, data, vv...)
	var results []interface{}
	res := 0
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return nil, fmt.Errorf("jq: transform error: %v", err)
		}
		results = append(results, v)
		res++
	}
	if t.opt.collectStat {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.stat.SerializationTime += st
		t.stat.ExecutionTime += time.Since(start)
		t.stat.Runs++
		t.stat.Transformed += res
	}
	if len(results) == 1 {
		return results[0], nil
	}

	return results, nil
}

func (t *Transformer) Stats() Stat {
	return Stat{
		CompilerTime:      t.stat.CompilerTime,
		SerializationTime: t.stat.SerializationTime / time.Duration(t.stat.Runs),
		ExecutionTime:     t.stat.ExecutionTime / time.Duration(t.stat.Runs),
		Runs:              t.stat.Runs,
		Transformed:       t.stat.Transformed,
	}
}
