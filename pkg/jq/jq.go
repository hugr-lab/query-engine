package jq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/itchyny/gojq"
)

type Transformer struct {
	code *gojq.Code
	vars []string

	stat Stat
}

type Stat struct {
	CompilerTime      time.Duration `json:"compiler_time"`
	SerializationTime time.Duration `json:"serialization_time"`
	ExecutionTime     time.Duration `json:"execution"`
	Runs              int           `json:"runs"`
	Transformed       int           `json:"transformed"`
}

func NewTransformer(query string, vars map[string]any) (*Transformer, error) {
	start := time.Now()
	q, err := gojq.Parse(query)
	if err != nil {
		return nil, err
	}

	var vv []string
	for k := range vars {
		vv = append(vv, k)
	}

	var opts []gojq.CompilerOption
	if len(vv) != 0 {
		opts = append(opts, gojq.WithVariables(vv))
	}

	c, err := gojq.Compile(q, opts...)
	if err != nil {
		return nil, err
	}

	return &Transformer{
		code: c,
		vars: vv,
		stat: Stat{
			CompilerTime: time.Since(start),
		},
	}, nil
}

func (t *Transformer) Transform(ctx context.Context, data interface{}, vars map[string]any) (interface{}, error) {
	start := time.Now()
	// serialize data to json and back to interface{} to avoid issues with gojq
	b, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("jq: json marshal error: %v", err)
	}
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, fmt.Errorf("jq: json unmarshal results error: %v", err)
	}
	t.stat.SerializationTime += time.Since(start)

	start = time.Now()
	var vv []any
	for _, name := range t.vars {
		vv = append(vv, vars[name])
	}
	iter := t.code.RunWithContext(ctx, data, vv...)
	var results []interface{}
	res := 0
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		results = append(results, v)
		res++
	}
	t.stat.ExecutionTime += time.Since(start)
	t.stat.Runs++
	t.stat.Transformed += res
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
