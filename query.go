package hugr

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/cache"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/hugr-lab/query-engine/pkg/metadata"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
	"golang.org/x/sync/errgroup"
)

var ErrParallelMutationNotSupported = errors.New("parallel mutation queries are not supported")

type result struct {
	data       any
	extensions map[string]any
	name       string
	path       []string
}

func (s *Service) processQuery(ctx context.Context, schema *ast.Schema, op *ast.OperationDefinition, vars map[string]any) (map[string]any, map[string]any, error) {
	// find all requested queries (top level query fields)
	queries, qtt := compiler.QueryRequestInfo(op.SelectionSet)
	// authorize queries
	p := perm.PermissionsFromCtx(ctx)
	if p != nil {
		for _, q := range queries {
			if err := p.CheckQuery(q.Field); err != nil {
				return nil, nil, err
			}
		}
	}

	start := time.Now()
	// create response data structure
	dataCh := make(chan result)
	eg, ctx := errgroup.WithContext(ctx)
	if s.config.MaxParallelQueries != 0 {
		eg.SetLimit(s.config.MaxParallelQueries + 1)
	}
	wg := sync.WaitGroup{}
	// if requested at least one mutation query need to run in a transaction and sequentially
	if !s.config.AllowParallel || qtt&(compiler.QueryTypeMutation|compiler.QueryTypeFunctionMutation) != 0 {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			if qtt&(compiler.QueryTypeMutation|compiler.QueryTypeFunctionMutation) == 0 {
				return s.processQuerySequential(ctx, schema, queries, vars, nil, dataCh)
			}

			ctx, err := s.db.WithTx(ctx)
			if err != nil {
				return err
			}
			defer s.db.Rollback(ctx)
			err = s.processQuerySequential(ctx, schema, queries, vars, nil, dataCh)
			if err != nil {
				return err
			}
			return s.db.Commit(ctx)
		})
	} else {
		// if requested only query queries can run in parallel
		s.processQueryParallel(ctx, &wg, eg, schema, queries, vars, nil, dataCh)
	}
	data := map[string]any{}
	extensions := map[string]any{}
	eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case res, ok := <-dataCh:
				if !ok {
					return nil
				}
				data, extensions = collectResult(data, extensions, res)
			}
		}
	})
	wg.Wait()
	close(dataCh)
	err := eg.Wait()
	if err != nil {
		types.DataClose(data)
		return nil, nil, err
	}
	if len(data) == 1 {
		for _, v := range data {
			if v == nil {
				data = nil
			}
		}
	}
	if len(extensions) == 0 && op.Directives.ForName(base.StatsDirectiveName) == nil {
		return data, nil, nil
	}
	ext := map[string]any{}
	if op.Directives.ForName(base.StatsDirectiveName) != nil {
		ext["stats"] = map[string]any{
			"name":       op.Name,
			"total_time": time.Since(start).String(),
		}
	}
	if len(ext) != 0 {
		ext["children"] = extensions
	} else {
		ext = extensions
	}

	return data, ext, nil
}

func collectResult(data, extensions map[string]any, res result) (map[string]any, map[string]any) {
	if len(res.path) == 0 {
		if res.data != nil || data[res.name] == nil {
			data[res.name] = res.data
		}
		if res.extensions != nil {
			var v any = res.extensions
			if p, ok := extensions[res.name]; ok {
				for k, vv := range res.extensions {
					p.(map[string]any)[k] = vv
				}
				v = p
			}
			extensions[res.name] = v
		}
		return data, extensions
	}
	node := res.path[0]
	d, ok := data[node]
	if !ok {
		d = make(map[string]any)
	}
	res.path = res.path[1:]
	e, ok := extensions[node]
	if ok {
		e = e.(map[string]any)["children"]
	}
	if !ok {
		e = make(map[string]any)
	}
	var ext map[string]any
	data[node], ext = collectResult(d.(map[string]any), e.(map[string]any), res)
	if len(ext) == 0 {
		return data, extensions
	}
	c, ok := extensions[node]
	if !ok {
		extensions[node] = map[string]any{"children": ext}
		return data, extensions
	}
	c.(map[string]any)["children"] = ext
	extensions[node] = c

	return data, extensions
}

func (s *Service) processQuerySequential(ctx context.Context,
	schema *ast.Schema,
	queries []compiler.QueryRequest,
	vars map[string]any,
	path []string,
	dataCh chan<- result,
) error {
	for _, query := range queries {
		var err error
		var res any
		var ext map[string]any
		switch query.QueryType {
		case compiler.QueryTypeNone:
			start := time.Now()
			if query.Subset != nil {
				err = s.processQuerySequential(ctx, schema, query.Subset, vars, append(path, query.Name), dataCh)
				if err != nil {
					return err
				}
			}
			if query.Field.Directives.ForName(base.StatsDirectiveName) != nil {
				ext = map[string]any{
					"stats": map[string]any{
						"name":      query.Name,
						"node_time": time.Since(start).String(),
					},
				}
			}
		case compiler.QueryTypeMeta:
			res, err = metadata.ProcessQuery(ctx, schema, query, s.config.MaxDepth, vars)
		case compiler.QueryTypeQuery, compiler.QueryTypeFunction, compiler.QueryTypeH3Aggregation:
			res, ext, err = s.processDataQuery(ctx, schema, query, vars)
		case compiler.QueryTypeMutation, compiler.QueryTypeFunctionMutation:
			res, ext, err = s.processDataQuery(ctx, schema, query, vars)
		case compiler.QueryTypeJQTransform:
			res, ext, err = s.processJQTransformation(ctx, schema, query, vars)
		}
		if err != nil {
			return err
		}
		if res == nil && ext == nil {
			continue
		}
		select {
		case <-ctx.Done():
		case dataCh <- result{data: res, extensions: ext, name: query.Name, path: path}:
		}
	}

	return nil
}

func (s *Service) processQueryParallel(
	ctx context.Context,
	wg *sync.WaitGroup,
	eg *errgroup.Group,
	schema *ast.Schema,
	queries []compiler.QueryRequest,
	vars map[string]any,
	path []string,
	dataCh chan<- result,
) {
	for _, query := range queries {
		switch query.QueryType {
		case compiler.QueryTypeNone:
			s.processQueryParallel(ctx, wg, eg, schema, query.Subset, vars, append(path, query.Name), dataCh)
		case compiler.QueryTypeJQTransform:
			wg.Add(1)
			eg.Go(func() error {
				defer wg.Done()
				res, ext, err := s.processJQTransformation(ctx, schema, query, vars)
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case dataCh <- result{data: res, extensions: ext, name: query.Name, path: path}:
				}
				return nil
			})
		case compiler.QueryTypeMeta:
			wg.Add(1)
			eg.Go(func() error {
				defer wg.Done()
				res, err := metadata.ProcessQuery(ctx, schema, query, s.config.MaxDepth, vars)
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case dataCh <- result{data: res, name: query.Name, path: path}:
				}
				return nil
			})
		case compiler.QueryTypeQuery, compiler.QueryTypeFunction, compiler.QueryTypeH3Aggregation:
			wg.Add(1)
			eg.Go(func() error {
				defer wg.Done()
				res, ext, err := s.processDataQuery(ctx, schema, query, vars)
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case dataCh <- result{data: res, extensions: ext, name: query.Name, path: path}:
				}
				return nil
			})
		case compiler.QueryTypeMutation, compiler.QueryTypeFunctionMutation:
			wg.Add(1)
			eg.Go(func() error {
				defer wg.Done()
				return ErrParallelMutationNotSupported
			})
		}
	}
}

func (s *Service) processDataQuery(ctx context.Context, schema *ast.Schema, query compiler.QueryRequest, vars map[string]any) (data any, ext map[string]any, err error) {
	start := time.Now()
	var plannerTime, compileTime time.Duration
	dataFunc := func() (any, error) {
		plan, err := s.planner.Plan(ctx, schema, query.Field, vars)
		if err != nil {
			return nil, err
		}
		plannerTime = time.Since(start)
		err = plan.Compile()
		if err != nil {
			return nil, err
		}
		compileTime = time.Since(start)

		if s.config.Debug {
			ai := auth.AuthInfoFromContext(ctx)
			if ai != nil {
				log.Printf("User: %s, Role: %s, Query: %s (%s), SQL: %s",
					ai.UserName,
					ai.Role,
					query.Field.Alias,
					query.Field.Name,
					plan.Log(),
				)
			}
			if auth.IsFullAccess(ctx) {
				log.Printf("Internal query: %s (%s), SQL: %s",
					query.Field.Alias,
					query.Field.Name,
					plan.Log(),
				)
			}
		}
		// execute query
		return plan.Execute(ctx, s.db)
	}

	ci := cache.QueryInfo(query.Field, vars)
	if !ci.Use {
		data, err = dataFunc()
	}

	if ci.Use {
		if ci.Key == "" {
			return nil, nil, compiler.ErrorPosf(query.Field.Position, "cache key is empty")
		}
		ci.Key = query.Field.Name + "_" + ci.Key
		data, err = s.cache.Load(ctx, ci.Key, dataFunc, ci.Options()...)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	if ci.Invalidate {
		if ci.Key != "" {
			err = s.cache.Delete(ctx, ci.Key)
			if err != nil {
				return nil, nil, err
			}
		}
		if len(ci.Tags) != 0 {
			err = s.cache.Invalidate(ctx, ci.Tags...)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	execTime := time.Since(start)
	if query.Field.Directives.ForName(base.StatsDirectiveName) != nil {
		ext = map[string]any{
			"stats": map[string]any{
				"name":          query.Field.Alias,
				"node_time":     execTime.String(),
				"planning_time": plannerTime.String(),
				"compile_time":  compileTime.String(),
				"exec_time":     (execTime - compileTime).String(),
			},
		}
	}
	return data, ext, nil
}

func (s *Service) processJQTransformation(ctx context.Context, schema *ast.Schema, query compiler.QueryRequest, vars map[string]any) (data any, ext map[string]any, err error) {
	start := time.Now()
	var dataTime, compilerTime, serializationTime, execTime time.Duration
	var rn, tn int

	dataFunc := func() (any, error) {
		am := query.Field.ArgumentMap(vars)
		if len(am) == 0 {
			return nil, compiler.ErrorPosf(query.Field.Position, "jq requires arguments")
		}
		if _, ok := am["query"]; !ok {
			return nil, compiler.ErrorPosf(query.Field.Position, "jq requires query argument")
		}
		q, ok := am["query"].(string)
		if !ok {
			return nil, compiler.ErrorPosf(query.Field.Position, "jq query argument should be string")
		}
		t, err := jq.NewTransformer(ctx, q, jq.WithVariables(vars), jq.WithQuerier(s), jq.WithCollectStat())
		if err != nil {
			return nil, compiler.ErrorPosf(query.Field.Position, "jq query compile error: %v", err)
		}
		a, includeResults := am["include_origin"]
		if includeResults {
			includeResults, ok = a.(bool)
			if !ok {
				return nil, compiler.ErrorPosf(query.Field.Position, "includeResults argument should be boolean")
			}
		}
		data, ext, err := s.processQuery(ctx, schema, &ast.OperationDefinition{
			Operation:    ast.Query,
			Name:         query.Field.Alias,
			SelectionSet: query.Field.SelectionSet,
			Position:     query.Field.Position,
			Comment:      query.Field.Comment,
		}, vars)
		if err != nil {
			return nil, err
		}
		if !includeResults {
			defer types.DataClose(data)
		}
		transformed, err := t.Transform(ctx, data, nil)
		if err != nil {
			return nil, compiler.ErrorPosf(query.Field.Position, "jq query execution error: %v", err)
		}
		extension := map[string]any{}
		if ext != nil {
			extension["children"] = ext
		}
		extension["jq"] = transformed
		out := map[string]any{
			"ext": extension,
		}
		if includeResults {
			out["data"] = data
		}
		js := t.Stats()
		dataTime = time.Since(start)
		compilerTime = js.CompilerTime
		serializationTime = js.SerializationTime
		execTime = js.ExecutionTime
		rn = js.Runs
		tn = js.Transformed
		return out, nil
	}

	ci := cache.QueryInfo(query.Field, vars)
	var res any
	if !ci.Use {
		res, err = dataFunc()
	}
	if ci.Use {
		if ci.Key == "" {
			return nil, nil, compiler.ErrorPosf(query.Field.Position, "cache key is empty")
		}
		ci.Key = query.Field.Name + "_" + ci.Key
		res, err = s.cache.Load(ctx, ci.Key, dataFunc, ci.Options()...)
	}

	if err != nil {
		return nil, nil, err
	}

	if ci.Invalidate {
		if ci.Key != "" {
			err = s.cache.Delete(ctx, ci.Key)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	out := res.(map[string]any)
	data = out["data"]
	ext = out["ext"].(map[string]any)
	if query.Field.Directives.ForName(base.StatsDirectiveName) != nil {
		ext["stats"] = map[string]any{
			"data_request_time":  dataTime.String(),
			"compiler_time":      compilerTime.String(),
			"serialization_time": serializationTime.String(),
			"execution_time":     execTime.String(),
			"node_time":          time.Since(start).String(),
			"runs":               rn,
			"transformed":        tn,
		}
	}

	return data, ext, nil
}
