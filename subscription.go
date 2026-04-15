package hugr

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/trace"
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Subscribe creates a subscription from a GraphQL subscription query.
// Dispatches to query streaming or native subscription based on the root field.
func (s *Service) Subscribe(ctx context.Context, query string, vars map[string]any) (*types.Subscription, error) {
	ctx, err := s.applyImpersonation(ctx)
	if err != nil {
		return nil, err
	}
	op, err := s.schema.ParseQuery(ctx, query, vars, "")
	if err != nil {
		return nil, err
	}
	if op.Definition.Operation != ast.Subscription {
		return nil, fmt.Errorf("expected subscription operation, got %s", op.Definition.Operation)
	}
	if len(op.Definition.SelectionSet) == 0 {
		return nil, fmt.Errorf("subscription must have at least one field")
	}
	rootField, ok := op.Definition.SelectionSet[0].(*ast.Field)
	if !ok {
		return nil, fmt.Errorf("subscription root selection must be a field")
	}

	if rootField.Name == "query" {
		return s.subscribeQuery(ctx, rootField, op)
	}
	return s.subscribeNative(ctx, rootField, op)
}

// subscribeQuery streams results of a regular query with optional periodic re-execution.
func (s *Service) subscribeQuery(ctx context.Context, queryField *ast.Field, op *catalog.Operation) (*types.Subscription, error) {
	interval, count := parseIntervalCount(queryField.ArgumentMap(op.Variables))
	innerQueries, _ := sdl.QueryRequestInfo(queryField.SelectionSet)

	ctx, cancel := context.WithCancel(ctx)
	eventCh := make(chan types.SubscriptionEvent)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("panic in subscription: %v", r)
			}
		}()
		defer close(eventCh)

		for tick := 1; ; tick++ {
			s.executeQueryTick(ctx, innerQueries, op.Variables, eventCh)

			if count > 0 && tick >= count {
				return
			}
			if interval == 0 {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()

	return &types.Subscription{Events: eventCh, Cancel: cancel}, nil
}

// executeQueryTick executes one round of the inner query.
// Each FlatQuery path produces a SubscriptionEvent with its own RecordReader.
func (s *Service) executeQueryTick(ctx context.Context, queries []base.QueryRequest, vars map[string]any, eventCh chan<- types.SubscriptionEvent) {
	qm := sdl.FlatQuery(queries)
	if len(qm) == 0 {
		return
	}

	provider := s.schema.Provider()
	ctx = planner.ContextWithRawResultsFlag(ctx)

	var wg sync.WaitGroup
	for path, q := range qm {
		wg.Add(1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic in subscription path %s: %v", path, r)
				}
			}()
			defer wg.Done()

			reader, err := s.executeStreamPath(ctx, provider, q, vars)
			if err != nil {
				trace.LoggerFromContext(ctx).Warn("subscription.path.error", "path", path, "error", err)
				return
			}
			select {
			case <-ctx.Done():
				reader.Release()
			case eventCh <- types.SubscriptionEvent{Path: path, Reader: reader}:
			}
		}()
	}
	wg.Wait()
}

// executeStreamPath plans and executes a single query path as a stream.
func (s *Service) executeStreamPath(ctx context.Context, provider catalog.Provider, q base.QueryRequest, vars map[string]any) (array.RecordReader, error) {
	plan, err := s.planner.Plan(ctx, provider, q.Field, vars)
	if err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	if err := plan.Compile(); err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}

	logger := trace.LoggerFromContext(ctx)
	logger.Debug("subscription.sql", "field", q.Field.Name, "alias", q.Field.Alias, "sql", plan.Log())
	if ai := auth.AuthInfoFromContext(ctx); ai != nil {
		logger.Debug("subscription.user", "user", ai.UserName, "role", ai.Role, "field", q.Field.Name)
	}

	table, finalize, err := plan.ExecuteStream(ctx, s.db)
	if err != nil {
		return nil, err
	}
	reader, err := table.Reader(false)
	if err != nil {
		table.Release()
		finalize()
		return nil, err
	}

	// Wrap reader with geometry and table info metadata from the query AST,
	// matching the behavior of ipc-query.go writeArrowTableToIPC.
	geomFields := geomFieldsInfoFromQuery(q.Field)
	return &metadataReader{
		reader:     reader,
		table:      table,
		finalize:   finalize,
		tableInfo:  table.Info(),
		geomFields: geomFields,
	}, nil
}

// subscribeNative handles native subscription fields resolved via @catalog.
func (s *Service) subscribeNative(ctx context.Context, rootField *ast.Field, op *catalog.Operation) (*types.Subscription, error) {
	leafField := walkToLeafField(rootField)
	if leafField == nil {
		return nil, fmt.Errorf("could not find leaf subscription field with @catalog")
	}

	catalogName := base.FieldDefCatalog(leafField.Definition)
	if catalogName == "" {
		return nil, fmt.Errorf("subscription field %q has no @catalog directive", leafField.Name)
	}

	subSrc, err := s.resolveSubscriptionSource(catalogName)
	if err != nil {
		return nil, err
	}

	result, err := subSrc.Subscribe(ctx, leafField, op.Variables)
	if err != nil {
		return nil, fmt.Errorf("subscription %q failed: %w", catalogName, err)
	}

	// Bridge SubscriptionResult (single RecordReader) into types.Subscription (event channel).
	// The source's Reader produces one record per event (e.g. one LLM token).
	// We pass it directly as a single event — consumer iterates via Reader.Next().
	ctx, cancel := context.WithCancel(ctx)
	eventCh := make(chan types.SubscriptionEvent, 1)

	go func() {
		defer close(eventCh)
		select {
		case <-ctx.Done():
			result.Reader.Release()
		case eventCh <- types.SubscriptionEvent{Reader: result.Reader}:
		}
	}()

	return &types.Subscription{
		Events: eventCh,
		Cancel: func() {
			cancel()
			if result.Cancel != nil {
				result.Cancel()
			}
		},
	}, nil
}

// --- Helpers ---

func parseIntervalCount(args map[string]any) (time.Duration, int) {
	var interval time.Duration
	var count int
	if v, ok := args["interval"]; ok && v != nil {
		if str, ok := v.(string); ok {
			d, _ := time.ParseDuration(str)
			interval = d
		}
	}
	if v, ok := args["count"]; ok && v != nil {
		switch c := v.(type) {
		case int:
			count = c
		case int64:
			count = int(c)
		case float64:
			count = int(c)
		}
	}
	return interval, count
}

// walkToLeafField descends through module wrapper fields to find the leaf
// subscription field that carries the @catalog directive.
func walkToLeafField(field *ast.Field) *ast.Field {
	for {
		if field.Definition != nil && base.FieldDefCatalog(field.Definition) != "" {
			return field
		}
		if len(field.SelectionSet) == 0 {
			return nil
		}
		nested, ok := field.SelectionSet[0].(*ast.Field)
		if !ok {
			return nil
		}
		field = nested
	}
}

// resolveSubscriptionSource resolves a data source by catalog name and asserts SubscriptionSource.
func (s *Service) resolveSubscriptionSource(catalogName string) (sources.SubscriptionSource, error) {
	// Regular data sources
	if src, err := s.ds.DataSource(catalogName); err == nil {
		if ss, ok := src.(sources.SubscriptionSource); ok {
			return ss, nil
		}
		return nil, fmt.Errorf("data source %q does not support subscriptions", catalogName)
	}
	// Runtime sources (core.models, core.store)
	if rs, ok := s.ds.RuntimeSourceByName(catalogName); ok {
		if ss, ok := rs.(sources.SubscriptionSource); ok {
			return ss, nil
		}
		return nil, fmt.Errorf("runtime source %q does not support subscriptions", catalogName)
	}
	return nil, fmt.Errorf("data source %q not found", catalogName)
}

// metadataReader wraps a RecordReader adding geometry and table info
// metadata to the Arrow schema — matching ipc-query.go behavior.
type metadataReader struct {
	reader     array.RecordReader
	table      types.ArrowTable
	finalize   func()
	tableInfo  string
	geomFields map[string]geomInfo
	schema     *arrow.Schema // cached schema with geometry metadata
	once       sync.Once
	current    arrow.RecordBatch
	chunkIdx   int
}

func (r *metadataReader) Schema() *arrow.Schema {
	if r.schema == nil {
		r.schema = addGeometryFieldMeta(r.reader.Schema(), r.geomFields)
	}
	return r.schema
}

func (r *metadataReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if !r.reader.Next() {
		return false
	}
	batch := r.reader.RecordBatch()
	// Add chunk index and geometry metadata to each batch
	meta := map[string]string{"chunk": fmt.Sprintf("%d", r.chunkIdx)}
	if r.tableInfo != "" {
		meta["table_info"] = r.tableInfo
	}
	ns := addMeta(batch.Schema(), meta)
	ns = addGeometryFieldMeta(ns, r.geomFields)
	r.current = array.NewRecordBatch(ns, batch.Columns(), batch.NumRows())
	r.chunkIdx++
	return true
}

func (r *metadataReader) Record() arrow.RecordBatch      { return r.current }
func (r *metadataReader) RecordBatch() arrow.RecordBatch  { return r.current }
func (r *metadataReader) Err() error                      { return r.reader.Err() }
func (r *metadataReader) Retain()                         { r.reader.Retain() }
func (r *metadataReader) Release() {
	r.once.Do(func() {
		if r.current != nil {
			r.current.Release()
		}
		r.reader.Release()
		r.table.Release()
		r.finalize()
	})
}
