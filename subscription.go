package hugr

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
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
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Subscribe creates a subscription from a GraphQL subscription query.
func (s *Service) Subscribe(ctx context.Context, query string, vars map[string]any) (*types.Subscription, error) {
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

	// Path 1: query streaming — `subscription { query(...) { ... } }`
	if rootField.Name == "query" {
		return s.subscribeQuery(ctx, rootField, op)
	}

	// Path 2: native subscription — resolve via @catalog on leaf field
	return s.subscribeNative(ctx, rootField, op)
}

// subscribeQuery handles `subscription { query(...) { ... } }`.
func (s *Service) subscribeQuery(ctx context.Context, queryField *ast.Field, op *catalog.Operation) (*types.Subscription, error) {
	am := queryField.ArgumentMap(op.Variables)
	var interval time.Duration
	var count int

	if v, ok := am["interval"]; ok && v != nil {
		if str, ok := v.(string); ok {
			d, err := time.ParseDuration(str)
			if err != nil {
				return nil, fmt.Errorf("invalid interval: %w", err)
			}
			interval = d
		}
	}
	if v, ok := am["count"]; ok && v != nil {
		switch c := v.(type) {
		case int:
			count = c
		case int64:
			count = int(c)
		case float64:
			count = int(c)
		}
	}

	// Build inner query operation from the `query` field's selection set.
	// The selection set is already enriched by the validator (definitions, types, etc.)
	innerQueries, _ := sdl.QueryRequestInfo(queryField.SelectionSet)

	ctx, cancel := context.WithCancel(ctx)
	eventCh := make(chan types.SubscriptionEvent)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("panic in subscription query: %v\n%s", r, debug.Stack())
			}
		}()
		defer close(eventCh)
		// Note: do NOT defer cancel() here — consumer owns the context lifecycle.
		// Producer just closes the channel when done.

		tick := 0
		for {
			tick++
			s.executeQueryTick(ctx, innerQueries, op.Variables, eventCh)

			if count > 0 && tick >= count {
				return
			}
			if interval == 0 {
				return // one-shot
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()

	return &types.Subscription{
		Events: eventCh,
		Cancel: cancel,
	}, nil
}

// executeQueryTick executes one round of the query and sends events.
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
					log.Printf("panic in subscription path %s: %v\n%s", path, r, debug.Stack())
				}
			}()
			defer wg.Done()

			reader, err := s.executeStreamPath(ctx, provider, q, vars)
			if err != nil {
				if s.config.Debug {
					log.Printf("subscription path %s error: %v", path, err)
				}
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
// Returns a RecordReader that cleans up table/connection on Release.
func (s *Service) executeStreamPath(ctx context.Context, provider catalog.Provider, q base.QueryRequest, vars map[string]any) (array.RecordReader, error) {
	plan, err := s.planner.Plan(ctx, provider, q.Field, vars)
	if err != nil {
		return nil, fmt.Errorf("failed to plan query: %w", err)
	}
	if err := plan.Compile(); err != nil {
		return nil, fmt.Errorf("failed to compile query: %w", err)
	}

	if s.config.Debug {
		ai := auth.AuthInfoFromContext(ctx)
		if ai != nil {
			log.Printf("Subscription stream: User: %s, Role: %s, Query: %s (%s), SQL: %s",
				ai.UserName, ai.Role, q.Field.Alias, q.Field.Name, plan.Log())
		}
		if auth.IsFullAccess(ctx) {
			log.Printf("Subscription stream: Internal query: %s (%s), SQL: %s",
				q.Field.Alias, q.Field.Name, plan.Log())
		}
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

	return &finalizeReader{reader: reader, table: table, finalize: finalize}, nil
}

// subscribeNative handles native subscription fields resolved via @catalog.
// It walks through module wrapper fields to find the leaf subscription field,
// reads its @catalog directive to resolve the data source, and delegates to
// the source's SubscriptionSource.Subscribe method.
func (s *Service) subscribeNative(ctx context.Context, rootField *ast.Field, op *catalog.Operation) (*types.Subscription, error) {
	// Walk module path to find the leaf subscription field.
	// Module fields are intermediate grouping fields without @catalog;
	// the leaf field carries @catalog identifying the owning data source.
	leafField := rootField
	for {
		catalogName := base.FieldDefCatalog(leafField.Definition)
		if catalogName != "" {
			break
		}
		// Not a leaf — descend into the first field of the selection set.
		if len(leafField.SelectionSet) == 0 {
			return nil, fmt.Errorf("subscription field %q has no @catalog and no nested selections", leafField.Name)
		}
		nested, ok := leafField.SelectionSet[0].(*ast.Field)
		if !ok {
			return nil, fmt.Errorf("subscription field %q: expected nested field, got %T", leafField.Name, leafField.SelectionSet[0])
		}
		leafField = nested
	}

	// Read the source name from @catalog directive.
	catalogName := base.FieldDefCatalog(leafField.Definition)
	if catalogName == "" {
		return nil, fmt.Errorf("subscription field %q has no @catalog directive", leafField.Name)
	}

	// Resolve the data source — check regular sources first, then runtime sources.
	var subSrc sources.SubscriptionSource
	src, err := s.ds.DataSource(catalogName)
	if err == nil {
		var ok bool
		subSrc, ok = src.(sources.SubscriptionSource)
		if !ok {
			return nil, fmt.Errorf("data source %q does not support native subscriptions", catalogName)
		}
	} else {
		// Try runtime sources (e.g. core.models, core.store).
		rs, found := s.ds.RuntimeSourceByName(catalogName)
		if !found {
			return nil, fmt.Errorf("failed to resolve data source %q: %w", catalogName, err)
		}
		var ok bool
		subSrc, ok = rs.(sources.SubscriptionSource)
		if !ok {
			return nil, fmt.Errorf("runtime source %q does not support native subscriptions", catalogName)
		}
	}

	// Call the source's Subscribe method.
	result, err := subSrc.Subscribe(ctx, leafField, op.Variables)
	if err != nil {
		return nil, fmt.Errorf("subscription on source %q failed: %w", catalogName, err)
	}

	// Convert SubscriptionResult to types.Subscription by forwarding records
	// from the result's Reader into an event channel.
	ctx, cancel := context.WithCancel(ctx)
	eventCh := make(chan types.SubscriptionEvent)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("panic in native subscription %s: %v\n%s", leafField.Name, r, debug.Stack())
			}
		}()
		defer close(eventCh)
		defer cancel()
		defer result.Reader.Release()

		for result.Reader.Next() {
			batch := result.Reader.RecordBatch()
			batch.Retain()
			reader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
			if err != nil {
				log.Printf("native subscription %s: failed to create record reader: %v", leafField.Name, err)
				return
			}
			event := types.SubscriptionEvent{
				Reader: reader,
			}
			select {
			case <-ctx.Done():
				reader.Release()
				return
			case eventCh <- event:
			}
		}
		if err := result.Reader.Err(); err != nil {
			log.Printf("native subscription %s: reader error: %v", leafField.Name, err)
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

// finalizeReader wraps a RecordReader with cleanup of table and DuckDB connection.
type finalizeReader struct {
	reader   array.RecordReader
	table    types.ArrowTable
	finalize func()
	once     sync.Once
}

func (r *finalizeReader) Retain()                        { r.reader.Retain() }
func (r *finalizeReader) Schema() *arrow.Schema           { return r.reader.Schema() }
func (r *finalizeReader) Next() bool                      { return r.reader.Next() }
func (r *finalizeReader) Record() arrow.RecordBatch       { return r.reader.Record() }
func (r *finalizeReader) RecordBatch() arrow.RecordBatch  { return r.reader.RecordBatch() }
func (r *finalizeReader) Err() error                      { return r.reader.Err() }
func (r *finalizeReader) Release() {
	r.once.Do(func() {
		r.reader.Release()
		r.table.Release()
		r.finalize()
	})
}
