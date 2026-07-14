package trace

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// TraceInfo holds per-request tracing state: trace ID, log level, and span tree.
// Created when tracing is active (global debug or @trace directive).
type TraceInfo struct {
	TraceID   string
	Level     slog.Level
	Root      *Span
	mu        sync.Mutex
	StartTime time.Time
}

// Span represents a timed segment of work within a request.
// Spans form a tree via Children.
type Span struct {
	Name     string         `json:"name"`
	Duration string         `json:"duration,omitempty"`
	Children []*Span        `json:"children,omitempty"`
	attrs    map[string]any
	start    time.Time
	parent   *Span
}

// NewTraceInfo creates a new TraceInfo with a root span.
func NewTraceInfo(traceID string, level slog.Level) *TraceInfo {
	return &TraceInfo{
		TraceID:   traceID,
		Level:     level,
		Root:      &Span{Name: "root", start: time.Now()},
		StartTime: time.Now(),
	}
}

// SetLevel updates the log level for this request.
func (t *TraceInfo) SetLevel(level slog.Level) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Level = level
}

// Result returns a safe-for-client representation of the trace (names and durations only).
func (t *TraceInfo) Result() map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return map[string]any{
		"trace_id":   t.TraceID,
		"total_time": time.Since(t.StartTime).String(),
		"spans":      spansResult(t.Root.Children),
	}
}

func spansResult(spans []*Span) []map[string]any {
	if len(spans) == 0 {
		return nil
	}
	result := make([]map[string]any, len(spans))
	for i, s := range spans {
		m := map[string]any{
			"name":     s.Name,
			"duration": s.Duration,
		}
		if children := spansResult(s.Children); children != nil {
			m["children"] = children
		}
		result[i] = m
	}
	return result
}

type traceInfoKey struct{}
type loggerKey struct{}
type traceIDKey struct{}
type spanKey struct{}

// ContextWithLogger stores a trace-aware logger and trace ID in the context.
func ContextWithLogger(ctx context.Context, logger *slog.Logger, traceID string) context.Context {
	ctx = context.WithValue(ctx, loggerKey{}, logger)
	ctx = context.WithValue(ctx, traceIDKey{}, traceID)
	return ctx
}

// ContextWithTrace stores TraceInfo in the context.
func ContextWithTrace(ctx context.Context, info *TraceInfo) context.Context {
	ctx = context.WithValue(ctx, traceInfoKey{}, info)
	return context.WithValue(ctx, spanKey{}, info.Root)
}

// FromContext returns TraceInfo from context, or nil if tracing is not active.
func FromContext(ctx context.Context) *TraceInfo {
	if v := ctx.Value(traceInfoKey{}); v != nil {
		if ti, ok := v.(*TraceInfo); ok {
			return ti
		}
	}
	return nil
}

// TraceIDFromContext returns the trace ID string from context, or "".
func TraceIDFromContext(ctx context.Context) string {
	if v := ctx.Value(traceIDKey{}); v != nil {
		if id, ok := v.(string); ok {
			return id
		}
	}
	return ""
}

// LoggerFromContext returns the trace-aware logger, or slog.Default().
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if v := ctx.Value(loggerKey{}); v != nil {
		if l, ok := v.(*slog.Logger); ok {
			return l
		}
	}
	return slog.Default()
}

func currentSpan(ctx context.Context) *Span {
	if v := ctx.Value(spanKey{}); v != nil {
		if s, ok := v.(*Span); ok {
			return s
		}
	}
	return nil
}

// StartSpan creates a child span under the current span (from ctx).
// Returns a new context with the child span as current.
// If tracing is not active, returns ctx unchanged.
func StartSpan(ctx context.Context, name string, attrs ...any) context.Context {
	ti := FromContext(ctx)
	if ti == nil {
		return ctx
	}
	parent := currentSpan(ctx)
	if parent == nil {
		parent = ti.Root
	}
	span := &Span{
		Name:   name,
		start:  time.Now(),
		parent: parent,
		attrs:  attrsToMap(attrs),
	}
	ti.mu.Lock()
	parent.Children = append(parent.Children, span)
	ti.mu.Unlock()

	return context.WithValue(ctx, spanKey{}, span)
}

// EndSpan ends the current span (from ctx), recording its duration.
// Safe to call when tracing is not active (no-op).
func EndSpan(ctx context.Context) {
	span := currentSpan(ctx)
	if span == nil || span.start.IsZero() {
		return
	}
	span.Duration = time.Since(span.start).String()
}

func attrsToMap(attrs []any) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]any, len(attrs)/2)
	for i := 0; i+1 < len(attrs); i += 2 {
		if key, ok := attrs[i].(string); ok {
			m[key] = attrs[i+1]
		}
	}
	return m
}
