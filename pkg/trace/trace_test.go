package trace

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestContextWithLogger(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default().With("trace_id", "test-123")
	ctx = ContextWithLogger(ctx, logger, "test-123")

	got := LoggerFromContext(ctx)
	if got != logger {
		t.Error("LoggerFromContext should return the stored logger")
	}

	id := TraceIDFromContext(ctx)
	if id != "test-123" {
		t.Errorf("TraceIDFromContext = %q, want %q", id, "test-123")
	}
}

func TestLoggerFromContext_Default(t *testing.T) {
	ctx := context.Background()
	got := LoggerFromContext(ctx)
	if got != slog.Default() {
		t.Error("LoggerFromContext on empty context should return slog.Default()")
	}
}

func TestTraceIDFromContext_Empty(t *testing.T) {
	ctx := context.Background()
	if id := TraceIDFromContext(ctx); id != "" {
		t.Errorf("TraceIDFromContext on empty context = %q, want empty", id)
	}
}

func TestFromContext_Nil(t *testing.T) {
	ctx := context.Background()
	if ti := FromContext(ctx); ti != nil {
		t.Error("FromContext on empty context should return nil")
	}
}

func TestNewTraceInfo(t *testing.T) {
	ti := NewTraceInfo("abc-123", slog.LevelDebug)
	if ti.TraceID != "abc-123" {
		t.Errorf("TraceID = %q, want %q", ti.TraceID, "abc-123")
	}
	if ti.Level != slog.LevelDebug {
		t.Errorf("Level = %v, want %v", ti.Level, slog.LevelDebug)
	}
	if ti.Root == nil {
		t.Fatal("Root span should not be nil")
	}
	if len(ti.Root.Children) != 0 {
		t.Error("Root should have no children initially")
	}
}

func TestStartSpan_EndSpan(t *testing.T) {
	ti := NewTraceInfo("test", slog.LevelDebug)
	ctx := context.Background()
	ctx = ContextWithTrace(ctx, ti)

	ctx2 := StartSpan(ctx, "planner.plan", "field", "users")
	time.Sleep(time.Millisecond)
	EndSpan(ctx2)

	if len(ti.Root.Children) != 1 {
		t.Fatalf("expected 1 child span, got %d", len(ti.Root.Children))
	}
	span := ti.Root.Children[0]
	if span.Name != "planner.plan" {
		t.Errorf("span name = %q, want %q", span.Name, "planner.plan")
	}
	if span.Duration == "" {
		t.Error("span duration should be set after EndSpan")
	}
	if span.attrs["field"] != "users" {
		t.Errorf("span attrs[field] = %v, want %q", span.attrs["field"], "users")
	}
}

func TestStartSpan_NoTrace(t *testing.T) {
	ctx := context.Background()
	ctx2 := StartSpan(ctx, "should.noop")
	if ctx2 != ctx {
		t.Error("StartSpan without TraceInfo should return the same context")
	}
}

func TestEndSpan_NoTrace(t *testing.T) {
	EndSpan(context.Background())
}

func TestNestedSpans(t *testing.T) {
	ti := NewTraceInfo("test", slog.LevelDebug)
	ctx := ContextWithTrace(context.Background(), ti)

	ctx1 := StartSpan(ctx, "parent")
	ctx2 := StartSpan(ctx1, "child")
	EndSpan(ctx2)
	EndSpan(ctx1)

	if len(ti.Root.Children) != 1 {
		t.Fatalf("expected 1 root child, got %d", len(ti.Root.Children))
	}
	parent := ti.Root.Children[0]
	if parent.Name != "parent" {
		t.Errorf("parent span name = %q, want %q", parent.Name, "parent")
	}
	if len(parent.Children) != 1 {
		t.Fatalf("expected 1 child under parent, got %d", len(parent.Children))
	}
	child := parent.Children[0]
	if child.Name != "child" {
		t.Errorf("child span name = %q, want %q", child.Name, "child")
	}
}

func TestParallelSpans(t *testing.T) {
	ti := NewTraceInfo("test", slog.LevelDebug)
	ctx := ContextWithTrace(context.Background(), ti)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			spanCtx := StartSpan(ctx, "parallel", "idx", idx)
			time.Sleep(time.Millisecond)
			EndSpan(spanCtx)
		}(i)
	}
	wg.Wait()

	if len(ti.Root.Children) != 10 {
		t.Errorf("expected 10 children, got %d", len(ti.Root.Children))
	}
}

func TestParallelSpans_CorrectParenting(t *testing.T) {
	ti := NewTraceInfo("test", slog.LevelDebug)
	ctx := ContextWithTrace(context.Background(), ti)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			fieldCtx := StartSpan(ctx, "field")
			childCtx := StartSpan(fieldCtx, "planner")
			EndSpan(childCtx)
			EndSpan(fieldCtx)
		}(i)
	}
	wg.Wait()

	if len(ti.Root.Children) != 5 {
		t.Fatalf("expected 5 root children, got %d", len(ti.Root.Children))
	}
	for _, span := range ti.Root.Children {
		if span.Name != "field" {
			t.Errorf("root child name = %q, want %q", span.Name, "field")
		}
		if len(span.Children) != 1 {
			t.Errorf("expected 1 child under field, got %d", len(span.Children))
			continue
		}
		if span.Children[0].Name != "planner" {
			t.Errorf("child name = %q, want %q", span.Children[0].Name, "planner")
		}
	}
}

func TestResult(t *testing.T) {
	ti := NewTraceInfo("abc", slog.LevelDebug)
	ctx := ContextWithTrace(context.Background(), ti)

	ctx1 := StartSpan(ctx, "parse")
	EndSpan(ctx1)

	ctx2 := StartSpan(ctx, "execute")
	EndSpan(ctx2)

	result := ti.Result()
	if result["trace_id"] != "abc" {
		t.Errorf("trace_id = %v, want %q", result["trace_id"], "abc")
	}
	spans, ok := result["spans"].([]map[string]any)
	if !ok {
		t.Fatalf("spans type = %T, want []map[string]any", result["spans"])
	}
	if len(spans) != 2 {
		t.Errorf("expected 2 spans, got %d", len(spans))
	}
}

func TestSetLevel(t *testing.T) {
	ti := NewTraceInfo("test", slog.LevelInfo)
	if ti.Level != slog.LevelInfo {
		t.Errorf("initial level = %v, want %v", ti.Level, slog.LevelInfo)
	}
	ti.SetLevel(slog.LevelDebug)
	if ti.Level != slog.LevelDebug {
		t.Errorf("after SetLevel = %v, want %v", ti.Level, slog.LevelDebug)
	}
}

func TestAttrsToMap(t *testing.T) {
	m := attrsToMap([]any{"key1", "val1", "key2", 42})
	if m["key1"] != "val1" {
		t.Errorf("key1 = %v, want %q", m["key1"], "val1")
	}
	if m["key2"] != 42 {
		t.Errorf("key2 = %v, want %d", m["key2"], 42)
	}
}

func TestAttrsToMap_Empty(t *testing.T) {
	m := attrsToMap(nil)
	if m != nil {
		t.Errorf("attrsToMap(nil) = %v, want nil", m)
	}
}

func TestAttrsToMap_OddCount(t *testing.T) {
	m := attrsToMap([]any{"key1", "val1", "orphan"})
	if len(m) != 1 {
		t.Errorf("expected 1 entry, got %d", len(m))
	}
}
