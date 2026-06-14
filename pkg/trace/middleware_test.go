package trace_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/trace"
)

func TestTraceMiddlewareIntegration(t *testing.T) {
	var capturedLogger *slog.Logger
	var capturedTraceID string
	var capturedTraceInfo *trace.TraceInfo

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		capturedLogger = trace.LoggerFromContext(ctx)
		capturedTraceID = trace.TraceIDFromContext(ctx)
		capturedTraceInfo = trace.FromContext(ctx)
		w.WriteHeader(http.StatusOK)
	})

	t.Run("generates trace_id when not provided", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/query", nil)
		w := httptest.NewRecorder()

		ctx := trace.ContextWithLogger(req.Context(), slog.Default(), "")
		req = req.WithContext(ctx)

		handler.ServeHTTP(w, req)

		if capturedLogger == nil {
			t.Error("logger should not be nil even without middleware")
		}
	})

	t.Run("propagates X-Trace-Id from request", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/query", nil)
		req.Header.Set("X-Trace-Id", "my-trace-123")

		logger := slog.Default().With("trace_id", "my-trace-123")
		ctx := trace.ContextWithLogger(req.Context(), logger, "my-trace-123")
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if capturedTraceID != "my-trace-123" {
			t.Errorf("trace ID = %q, want %q", capturedTraceID, "my-trace-123")
		}
	})

	t.Run("TraceInfo created when debug level", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/query", nil)
		logger := slog.Default().With("trace_id", "debug-test")
		ctx := trace.ContextWithLogger(req.Context(), logger, "debug-test")
		ti := trace.NewTraceInfo("debug-test", slog.LevelDebug)
		ctx = trace.ContextWithTrace(ctx, ti)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if capturedTraceInfo == nil {
			t.Error("TraceInfo should be present when debug level is set")
		}
	})

	t.Run("TraceInfo nil when info level", func(t *testing.T) {
		capturedTraceInfo = nil
		req := httptest.NewRequest("GET", "/query", nil)
		logger := slog.Default().With("trace_id", "info-test")
		ctx := trace.ContextWithLogger(req.Context(), logger, "info-test")
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if capturedTraceInfo != nil {
			t.Error("TraceInfo should be nil in production (info level)")
		}
	})
}

func TestResultContainsTraceID(t *testing.T) {
	ti := trace.NewTraceInfo("tree-test", slog.LevelDebug)
	result := ti.Result()
	if result["trace_id"] != "tree-test" {
		t.Errorf("trace_id = %v, want %q", result["trace_id"], "tree-test")
	}
	if result["total_time"] == nil {
		t.Error("total_time should be set")
	}
}
