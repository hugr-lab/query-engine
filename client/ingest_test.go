package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- shared helpers -------------------------------------------------------

// ingestOKHandler is a server that decodes the incoming Arrow IPC stream,
// counts rows, and answers with a canonical IngestResult. The decoded
// schema's column names are returned in the response so tests can assert
// per-column fidelity.
func ingestOKHandler(t *testing.T, pool memory.Allocator) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/ipc/ingest" {
			t.Errorf("expected /ipc/ingest, got %s", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/vnd.apache.arrow.stream") {
			t.Errorf("unexpected content-type: %s", ct)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		rr, err := ipc.NewReader(bytes.NewReader(body), ipc.WithAllocator(pool))
		if err != nil {
			t.Fatalf("decode body as arrow stream: %v", err)
		}
		defer rr.Release()
		var rows int64
		var cols []string
		for _, f := range rr.Schema().Fields() {
			cols = append(cols, f.Name)
		}
		for rr.Next() {
			rows += rr.RecordBatch().NumRows()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data_object": r.URL.Query().Get("data_object"),
			"inserted":    rows,
			"columns":     cols,
		})
	}
}

// smallRecord builds a single 2-row record with an int32 + string column.
func smallRecord(t *testing.T, pool memory.Allocator) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"alpha", "beta"}, nil)
	return b.NewRecord()
}

func TestBuildIngestURL(t *testing.T) {
	tests := []struct {
		name       string
		base       string
		dataObject string
		want       string
	}{
		{
			name:       "canonical /ipc base",
			base:       "http://localhost:15000/ipc",
			dataObject: "pg_store.public.events",
			want:       "http://localhost:15000/ipc/ingest?data_object=pg_store.public.events",
		},
		{
			name:       "trailing slash on /ipc",
			base:       "http://localhost:15000/ipc/",
			dataObject: "events",
			want:       "http://localhost:15000/ipc/ingest?data_object=events",
		},
		{
			name:       "base without /ipc",
			base:       "http://localhost:15000",
			dataObject: "events",
			want:       "http://localhost:15000/ipc/ingest?data_object=events",
		},
		{
			name:       "base already at /ipc/ingest",
			base:       "http://localhost:15000/ipc/ingest",
			dataObject: "events",
			want:       "http://localhost:15000/ipc/ingest?data_object=events",
		},
		{
			name:       "data_object with special chars is encoded",
			base:       "http://localhost:15000/ipc",
			dataObject: "schema.table with space",
			want:       "http://localhost:15000/ipc/ingest?data_object=schema.table+with+space",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildIngestURL(tc.base, tc.dataObject)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBuildIngestURL_BadBase(t *testing.T) {
	_, err := buildIngestURL("://not a url", "events")
	require.Error(t, err)
}

// TestIngest_RoundTrip exercises the full client path against an in-memory
// HTTP server: it verifies the URL, headers, that the body is a valid Arrow
// IPC stream, and that the success response is parsed back into IngestResult.
func TestIngest_RoundTrip(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/ipc/ingest" {
			t.Errorf("expected /ipc/ingest, got %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("data_object"); got != "ns.mytable" {
			t.Errorf("expected data_object=ns.mytable, got %q", got)
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/vnd.apache.arrow.stream") {
			t.Errorf("unexpected content-type: %s", ct)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		rr, err := ipc.NewReader(bytes.NewReader(body), ipc.WithAllocator(pool))
		if err != nil {
			t.Fatalf("decode body as arrow stream: %v", err)
		}
		defer rr.Release()
		var rows int64
		for rr.Next() {
			rows += rr.RecordBatch().NumRows()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data_object": r.URL.Query().Get("data_object"),
			"inserted":    rows,
			"columns":     []string{"id", "name"},
		})
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL + "/ipc")
	res, err := c.IngestRecord(context.Background(), "ns.mytable", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "ns.mytable", res.DataObject)
	assert.Equal(t, int64(3), res.Inserted)
	assert.Equal(t, []string{"id", "name"}, res.Columns)
}

func TestIngest_ServerError(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "column foo is not defined"})
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL + "/ipc")
	_, err := c.IngestRecord(context.Background(), "ns.x", rec)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "column foo is not defined"),
		"error should surface server message, got: %v", err)
}

func TestIngest_NilReader(t *testing.T) {
	c := NewClient("http://localhost/ipc")
	_, err := c.Ingest(context.Background(), "ns.x", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, err))
}

func TestIngest_EmptyDataObject(t *testing.T) {
	c := NewClient("http://localhost/ipc")
	_, err := c.IngestRecord(context.Background(), "", nil)
	require.Error(t, err)
}

func TestIngest_ServerErrorTextBody(t *testing.T) {
	// 4xx with a non-JSON body — error message must still be surfaced.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusUnsupportedMediaType)
		_, _ = w.Write([]byte("Content-Type must be application/vnd.apache.arrow.stream"))
	}))
	t.Cleanup(srv.Close)

	pool := memory.NewGoAllocator()
	rec := smallRecord(t, pool)
	defer rec.Release()
	c := NewClient(srv.URL + "/ipc")
	_, err := c.IngestRecord(context.Background(), "ns.x", rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Content-Type must be")
}

func TestIngest_WriterErrorWinsOverHTTP(t *testing.T) {
	// reader.Err() returns a non-nil error AFTER yielding one good batch.
	// The HTTP side will see EOF / truncated stream and may respond 4xx;
	// the client must surface the writer-side error, not the HTTP one.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain body to unblock the writer, then respond with a generic 500
		// so we can confirm the client prefers writer error over this.
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server failed"))
	}))
	t.Cleanup(srv.Close)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	pool := memory.NewGoAllocator()
	errBoom := errors.New("reader source explosion")
	calls := 0
	reader := NewLazyReader(schema, func() (arrow.RecordBatch, error) {
		calls++
		if calls == 1 {
			b := array.NewRecordBuilder(pool, schema)
			defer b.Release()
			b.Field(0).(*array.Int32Builder).Append(1)
			return b.NewRecord(), nil
		}
		return nil, errBoom
	})
	defer reader.Release()

	c := NewClient(srv.URL + "/ipc")
	_, err := c.Ingest(context.Background(), "ns.x", reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reader source explosion",
		"writer-side error must be surfaced, got: %v", err)
}

// --- IngestStream ---------------------------------------------------------

func TestIngestStream_Happy(t *testing.T) {
	pool := memory.NewGoAllocator()
	srv := httptest.NewServer(ingestOKHandler(t, pool))
	t.Cleanup(srv.Close)

	rec := smallRecord(t, pool)
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	c := NewClient(srv.URL + "/ipc")
	res, err := c.IngestStream(context.Background(), "ns.t", &buf)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.Equal(t, "ns.t", res.DataObject)
	assert.ElementsMatch(t, []string{"id", "name"}, res.Columns)
}

func TestIngestStream_NilBody(t *testing.T) {
	c := NewClient("http://localhost/ipc")
	_, err := c.IngestStream(context.Background(), "ns.t", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "body is nil")
}

func TestIngestStream_EmptyDataObject(t *testing.T) {
	c := NewClient("http://localhost/ipc")
	_, err := c.IngestStream(context.Background(), "", bytes.NewReader(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "data_object")
}

func TestIngestStream_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid arrow stream"})
	}))
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL + "/ipc")
	_, err := c.IngestStream(context.Background(), "ns.t", bytes.NewReader([]byte("not arrow")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid arrow stream")
}

// --- IngestArrowIPCFile ---------------------------------------------------

func writeArrowStreamFile(t *testing.T, dir string, pool memory.Allocator) (string, *arrow.Schema) {
	t.Helper()
	rec := smallRecord(t, pool)
	defer rec.Release()
	path := filepath.Join(dir, "data.arrows")
	f, err := os.Create(path)
	require.NoError(t, err)
	w := ipc.NewWriter(f, ipc.WithSchema(rec.Schema()))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
	return path, rec.Schema()
}

func writeArrowIPCFile(t *testing.T, dir string, pool memory.Allocator) (string, *arrow.Schema) {
	t.Helper()
	rec := smallRecord(t, pool)
	defer rec.Release()
	path := filepath.Join(dir, "data.arrow")
	f, err := os.Create(path)
	require.NoError(t, err)
	fw, err := ipc.NewFileWriter(f, ipc.WithSchema(rec.Schema()))
	require.NoError(t, err)
	require.NoError(t, fw.Write(rec))
	require.NoError(t, fw.Close())
	require.NoError(t, f.Close())
	return path, rec.Schema()
}

func TestIngestArrowIPCFile_StreamFormat(t *testing.T) {
	pool := memory.NewGoAllocator()
	srv := httptest.NewServer(ingestOKHandler(t, pool))
	t.Cleanup(srv.Close)

	path, _ := writeArrowStreamFile(t, t.TempDir(), pool)
	// Sanity-check: file is *stream* format (no ARROW1 magic).
	head, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(head), 6)
	assert.NotEqual(t, "ARROW1", string(head[:6]))

	c := NewClient(srv.URL + "/ipc")
	res, err := c.IngestArrowIPCFile(context.Background(), "ns.t", path)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
}

func TestIngestArrowIPCFile_FileFormat(t *testing.T) {
	pool := memory.NewGoAllocator()
	srv := httptest.NewServer(ingestOKHandler(t, pool))
	t.Cleanup(srv.Close)

	path, _ := writeArrowIPCFile(t, t.TempDir(), pool)
	// Sanity-check: file is *file* format (ARROW1 magic).
	head, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(head), 6)
	assert.Equal(t, "ARROW1", string(head[:6]))

	c := NewClient(srv.URL + "/ipc")
	res, err := c.IngestArrowIPCFile(context.Background(), "ns.t", path)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
}

func TestIngestArrowIPCFile_NotFound(t *testing.T) {
	c := NewClient("http://localhost/ipc")
	_, err := c.IngestArrowIPCFile(context.Background(), "ns.t",
		filepath.Join(t.TempDir(), "does-not-exist.arrows"))
	require.Error(t, err)
}

func TestIngestArrowIPCFile_EmptyPath(t *testing.T) {
	c := NewClient("http://localhost/ipc")
	_, err := c.IngestArrowIPCFile(context.Background(), "ns.t", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path is required")
}

// --- NewLazyReader --------------------------------------------------------

func TestNewLazyReader_CompletesOnNilNil(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	i := 0
	r := NewLazyReader(schema, func() (arrow.RecordBatch, error) {
		if i >= 3 {
			return nil, nil // signal end-of-stream
		}
		i++
		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()
		b.Field(0).(*array.Int32Builder).Append(int32(i))
		return b.NewRecord(), nil
	})
	defer r.Release()

	assert.Equal(t, schema, r.Schema())
	seen := 0
	for r.Next() {
		require.NotNil(t, r.RecordBatch())
		assert.Equal(t, int64(1), r.RecordBatch().NumRows())
		seen++
	}
	require.NoError(t, r.Err())
	assert.Equal(t, 3, seen)
	assert.False(t, r.Next(), "Next stays false after end-of-stream")
}

func TestNewLazyReader_PropagatesError(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	errBoom := errors.New("source failure")
	i := 0
	r := NewLazyReader(schema, func() (arrow.RecordBatch, error) {
		if i == 2 {
			return nil, errBoom
		}
		i++
		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()
		b.Field(0).(*array.Int32Builder).Append(int32(i))
		return b.NewRecord(), nil
	})
	defer r.Release()
	seen := 0
	for r.Next() {
		seen++
	}
	assert.Equal(t, 2, seen, "should yield batches before the failing call")
	require.Error(t, r.Err())
	assert.ErrorIs(t, r.Err(), errBoom)
	assert.False(t, r.Next(), "Next stays false after error")
}

func TestNewLazyReader_RetainReleaseRefcount(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	r := NewLazyReader(schema, func() (arrow.RecordBatch, error) { return nil, nil })
	// initial refCount = 1 (set by constructor)
	rc := refCountOf(t, r)
	assert.Equal(t, int64(1), rc.Load())
	r.Retain()
	assert.Equal(t, int64(2), rc.Load())
	r.Release()
	assert.Equal(t, int64(1), rc.Load())
	r.Release()
	assert.Equal(t, int64(0), rc.Load())
}

// refCountOf reaches into the concrete *lazyReader to verify retain/release
// semantics. Test-only — the field is unexported on purpose.
func refCountOf(t *testing.T, r array.RecordReader) *atomic.Int64 {
	t.Helper()
	lr, ok := r.(*lazyReader)
	require.True(t, ok, "expected *lazyReader, got %T", r)
	return &lr.refCount
}
