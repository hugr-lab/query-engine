package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
