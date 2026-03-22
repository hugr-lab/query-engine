package datasources

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpSource "github.com/hugr-lab/query-engine/pkg/data-sources/sources/http"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"

	. "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

func testHTTPServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/no_content":
			w.WriteHeader(http.StatusNoContent)
		case "/ok_array":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"id":1,"name":"test"}]`))
		case "/ok_object":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"id":1}`))
		case "/server_error":
			http.Error(w, "internal error", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
}

func testService(t *testing.T, serverURL string) *Service {
	t.Helper()
	testDB, err := db.NewPool("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testDB.Close() })

	ds := types.DataSource{
		Name: "test_http",
		Type: "http",
		Path: serverURL,
	}
	src, err := httpSource.New(ds, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Attach(context.Background(), testDB); err != nil {
		t.Fatal(err)
	}

	return &Service{
		dataSources: map[string]Source{
			"test_http": src,
		},
	}
}

func TestService_HttpRequest(t *testing.T) {
	server := testHTTPServer()
	defer server.Close()

	svc := testService(t, server.URL)

	tests := []struct {
		name     string
		path     string
		wantJSON string
		wantErr  bool
	}{
		{
			name:     "200 with array body",
			path:     "/ok_array",
			wantJSON: `[{"id":1,"name":"test"}]`,
		},
		{
			name:     "200 with object body",
			path:     "/ok_object",
			wantJSON: `{"id":1}`,
		},
		{
			name:     "204 no content returns null",
			path:     "/no_content",
			wantJSON: `null`,
		},
		{
			name:    "500 returns error",
			path:    "/server_error",
			wantErr: true,
		},
		{
			name:    "404 returns error",
			path:    "/not_found",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := svc.HttpRequest(
				context.Background(),
				"test_http", tt.path, "GET",
				"{}", "{}", "{}", "",
			)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			b, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("failed to marshal result: %v", err)
			}

			var want, got any
			if err := json.Unmarshal([]byte(tt.wantJSON), &want); err != nil {
				t.Fatalf("bad wantJSON: %v", err)
			}
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("failed to unmarshal result: %v", err)
			}

			wantBytes, _ := json.Marshal(want)
			gotBytes, _ := json.Marshal(got)
			if string(wantBytes) != string(gotBytes) {
				t.Errorf("result mismatch:\n  want: %s\n  got:  %s", wantBytes, gotBytes)
			}
		})
	}
}

func TestService_HttpRequest_NoContent_ConvertOutput(t *testing.T) {
	server := testHTTPServer()
	defer server.Close()

	svc := testService(t, server.URL)

	result, err := svc.HttpRequest(
		context.Background(),
		"test_http", "/no_content", "GET",
		"{}", "{}", "{}", "",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	if string(b) != "null" {
		t.Errorf("ConvertOutput would produce %q, want %q", string(b), "null")
	}
}
