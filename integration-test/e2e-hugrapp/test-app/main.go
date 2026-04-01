package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/client/app"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	hugrURL := os.Getenv("HUGR_URL")
	if hugrURL == "" {
		hugrURL = "http://localhost:15100/ipc"
	}
	secret := os.Getenv("APP_SECRET")
	if secret == "" {
		secret = "test-secret"
	}
	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "50051"
	}

	c := client.NewClient(hugrURL, client.WithTimeout(5*time.Minute))

	myApp := &TestApp{port: port}
	err := c.RunApplication(ctx, myApp, client.WithSecretKey(secret))
	if err != nil {
		log.Fatalf("app error: %v", err)
	}
}

type TestApp struct {
	port string
}

func (a *TestApp) Info() app.AppInfo {
	version := os.Getenv("APP_VERSION")
	if version == "" {
		version = "1.0.0"
	}
	return app.AppInfo{
		Name:        "test_app",
		Description: "E2E test application",
		Version:     version,
		URI:         fmt.Sprintf("grpc://%s:%s", envOrDefault("APP_HOST", "test-app"), a.port),
	}
}

func (a *TestApp) Listner() (net.Listener, error) {
	return net.Listen("tcp", "0.0.0.0:"+a.port)
}

func (a *TestApp) Init(ctx context.Context) error {
	log.Println("TestApp initialized")
	return nil
}

func (a *TestApp) Shutdown(ctx context.Context) error {
	log.Println("TestApp shutting down")
	return nil
}

// DataSources implements [app.DataSourceUser].
// Declares a PostgreSQL data source that hugr will provision.
func (a *TestApp) DataSources(ctx context.Context) ([]app.DataSourceInfo, error) {
	pgDSN := os.Getenv("PG_DSN")
	if pgDSN == "" {
		return nil, nil // no DB needed if PG_DSN not set
	}
	return []app.DataSourceInfo{
		{
			Name:        "store",
			Type:        "postgres",
			Description: "Test app PostgreSQL store",
			Path:        pgDSN,
			Version:     a.Info().Version,
			ReadOnly:    false,
			HugrSchema: `
"""Events log table"""
type events @table(name: "events") {
  id: Int! @pk
  event_type: String!
  """Event payload data"""
  payload: String
  created_at: Timestamp
  severity: String
}
`,
		},
	}, nil
}

// InitDBSchemaTemplate implements [app.ApplicationDBInitializer].
// Returns SQL to create the initial schema for a data source.
func (a *TestApp) InitDBSchemaTemplate(ctx context.Context, name string) (string, error) {
	switch name {
	case "store":
		return `
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
INSERT INTO events (event_type, payload) VALUES
    ('app_start', 'test-app initialized'),
    ('test_event', 'hello from test-app');
`, nil
	default:
		return "", fmt.Errorf("unknown data source: %s", name)
	}
}

// MigrateDBSchemaTemplate implements [app.ApplicationDBMigrator].
func (a *TestApp) MigrateDBSchemaTemplate(ctx context.Context, name, fromVersion string) (string, error) {
	switch name {
	case "store":
		return `
ALTER TABLE events ADD COLUMN IF NOT EXISTS severity TEXT DEFAULT 'info';
`, nil
	default:
		return "", fmt.Errorf("unknown data source: %s", name)
	}
}

// Compile-time interface checks.
var (
	_ app.DataSourceUser       = (*TestApp)(nil)
	_ app.ApplicationDBInitializer = (*TestApp)(nil)
	_ app.ApplicationDBMigrator    = (*TestApp)(nil)
)

func (a *TestApp) Catalog(ctx context.Context) (catalog.Catalog, error) {
	mux := app.New()

	err := mux.HandleFunc("default", "add", func(w *app.Result, r *app.Request) error {
		return w.Set(r.Int64("a") + r.Int64("b"))
	}, app.Arg("a", app.Int64), app.Arg("b", app.Int64), app.Return(app.Int64))
	if err != nil {
		return nil, err
	}

	err = mux.HandleFunc("default", "echo", func(w *app.Result, r *app.Request) error {
		return w.Set(r.String("msg"))
	}, app.Arg("msg", app.String), app.Return(app.String))
	if err != nil {
		return nil, err
	}

	mux.Table("default", &staticTable{})

	// Table function: search(query) returns filtered items
	err = mux.HandleTableFunc("default", "search", func(w *app.Result, r *app.Request) error {
		q := r.String("query")
		data := map[string][]any{
			"alpha": {int64(1), "alpha"},
			"beta":  {int64(2), "beta"},
			"gamma": {int64(3), "gamma"},
		}
		for name, row := range data {
			if q == "" || strings.Contains(name, q) {
				w.Append(row...)
			}
		}
		return nil
	}, app.Arg("query", app.String),
		app.ColPK("id", app.Int64),
		app.Col("name", app.String),
	)
	if err != nil {
		return nil, err
	}

	// Named schema "admin" — becomes nested module: { test_app { admin { ... } } }
	err = mux.HandleFunc("admin", "user_count", func(w *app.Result, r *app.Request) error {
		return w.Set(int64(99))
	}, app.Return(app.Int64), app.Desc("Total users in admin"))
	if err != nil {
		return nil, err
	}

	mux.HandleTableFunc("admin", "audit", func(w *app.Result, r *app.Request) error {
		limit := r.Int64("limit")
		if limit <= 0 {
			limit = 100
		}
		data := [][]any{
			{int64(1), "login", "admin_user"},
			{int64(2), "export", "analyst"},
		}
		for i, row := range data {
			if int64(i) >= limit {
				break
			}
			w.Append(row...)
		}
		return nil
	}, app.Arg("limit", app.Int64),
		app.ColPK("id", app.Int64), app.Col("action", app.String), app.Col("user_name", app.String))

	return mux, nil
}

type staticTable struct{}

func (t *staticTable) Name() string    { return "items" }
func (t *staticTable) Comment() string { return "Test items" }

func (t *staticTable) ArrowSchema(columns []string) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func (t *staticTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	schema := t.ArrowSchema(nil)
	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "gamma"}, nil)
	rec := bldr.NewRecord()
	return array.NewRecordReader(schema, []arrow.Record{rec})
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
