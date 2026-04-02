// Basic hugr-app example: a minimal application with one scalar function and one table.
//
// Usage:
//
//	# Start hugr dev-server first, then:
//	go run ./examples/hugr-app/basic/
//
// GraphQL queries:
//
//	{ function { basic_app { add(a: 1, b: 2) } } }
//	{ basic_app { users { id name email } } }
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

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
		hugrURL = "http://localhost:15000/ipc"
	}

	c := client.NewClient(hugrURL)
	if _, err := c.Ping(ctx); err != nil {
		log.Fatalf("cannot connect to hugr at %s: %v", hugrURL, err)
	}

	myApp := &BasicApp{}
	err := c.RunApplication(ctx, myApp, client.WithSecretKey("basic-app-secret"))
	if err != nil {
		log.Fatalf("application error: %v", err)
	}
}

// BasicApp implements app.Application with a simple CatalogMux.
type BasicApp struct{}

func (a *BasicApp) Info() app.AppInfo {
	return app.AppInfo{
		Name:        "basic_app",
		Description: "A minimal hugr-app example",
		Version:     "1.0.0",
		URI:         "grpc://localhost:50051",
	}
}

func (a *BasicApp) Listner() (net.Listener, error) {
	return net.Listen("tcp", "localhost:50051")
}

func (a *BasicApp) Init(ctx context.Context) error {
	slog.Info("BasicApp initialized")
	return nil
}

func (a *BasicApp) Shutdown(ctx context.Context) error {
	slog.Info("BasicApp shutting down")
	return nil
}

func (a *BasicApp) Catalog(ctx context.Context) (catalog.Catalog, error) {
	mux := app.New()

	// Register a scalar function: add(a, b) → Int64
	err := mux.HandleFunc("default", "add", func(w *app.Result, r *app.Request) error {
		return w.Set(r.Int64("a") + r.Int64("b"))
	},
		app.Desc("Add two integers"),
		app.Arg("a", app.Int64),
		app.Arg("b", app.Int64),
		app.Return(app.Int64),
	)
	if err != nil {
		return nil, fmt.Errorf("register add: %w", err)
	}

	// Register a table: users with static data
	mux.Table("default", &usersTable{}, app.WithPK("id"), app.WithDescription("Example users table"))

	return mux, nil
}

// usersTable is a simple in-memory table.
type usersTable struct{}

func (t *usersTable) Name() string    { return "users" }
func (t *usersTable) Comment() string { return "Example users table" }

func (t *usersTable) ArrowSchema(columns []string) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func (t *usersTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	schema := t.ArrowSchema(nil)
	mem := memory.DefaultAllocator

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	// Static data
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"alice@example.com", "bob@example.com", ""}, []bool{true, true, false})

	rec := bldr.NewRecord()
	return array.NewRecordReader(schema, []arrow.Record{rec})
}
