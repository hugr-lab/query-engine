package main

import (
	"context"
	"fmt"
	"log"
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

	c := client.NewClient(hugrURL)

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
		URI:         fmt.Sprintf("grpc://test-app:%s", a.port),
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

	// Manual SDL for testing
	// Functions: catalog prefix added automatically by planner (EngineFunctionCallWithCatalog)
	// Tables: catalog prefix added automatically by ATTACH AS
	mux.WithSDL(`
extend type Function {
  add(a: BigInt!, b: BigInt!): BigInt @function(name: "\"default\".\"ADD\"")
  echo(msg: String!): String @function(name: "\"default\".\"ECHO\"")
}

type items @table(name: "\"default\".\"ITEMS\"") {
  id: BigInt! @pk
  name: String!
}
`)

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
