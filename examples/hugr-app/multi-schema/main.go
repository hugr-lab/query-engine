// Multi-schema hugr-app example: demonstrates default + named schemas.
//
// Schemas:
//   - "default" → objects at app root: { analytics { users { ... } } }
//   - "reports" → nested module: { analytics { reports { monthly { ... } } } }
//   - "admin"   → nested module: { analytics { admin { audit_log { ... } } } }
//
// Functions:
//   - { function { analytics { user_count } } }  (default schema)
//   - { function { analytics { reports { generate_report(year: 2025) } } } }
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

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
	err := c.RunApplication(ctx, &AnalyticsApp{}, client.WithSecretKey("analytics-secret"))
	if err != nil {
		log.Fatalf("app error: %v", err)
	}
}

type AnalyticsApp struct{}

func (a *AnalyticsApp) Info() app.AppInfo {
	return app.AppInfo{
		Name:        "analytics",
		Description: "Multi-schema analytics app",
		Version:     "1.0.0",
		URI:         "grpc://localhost:50052",
	}
}

func (a *AnalyticsApp) Listner() (net.Listener, error) {
	return net.Listen("tcp", "localhost:50052")
}

func (a *AnalyticsApp) Init(ctx context.Context) error    { return nil }
func (a *AnalyticsApp) Shutdown(ctx context.Context) error { return nil }

func (a *AnalyticsApp) Catalog(ctx context.Context) (catalog.Catalog, error) {
	mux := app.New()

	// === "default" schema — appears at root of analytics module ===

	_ = mux.HandleFunc("default", "user_count", func(w *app.Result, r *app.Request) error {
		return w.Set(int64(42))
	}, app.Return(app.Int64), app.Desc("Total user count"))

	// === "reports" schema — nested module { analytics { reports { ... } } } ===

	_ = mux.HandleFunc("reports", "generate_report", func(w *app.Result, r *app.Request) error {
		return w.Set("Report for year " + string(rune(r.Int64("year"))))
	}, app.Arg("year", app.Int64), app.Return(app.String), app.Desc("Generate annual report"))

	_ = mux.HandleTableFunc("reports", "monthly", func(w *app.Result, r *app.Request) error {
		for i := int64(1); i <= 12; i++ {
			w.Append(i, float64(i)*1000.50)
		}
		return nil
	}, app.Arg("year", app.Int64), app.ColPK("month", app.Int64), app.Col("revenue", app.Float64))

	// === "admin" schema — nested module { analytics { admin { ... } } } ===

	_ = mux.HandleTableFunc("admin", "audit_log", func(w *app.Result, r *app.Request) error {
		w.Append(int64(1), "user_login", "admin")
		w.Append(int64(2), "data_export", "analyst")
		return nil
	}, app.ColPK("id", app.Int64), app.Col("action", app.String), app.Col("user", app.String))

	return mux, nil
}
