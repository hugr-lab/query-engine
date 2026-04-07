package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/auth"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/query-engine/client/app"
	"github.com/hugr-lab/query-engine/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type ApplicationOptions func(*serveConfig)

type serveConfig struct {
	airport.ServerConfig
	secretKey      string
	startupTimeout time.Duration
}

func (cfg *serveConfig) validate() error {
	if cfg.Allocator == nil {
		cfg.Allocator = memory.DefaultAllocator
	}
	if cfg.startupTimeout == 0 {
		cfg.startupTimeout = 250 * time.Millisecond
	}
	return nil
}

func WithAllocator(alloc memory.Allocator) ApplicationOptions {
	return func(cfg *serveConfig) {
		cfg.Allocator = alloc
	}
}

func WithLogger(logger *slog.Logger) ApplicationOptions {
	return func(cfg *serveConfig) {
		cfg.Logger = logger
	}
}

func WithLogLevel(level *slog.Level) ApplicationOptions {
	return func(cfg *serveConfig) {
		cfg.LogLevel = level
	}
}

func WithMaxMessageSize(size int) ApplicationOptions {
	return func(cfg *serveConfig) {
		cfg.MaxMessageSize = size
	}
}

func WithSecretKey(key string) ApplicationOptions {
	return func(cfg *serveConfig) {
		cfg.secretKey = key
		cfg.Auth = &appAuth{secretKey: key}
	}
}

func WithStartupTimeout(timeout time.Duration) ApplicationOptions {
	return func(cfg *serveConfig) {
		cfg.startupTimeout = timeout
	}
}

func (c *Client) RunApplication(ctx context.Context, application app.Application, opts ...ApplicationOptions) error {
	info := application.Info()
	slog.Info("starting application server", "name", info.Name, "description", info.Description, "version", info.Version, "uri", info.URI)
	if err := validateAppInfo(info); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := &serveConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	err := cfg.validate()
	if err != nil {
		return err
	}

	cfg.Catalog, err = newAppCatalog(ctx, cfg.Allocator, application)
	if err != nil {
		return err
	}
	var grpcOpts []grpc.ServerOption
	var mCfg airport.MultiCatalogServerConfig
	if _, ok := application.(app.MultiCatalogProvider); ok {
		mCfg = airport.MultiCatalogServerConfig{
			Catalogs:       []catalog.Catalog{cfg.Catalog},
			Allocator:      cfg.Allocator,
			Logger:         cfg.Logger,
			LogLevel:       cfg.LogLevel,
			MaxMessageSize: cfg.MaxMessageSize,
			Auth:           cfg.Auth,
			// TransactionManager: nil, // TODO: add transaction manager support for multi-catalog apps
		}
		grpcOpts = airport.MultiCatalogServerOptions(mCfg)
	} else {
		grpcOpts = airport.ServerOptions(cfg.ServerConfig)
	}
	if tlsProvider, ok := application.(app.TLSConfigProvider); ok {
		tlsConfig, err := tlsProvider.TLSConfig(ctx)
		if err != nil {
			return fmt.Errorf("failed to get TLS config: %w", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	grpcSrv := grpc.NewServer(grpcOpts...)
	if mc, ok := application.(app.MultiCatalogProvider); ok {
		mux, err := airport.NewMultiCatalogServer(grpcSrv, mCfg)
		if err != nil {
			return err
		}
		mc.SetMultiCatalogMux(mux)
	} else {
		if err := airport.NewServer(grpcSrv, cfg.ServerConfig); err != nil {
			return err
		}
	}

	listener, err := application.Listner()
	if err != nil {
		return err
	}

	go func() {
		if err := grpcSrv.Serve(listener); err != nil {
			slog.Error("application server error", "error", err)
		}
	}()

	// Wait for gRPC server to start, then register with Hugr
	time.AfterFunc(cfg.startupTimeout, func() {
		path := info.URI
		sep := "?"
		if cfg.secretKey != "" {
			path += sep + "secret_key=" + cfg.secretKey
			sep = "&"
		}
		if info.Version != "" {
			path += sep + "version=" + info.Version
		}

		// Check if already registered in hugr
		slog.Info("checking registration", "name", info.Name, "path", path)
		existing, err := c.lookupDataSource(ctx, info.Name)
		if err != nil {
			slog.Error("failed to check registration", "name", info.Name, "error", err)
			cancel()
			return
		}

		switch {
		case existing == nil:
			// Not registered — register new data source
			slog.Info("registering new hugr-app", "name", info.Name)
			ds := types.DataSource{
				Name:        info.Name,
				Type:        "hugr-app",
				Description: info.Description,
				Prefix:      prefixFromName(info.Name),
				Path:        path,
				AsModule:    true,
				SelfDefined: true,
			}
			if err := c.RegisterDataSource(ctx, ds); err != nil {
				slog.Error("failed to register data source", "error", err)
				cancel()
				return
			}

		case existing.Type != "hugr-app":
			slog.Error("data source type mismatch", "name", info.Name, "expected", "hugr-app", "got", existing.Type)
			cancel()
			return

		case existing.Path != path:
			// Only version changed — update path so hugr sees new version on reload
			slog.Info("version changed, updating path",
				"name", info.Name, "old", existing.Path, "new", path)
			if err := c.updateDataSourcePath(ctx, info.Name, path); err != nil {
				slog.Error("failed to update data source path", "error", err)
				cancel()
				return
			}

		default:
			// Same path (URI + secret + version) — just reload
			slog.Info("already registered, reloading", "name", info.Name)
		}

		// Load (hugr auto-unloads and reloads if already loaded).
		// Retry with backoff — on restart hugr may not be fully ready yet.
		var loadErr error
		for attempt := range 5 {
			loadErr = c.LoadDataSource(ctx, info.Name)
			if loadErr == nil {
				break
			}
			slog.Warn("load data source failed, retrying...", "attempt", attempt+1, "error", loadErr)
			select {
			case <-ctx.Done():
				cancel()
				return
			case <-time.After(time.Duration(attempt+1) * 2 * time.Second):
			}
		}
		if loadErr != nil {
			slog.Error("failed to load data source after retries", "error", loadErr)
			cancel()
			return
		}
		slog.Info("application server started and registered with Hugr", "name", info.Name)

		if mc, ok := application.(app.MultiCatalogProvider); ok {
			if err := mc.InitMultiCatalog(ctx); err != nil {
				slog.Error("failed to initialize multi-catalog application", "error", err)
				cancel()
				return
			}
			slog.Info("multi-catalog application initialized", "name", info.Name)
		}
	})

	<-ctx.Done()

	// Graceful shutdown: call app.Shutdown(), then unload from hugr
	slog.Info("shutting down application", "name", info.Name)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := application.Shutdown(shutdownCtx); err != nil {
		slog.Error("application shutdown error", "name", info.Name, "error", err)
	}
	if err := c.UnloadDataSource(shutdownCtx, info.Name, types.WithHardUnload()); err != nil {
		slog.Error("failed to unload data source on shutdown", "name", info.Name, "error", err)
	}

	grpcSrv.GracefulStop()
	slog.Info("application stopped", "name", info.Name)
	return err
}

func validateAppInfo(info app.AppInfo) error {
	// Name validation
	if info.Name == "" {
		return errors.New("application name cannot be empty")
	}
	// check the app name does not started with '_', not contain special characters other than '.', '-' and '_', and does not contain spaces
	// and does not started with numbers
	if strings.HasPrefix(info.Name, "_") {
		return errors.New("application name cannot start with '_'")
	}
	if strings.ContainsAny(info.Name, " !@#$%^&*()+=[]{}|\\;:'\",<>/?") {
		return errors.New("application name cannot contain spaces or special characters other than '.', '-' and '_'")
	}
	if unicode.IsDigit(rune(info.Name[0])) {
		return errors.New("application name cannot start with a number")
	}
	// Reserved name validation
	topName := strings.SplitN(info.Name, ".", 2)[0]
	switch topName {
	case "function", "mutation_function", "core", "_system":
		return fmt.Errorf("application name %q conflicts with reserved name %q", info.Name, topName)
	}
	// URI validation
	if info.URI == "" {
		return errors.New("application URI cannot be empty")
	}
	// this is a grpc URI, it should start with grpc:// or grpc+tls://
	if !strings.HasPrefix(info.URI, "grpc://") && !strings.HasPrefix(info.URI, "grpc+tls://") {
		return errors.New("application URI must start with grpc:// or grpc+tls://")
	}

	return nil
}

func prefixFromName(name string) string {
	// Use the application name as the prefix for the schema, replacing invalid characters with '_'
	prefix := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, name)
	return prefix
}

type dsLookupResult struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

func (c *Client) lookupDataSource(ctx context.Context, name string) (*dsLookupResult, error) {
	res, err := c.Query(ctx,
		`query($name: String!) { core { data_sources_by_pk(name: $name) { type path } } }`,
		map[string]any{"name": name})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if len(res.Errors) != 0 {
		return nil, fmt.Errorf("query errors: %v", res.Errors)
	}
	var ds dsLookupResult
	if err := res.ScanData("core.data_sources_by_pk", &ds); err != nil {
		if errors.Is(err, types.ErrNoData) {
			return nil, nil
		}
		return nil, err
	}
	return &ds, nil
}

func (c *Client) updateDataSourcePath(ctx context.Context, name, path string) error {
	res, err := c.Query(ctx,
		`mutation($name: String!, $path: String!) { core { update_data_sources(filter: { name: { eq: $name } }, data: { path: $path }) { success } } }`,
		map[string]any{"name": name, "path": path})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func newAppCatalog(ctx context.Context, mem memory.Allocator, app app.Application) (catalog.Catalog, error) {
	cat, err := app.Catalog(ctx)
	if err != nil {
		return nil, err
	}
	mount, err := newAppMountSchema(mem, app)
	if err != nil {
		return nil, err
	}
	return &appCatalog{
		Catalog:     cat,
		mountSchema: mount,
	}, nil
}

type appAuth struct {
	secretKey string
}

var _ airport.Authenticator = (*appAuth)(nil)

// Authenticate implements [auth.Authenticator].
func (a *appAuth) Authenticate(ctx context.Context, token string) (identity string, err error) {
	if token == a.secretKey {
		return "hugr-app", nil
	}
	return "", auth.ErrUnauthenticated
}

type appCatalog struct {
	catalog.Catalog
	mountSchema *appMountSchema
	version     uint64
}

// CatalogVersion implements [catalog.VersionedCatalog].
func (a *appCatalog) CatalogVersion(ctx context.Context) (catalog.CatalogVersion, error) {
	if vc, ok := a.Catalog.(catalog.VersionedCatalog); ok {
		return vc.CatalogVersion(ctx)
	}
	_, isDyn := a.Catalog.(catalog.DynamicCatalog)
	return catalog.CatalogVersion{
		Version: a.version,
		IsFixed: !isDyn,
	}, nil
}

var _ catalog.VersionedCatalog = (*appCatalog)(nil)

func (a *appCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	schemas, err := a.Catalog.Schemas(ctx)
	if err != nil {
		return nil, err
	}
	return append(schemas, a.mountSchema), nil
}

func (a *appCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	if name == a.mountSchema.Name() {
		return a.mountSchema, nil
	}
	return a.Catalog.Schema(ctx, name)
}

var _ catalog.DynamicCatalog = (*appCatalog)(nil)

var errUnsupportedSchemaOperation = errors.New("unsupported schema operation on reserved schema name")

// CreateSchema implements [catalog.DynamicCatalog].
func (a *appCatalog) CreateSchema(ctx context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
	if name == a.mountSchema.Name() {
		return nil, errUnsupportedSchemaOperation
	}
	dc, ok := a.Catalog.(catalog.DynamicCatalog)
	if !ok {
		return nil, catalog.ErrUnimplemented
	}
	return dc.CreateSchema(ctx, name, opts)
}

// DropSchema implements [catalog.DynamicCatalog].
func (a *appCatalog) DropSchema(ctx context.Context, name string, opts catalog.DropSchemaOptions) error {
	if name == a.mountSchema.Name() {
		return errUnsupportedSchemaOperation
	}
	dc, ok := a.Catalog.(catalog.DynamicCatalog)
	if !ok {
		return catalog.ErrUnimplemented
	}
	return dc.DropSchema(ctx, name, opts)
}

type appMountSchema struct {
	mem    memory.Allocator
	app    app.Application
	tables []catalog.Table
}

func newAppMountSchema(mem memory.Allocator, app app.Application) (*appMountSchema, error) {
	ds, err := newDatasourcesTable(context.Background(), mem, app)
	if err != nil {
		return nil, err
	}
	return &appMountSchema{
		mem:    mem,
		app:    app,
		tables: []catalog.Table{ds},
	}, nil
}

// Comment implements [catalog.Schema].
func (a *appMountSchema) Comment() string {
	return "Internal schema for application metadata and utilities. Not intended for direct use."
}

// Name implements [catalog.Schema].
func (a *appMountSchema) Name() string {
	return "_mount"
}

// ScalarFunctions implements [catalog.Schema].
func (a *appMountSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return []catalog.ScalarFunction{
		&appInfoFunc{app: a.app, mem: a.mem},
		&initScalarFunc{app: a.app, mem: a.mem},
		&schemaSdlFunc{app: a.app, mem: a.mem},
		&dbInitFunc{app: a.app, mem: a.mem},
		&dbMigrateFunc{app: a.app, mem: a.mem},
	}, nil
}

// Table implements [catalog.Schema].
func (a *appMountSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	for _, table := range a.tables {
		if table.Name() == name {
			return table, nil
		}
	}
	return nil, nil
}

// TableFunctions implements [catalog.Schema].
func (a *appMountSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

// TableFunctionsInOut implements [catalog.Schema].
func (a *appMountSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
	return nil, nil
}

// Tables implements [catalog.Schema].
func (a *appMountSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	return a.tables, nil
}

type appInfoFunc struct {
	mem memory.Allocator
	app app.Application
}

// Comment implements [catalog.ScalarFunction].
func (a *appInfoFunc) Comment() string {
	return "Returns application information as a struct: name, description, version, uri."
}

// Execute implements [catalog.ScalarFunction].
func (a *appInfoFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	info := a.app.Info()
	arrayBuilder := array.NewStructBuilder(a.mem, arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "description", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "version", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "uri", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "is_db_initializer", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "is_db_migrator", Type: arrow.FixedWidthTypes.Boolean},
	))
	defer arrayBuilder.Release()
	for range input.NumRows() {
		arrayBuilder.Append(true)
		arrayBuilder.FieldBuilder(0).AppendValueFromString(info.Name)
		arrayBuilder.FieldBuilder(1).AppendValueFromString(info.Description)
		arrayBuilder.FieldBuilder(2).AppendValueFromString(info.Version)
		arrayBuilder.FieldBuilder(3).AppendValueFromString(info.URI)
		_, isDBInit := a.app.(app.ApplicationDBInitializer)
		_, isDBMigrator := a.app.(app.ApplicationDBMigrator)
		arrayBuilder.FieldBuilder(4).(*array.BooleanBuilder).Append(isDBInit)
		arrayBuilder.FieldBuilder(5).(*array.BooleanBuilder).Append(isDBMigrator)
	}

	return arrayBuilder.NewArray(), nil
}

// Name implements [catalog.ScalarFunction].
func (a *appInfoFunc) Name() string {
	return "info"
}

// Signature implements [catalog.ScalarFunction].
func (a *appInfoFunc) Signature() catalog.FunctionSignature {
	out := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "description", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "version", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "uri", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "is_db_initializer", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "is_db_migrator", Type: arrow.FixedWidthTypes.Boolean},
	)
	return catalog.FunctionSignature{
		ReturnType: out,
	}
}

var _ catalog.ScalarFunction = (*appInfoFunc)(nil)

type initScalarFunc struct {
	mem memory.Allocator
	app app.Application
}

// Comment implements [catalog.ScalarFunction].
func (a *initScalarFunc) Comment() string {
	return "Initializes the application. Should be called once after app data sources are registered."
}

// Execute implements [catalog.ScalarFunction].
func (a *initScalarFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	err := a.app.Init(ctx)
	if err != nil {
		return nil, err
	}
	// Return an empty struct array to indicate success.
	out := array.NewBooleanBuilder(a.mem)
	defer out.Release()
	for range input.NumRows() {
		out.Append(true)
	}
	return out.NewArray(), nil
}

// Name implements [catalog.ScalarFunction].
func (a *initScalarFunc) Name() string {
	return "init"
}

// Signature implements [catalog.ScalarFunction].
func (a *initScalarFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		ReturnType: arrow.FixedWidthTypes.Boolean,
	}
}

var _ catalog.ScalarFunction = (*initScalarFunc)(nil)

// schemaSdlFunc returns the full GraphQL SDL for the application's catalog.
type schemaSdlFunc struct {
	mem memory.Allocator
	app app.Application
}

func (a *schemaSdlFunc) Name() string { return "schema_sdl" }
func (a *schemaSdlFunc) Comment() string {
	return "Returns the full GraphQL SDL for this application's catalog."
}

func (a *schemaSdlFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		ReturnType: arrow.BinaryTypes.String,
	}
}

func (a *schemaSdlFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	cat, err := a.app.Catalog(ctx)
	if err != nil {
		return nil, fmt.Errorf("schema_sdl: %w", err)
	}
	// If the catalog is a CatalogMux, use SDLWithModules for default schema support
	var sdl string
	if mux, ok := cat.(*app.CatalogMux); ok {
		sdl = mux.SDLWithModules(a.app.Info().DefaultSchemaName())
	} else if provider, ok := cat.(interface{ SDL() string }); ok {
		sdl = provider.SDL()
	}
	b := array.NewStringBuilder(a.mem)
	defer b.Release()
	for range input.NumRows() {
		b.Append(sdl)
	}
	return b.NewArray(), nil
}

var _ catalog.ScalarFunction = (*schemaSdlFunc)(nil)

type dbInitFunc struct {
	mem memory.Allocator
	app app.Application
}

var _ catalog.ScalarFunction = (*dbInitFunc)(nil)

// Comment implements [catalog.ScalarFunction].
func (d *dbInitFunc) Comment() string {
	return "Initializes the application database schema. Should be called once after app data sources are registered."
}

// Name implements [catalog.ScalarFunction].
func (d *dbInitFunc) Name() string {
	return "init_ds_schema"
}

// Signature implements [catalog.ScalarFunction].
func (d *dbInitFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.BinaryTypes.String, // source name
		},
		ReturnType: arrow.BinaryTypes.String,
	}
}

// Execute implements [catalog.ScalarFunction].
func (d *dbInitFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	dbInitApp, ok := d.app.(app.ApplicationDBInitializer)
	if !ok {
		return nil, fmt.Errorf("application does not implement ApplicationDBInitializer")
	}
	// For simplicity, we assume a single row input with a single string column for the schema name.
	if input.NumRows() == 0 || input.NumCols() == 0 {
		return nil, fmt.Errorf("input must have at least one row and one column")
	}
	sna, ok := input.Column(0).(*array.String)
	if !ok {
		return nil, fmt.Errorf("input column must be of type string")
	}
	b := array.NewStringBuilder(d.mem)
	defer b.Release()
	for i := range input.NumRows() {
		sn := sna.Value(int(i))
		template, err := dbInitApp.InitDBSchemaTemplate(ctx, sn)
		if err != nil {
			return nil, err
		}
		b.Append(template)
	}

	return b.NewArray(), nil
}

type dbMigrateFunc struct {
	mem memory.Allocator
	app app.Application
}

var _ catalog.ScalarFunction = (*dbMigrateFunc)(nil)

// Comment implements [catalog.ScalarFunction].
func (d *dbMigrateFunc) Comment() string {
	return "Migrates the application database schema to a new version. Should be called once after app data sources are registered."
}

// Name implements [catalog.ScalarFunction].
func (d *dbMigrateFunc) Name() string {
	return "migrate_ds_schema"
}

// Signature implements [catalog.ScalarFunction].
func (d *dbMigrateFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.BinaryTypes.String, // source name
			arrow.BinaryTypes.String, // target version
		},
		ReturnType: arrow.BinaryTypes.String,
	}
}

// Execute implements [catalog.ScalarFunction].
func (d *dbMigrateFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	dbMigratorApp, ok := d.app.(app.ApplicationDBMigrator)
	if !ok {
		return nil, fmt.Errorf("application does not implement ApplicationDBMigrator")
	}
	// For simplicity, we assume a single row input with two string columns for the source name and target version.
	if input.NumRows() == 0 || input.NumCols() < 2 {
		return nil, fmt.Errorf("input must have at least one row and two columns")
	}
	sna, ok := input.Column(0).(*array.String)
	if !ok {
		return nil, fmt.Errorf("first input column must be of type string")
	}
	tva, ok := input.Column(1).(*array.String)
	if !ok {
		return nil, fmt.Errorf("second input column must be of type string")
	}
	b := array.NewStringBuilder(d.mem)
	defer b.Release()
	for i := range input.NumRows() {
		sn := sna.Value(int(i))
		tv := tva.Value(int(i))
		result, err := dbMigratorApp.MigrateDBSchemaTemplate(ctx, sn, tv)
		if err != nil {
			return nil, err
		}
		b.Append(result)
	}

	return b.NewArray(), nil
}

type datasourcesTable struct {
	mem    memory.Allocator
	dd     []app.DataSourceInfo
	schema *arrow.Schema
}

var _ catalog.Table = (*datasourcesTable)(nil)

func newDatasourcesTable(ctx context.Context, mem memory.Allocator, a app.Application) (*datasourcesTable, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "type", Type: arrow.BinaryTypes.String},
		{Name: "description", Type: arrow.BinaryTypes.String},
		{Name: "read_only", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "path", Type: arrow.BinaryTypes.String},
		{Name: "version", Type: arrow.BinaryTypes.String},
		{Name: "hugr_schema", Type: arrow.BinaryTypes.String},
	}, nil)

	da, ok := a.(app.DataSourceUser)
	if !ok {
		return &datasourcesTable{
			mem:    mem,
			schema: schema,
		}, nil
	}

	dd, err := da.DataSources(ctx)
	if err != nil {
		return nil, err
	}
	return &datasourcesTable{
		mem:    mem,
		dd:     dd,
		schema: schema,
	}, nil
}

// Comment implements [catalog.Table].
func (d *datasourcesTable) Comment() string {
	return "Returns a list of application data sources."
}

// ArrowSchema implements [catalog.Table].
func (d *datasourcesTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(d.schema, columns)
}

// Name implements [catalog.Table].
func (d *datasourcesTable) Name() string {
	return "data_sources"
}

// Scan implements [catalog.Table].
func (d *datasourcesTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	schema := d.ArrowSchema(opts.Columns)

	builder := array.NewRecordBuilder(d.mem, schema)
	defer builder.Release()

	for _, ds := range d.dd {
		for i := 0; i < schema.NumFields(); i++ {
			f := schema.Field(i)
			switch f.Name {
			case "name":
				builder.Field(i).(*array.StringBuilder).Append(ds.Name)
			case "type":
				builder.Field(i).(*array.StringBuilder).Append(ds.Type)
			case "description":
				builder.Field(i).(*array.StringBuilder).Append(ds.Description)
			case "read_only":
				builder.Field(i).(*array.BooleanBuilder).Append(ds.ReadOnly)
			case "path":
				builder.Field(i).(*array.StringBuilder).Append(ds.Path)
			case "version":
				builder.Field(i).(*array.StringBuilder).Append(ds.Version)
			case "hugr_schema":
				builder.Field(i).(*array.StringBuilder).Append(ds.HugrSchema)
			default:
				// For any columns that are not recognized, append nulls.
				builder.Field(i).AppendNull()
			}
		}
	}

	return array.NewRecordReader(schema, []arrow.RecordBatch{builder.NewRecordBatch()})
}
