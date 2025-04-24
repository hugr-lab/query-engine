package coredb

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"

	cs "github.com/hugr-lab/query-engine/pkg/catalogs/sources"
)

const (
	dbVersion = "0.0.5"
	dbName    = "core"
)

var (
	ErrDBIsNotInitialized   = errors.New("db is not initialized")
	ErrDBIsDifferentVersion = errors.New("db is different version")
)

//go:embed schema.sql
var dbSchema string

//go:embed pg-schema.sql
var pgSchema string

//go:embed schema.graphql
var schema string

type Config struct {
	Path     string
	ReadOnly bool

	S3Bucket string
	S3Region string
	S3Key    string
	S3Secret string
}

type Source struct {
	c      Config
	dbType types.DataSourceType
	e      engines.Engine
}

func New(c Config) *Source {
	if strings.HasPrefix(c.Path, "postgres://") {
		return &Source{c: c, dbType: sources.Postgres, e: engines.NewPostgres()}
	}
	return &Source{c: c, dbType: sources.DuckDB, e: engines.NewDuckDB()}
}

func (s *Source) Name() string {
	return dbName
}

func (s *Source) Engine() engines.Engine {
	return s.e
}

func (s *Source) IsReadonly() bool {
	return s.c.ReadOnly
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	// check if db is attached
	err := sources.CheckDBExists(ctx, db, s.Name(), s.dbType)
	if err != nil {
		return err
	}

	if s.c.Path == "" {
		// attach as in-memory
		s.c.Path = ":memory:"
	}

	sql := "ATTACH DATABASE '" + s.c.Path + "' AS " + dbName
	switch {
	case s.dbType == sources.DuckDB && s.IsReadonly():
		sql += " (READ_ONLY)"
	case s.dbType == sources.Postgres && !s.IsReadonly():
		sql += " (TYPE POSTGRES)"
	case s.dbType == sources.Postgres && s.IsReadonly():
		sql += " (TYPE POSTGRES, READ_ONLY)"
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return err
	}

	// check if db is not initialized apply schema
	err = checkDBVersion(ctx, db)
	if errors.Is(err, ErrDBIsNotInitialized) && !s.IsReadonly() {
		return s.applySchema(ctx, db)
	}

	return err
}

func (s *Source) Catalog(ctx context.Context) cs.Source {
	return cs.NewStringSource(schema)
}

func checkDBVersion(ctx context.Context, db *db.Pool) error {
	// check if the db is already initialized
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("core db initialization: %w", err)
	}
	defer conn.Close()
	var isExists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(FROM duckdb_tables() WHERE database_name = '"+dbName+"' AND table_name = 'version');",
	).Scan(&isExists)
	if err != nil {
		return fmt.Errorf("core db check version: %w", err)
	}
	if !isExists {
		return ErrDBIsNotInitialized
	}

	var version *string
	err = conn.QueryRow(ctx, `SELECT "version" FROM core."version" LIMIT 1;`).Scan(&version)
	if err != nil {
		return err
	}
	if version == nil {
		return ErrDBIsNotInitialized
	}
	if *version != dbVersion {
		return ErrDBIsDifferentVersion
	}
	return nil
}

func (s *Source) applySchema(ctx context.Context, db *db.Pool) error {
	sql := dbSchema
	if s.dbType == sources.Postgres {
		sql = "FROM " + s.e.(engines.EngineQueryScanner).WrapExec(s.Name(), pgSchema)
	}
	_, err := db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("core db initialization: %w", err)
	}
	return nil
}
