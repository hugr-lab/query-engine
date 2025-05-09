package sources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
)

var (
	ErrDataSourceNotFound                  = errors.New("data source not found")
	ErrDataSourceExists                    = errors.New("data source already exists")
	ErrDataSourceAttached                  = errors.New("data source is attached")
	ErrDataSourceNotAttached               = errors.New("data source is not attached")
	ErrUnknownDataSourceType               = errors.New("unknown data source type")
	ErrDataSourceAttachedWithDifferentType = errors.New("data source already attached with different type exists")
	ErrEmptyQuery                          = errors.New("empty query")
	ErrQueryParsingFailed                  = errors.New("query parsing failed")
)

func CheckDBExists(ctx context.Context, db *db.Pool, name string, dsType types.DataSourceType) error {
	var dbType string
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.QueryRow(ctx, "SELECT type FROM duckdb_databases() WHERE database_name= $1", name).Scan(&dbType)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if dbType != string(dsType) {
		return ErrDataSourceAttachedWithDifferentType
	}
	return ErrDataSourceAttached
}

type ParsedDSN struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string

	Params map[string]string
}

func ParseDSN(dsn string) (ParsedDSN, error) {
	dsn, err := ApplyEnvVars(dsn)
	if err != nil {
		return ParsedDSN{}, err
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return ParsedDSN{}, err
	}

	path := strings.TrimPrefix(u.Path, "/")
	parsed := ParsedDSN{
		Host:     u.Hostname(),
		Port:     u.Port(),
		User:     u.User.Username(),
		Password: "",
		DBName:   path,
		Params:   make(map[string]string),
	}

	if password, ok := u.User.Password(); ok {
		parsed.Password = password
	}

	// Parse query params
	queryParams := u.Query()
	for key, values := range queryParams {
		parsed.Params[key] = values[0] // Get first value
	}
	return parsed, nil
}

var reSQLField = regexp.MustCompile(`\[\$?[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\]`)

func ApplyEnvVars(dsn string) (string, error) {
	matches := reSQLField.FindAllString(dsn, -1)
	if len(matches) == 0 {
		return dsn, nil
	}
	for _, match := range matches {
		envVar := strings.TrimSuffix(strings.TrimPrefix(match, "["), "]")
		if !strings.HasPrefix(envVar, "$") {
			continue
		}
		envVar = strings.TrimPrefix(envVar, "$")
		envValue := os.Getenv(envVar)
		if envValue == "" {
			return "", fmt.Errorf("environment variable %s is not set", envVar)
		}
		dsn = strings.ReplaceAll(dsn, match, envValue)
	}
	return dsn, nil
}
