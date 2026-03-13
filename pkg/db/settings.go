package db

import (
	"fmt"
	"strings"
)

type Settings struct {
	AllowedDirectories   []string `json:"allowed_directories"`
	AllowedPaths         []string `json:"allowed_paths"`
	EnableLogging        bool     `json:"enable_logging"`
	MaxMemory            int      `json:"max_memory"`
	MaxTempDirectorySize int      `json:"max_temp_directory_size"`
	TempDirectory        string   `json:"temp_directory"`
	WorkerThreads        int      `json:"worker_threads"`
	HomeDirectory        string   `json:"home_directory"`
	// POSTGRESQL
	PGConnectionLimit int `json:"pg_connection_limit"`
	PGPagesPerTask    int `json:"pg_pages_per_task"`

	// MSSQL
	MSSQLConnectionLimit   int `json:"mssql_connection_limit"`
	MSSQLConnectionTimeout int `json:"mssql_connection_timeout"`
	MSSQLIdleTimeout       int `json:"mssql_idle_timeout"`
	MSSQLQueryTimeout      int `json:"mssql_query_timeout"`
	MSSQLCatalogCacheTTL   int `json:"mssql_catalog_cache_ttl"`
}

func (s Settings) applySQL() string {
	var sql []string

	if len(s.AllowedDirectories) != 0 {
		sql = append(sql, "SET allowed_directories = "+sqlStringArray(s.AllowedDirectories)+";")
	}

	if len(s.AllowedPaths) != 0 {
		sql = append(sql, "SET allowed_paths = "+sqlStringArray(s.AllowedDirectories)+";")
	}

	if s.EnableLogging {
		sql = append(sql, "SET enable_logging = true;")
	}

	if s.MaxMemory != 0 {
		sql = append(sql, fmt.Sprintf("SET max_memory = '%vGB';", s.MaxMemory))
	}

	if s.MaxTempDirectorySize != 0 {
		sql = append(sql, fmt.Sprintf("SET max_temp_directory_size = '%vGB';", s.MaxTempDirectorySize))
	}

	if s.TempDirectory != "" {
		sql = append(sql, fmt.Sprintf("SET temp_directory = '%s';", s.TempDirectory))
	}

	if s.WorkerThreads != 0 {
		sql = append(sql, fmt.Sprintf("SET worker_threads = %d;", s.WorkerThreads))
	}
	if s.PGConnectionLimit != 0 {
		sql = append(sql, fmt.Sprintf("SET pg_connection_limit = %d;", s.PGConnectionLimit))
	}
	if s.PGPagesPerTask != 0 {
		sql = append(sql, fmt.Sprintf("SET pg_pages_per_task = %d;", s.PGPagesPerTask))
	}

	// MSSQL
	if s.MSSQLConnectionLimit != 0 {
		sql = append(sql, fmt.Sprintf("SET mssql_connection_limit = %d;", s.MSSQLConnectionLimit))
	}
	if s.MSSQLConnectionTimeout != 0 {
		sql = append(sql, fmt.Sprintf("SET mssql_connection_timeout = %d;", s.MSSQLConnectionTimeout))
	}
	if s.MSSQLIdleTimeout != 0 {
		sql = append(sql, fmt.Sprintf("SET mssql_idle_timeout = %d;", s.MSSQLIdleTimeout))
	}
	if s.MSSQLQueryTimeout != 0 {
		sql = append(sql, fmt.Sprintf("SET mssql_query_timeout = %d;", s.MSSQLQueryTimeout))
	}
	if s.MSSQLCatalogCacheTTL != 0 {
		sql = append(sql, fmt.Sprintf("SET mssql_catalog_cache_ttl = %d;", s.MSSQLCatalogCacheTTL))
	}
	sql = append(sql, "SET mssql_order_pushdown = true;")

	if s.HomeDirectory != "" {
		sql = append(sql, fmt.Sprintf("SET home_directory = '%s';", s.HomeDirectory))
		sql = append(sql, fmt.Sprintf("SET secret_directory = '%s/.stored_secrets';", s.HomeDirectory))
	}
	sql = append(sql, "PRAGMA enable_checkpoint_on_shutdown; PRAGMA force_checkpoint;")

	return strings.Join(sql, "\n")
}

func sqlStringArray(values []string) string {
	if len(values) == 0 {
		return "ARRAY[]"
	}
	vv := make([]string, len(values))
	for i, v := range values {
		vv[i] = fmt.Sprintf("'%s'", v)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(vv, ","))
}
