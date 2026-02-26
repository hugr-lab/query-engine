package mssql

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

type Source struct {
	ds         types.DataSource
	isAttached bool

	engine engines.Engine
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewMssql(),
	}, nil
}

func (s *Source) Definition() types.DataSource {
	return s.ds
}

func (s *Source) Name() string {
	return s.ds.Name
}

func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}

func (s *Source) Engine() engines.Engine {
	return s.engine
}

func (*Source) EngineType() engines.Type {
	return engines.TypeDuckDB
}

func (s *Source) IsAttached() bool {
	return s.isAttached
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	// check if db is attached
	err := sources.CheckDBExists(ctx, db, engines.Ident(s.ds.Name), sources.MSSQL)
	if err != nil {
		return err
	}

	p, err := sources.ParseDSN(s.ds.Path)
	if err != nil {
		return err
	}

	secretName := strings.ReplaceAll(strings.ReplaceAll(s.ds.Name, ".", "_"), "-", "_")
	secSQL := ""
	if p.Port == "" {
		p.Port = "1433"
	}
	switch p.Proto {
	case "mssql", "sqlserver":
		if _, ok := p.Params["use_encrypt"]; !ok {
			p.Params["use_encrypt"] = "true"
		}
		secSQL = fmt.Sprintf(`
			CREATE OR REPLACE SECRET _%s_secret (
				TYPE mssql,
				HOST '%s',
				PORT %s,
				DATABASE '%s',
				USER '%s',
				PASSWORD '%s',
				use_encrypt %s
			);`,
			secretName, p.Host, p.Port, p.DBName, p.User, p.Password, p.Params["use_encrypt"],
		)
	case "azure":
		tenantID := trimParam(p.Params["tenant_id"])
		if tenantID == "" {
			return fmt.Errorf("missing tenant_id parameter for azure mssql data source")
		}
		clientID := trimParam(p.Params["client_id"])
		if clientID == "" {
			return fmt.Errorf("missing client_id parameter for azure mssql data source")
		}
		clientSecret := trimParam(p.Params["client_secret"])
		if clientSecret == "" {
			return fmt.Errorf("missing client_secret parameter for azure mssql data source")
		}
		secSQL = fmt.Sprintf(`
			CREATE OR REPLACE SECRET _%s_azure_secret (
				TYPE azure,
				PROVIDER service_principal,
				TENANT_ID '%s',
				CLIENT_ID '%s',
				CLIENT_SECRET '%s'
			);
			CREATE OR REPLACE SECRET _%s_secret (
				TYPE mssql,
				HOST '%s',
				DATABASE '%s',
				PORT %s,
				AZURE_SECRET '_%[1]s_azure_secret'
			);`,
			secretName, tenantID, clientID, clientSecret,
			secretName, p.Host, p.DBName, p.Port,
		)
	}
	readonly := ""
	if s.ds.ReadOnly {
		readonly = ", READ_ONLY"
	}
	_, err = db.Exec(ctx, fmt.Sprintf(`
		%s
		ATTACH '' AS %s (TYPE mssql, SECRET _%s_secret %s);`,
		secSQL,
		engines.Ident(s.ds.Name), secretName, readonly,
	))
	if err != nil {
		return err
	}
	s.isAttached = true

	return nil
}

func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	_, err := db.Exec(ctx, fmt.Sprintf("DETACH %s;", engines.Ident(s.ds.Name)))
	if err != nil {
		return err
	}
	s.isAttached = false

	return nil
}

func trimParam(param string) string {
	if strings.HasPrefix(param, "'") && strings.HasSuffix(param, "'") {
		return param[1 : len(param)-1]
	}
	if strings.HasPrefix(param, "\"") && strings.HasSuffix(param, "\"") {
		return param[1 : len(param)-1]
	}
	return param
}
