package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	"github.com/marcboeker/go-duckdb/v2"
)

type Config struct {
	Path         string `json:"path"`
	MaxOpenConns int    `json:"max_open_conns"`
	MaxIdleConns int    `json:"max_idle_conns"`

	Settings Settings `json:"settings"`
}

func Connect(ctx context.Context, config Config) (*Pool, error) {
	pool, err := NewPool(config.Path)
	if err != nil {
		return nil, err
	}

	pool.SetMaxOpenConns(config.MaxOpenConns)
	pool.SetMaxIdleConns(config.MaxIdleConns)

	// load extensions
	_, err = pool.Exec(ctx, `
		INSTALL postgres; LOAD postgres;
		INSTALL spatial; LOAD spatial;
		INSTALL httpfs; LOAD httpfs;
	`)
	if err != nil {
		return nil, err
	}

	// set settings
	sql := config.Settings.applySQL()
	if sql != "" {
		_, err = pool.Exec(ctx, sql)
		if err != nil {
			return nil, err
		}
	}

	return pool, nil
}

type Pool struct {
	connector *duckdb.Connector
	db        *sql.DB

	mu            sync.Mutex
	maxConnection int
	openConns     int
}

func NewPool(path string) (*Pool, error) {
	connector, err := duckdb.NewConnector(path, nil)
	if err != nil {
		return nil, err
	}

	conn := sql.OpenDB(connector)

	return &Pool{
		connector: connector,
		db:        conn,
	}, nil
}

func (p *Pool) Connector() *duckdb.Connector {
	return p.connector
}

func (p *Pool) SetMaxIdleConns(n int) {
	p.db.SetMaxIdleConns(n)
}

func (p *Pool) SetMaxOpenConns(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.db.SetMaxOpenConns(n)
	p.maxConnection = n
}

func (p *Pool) Close() error {
	err := p.db.Close()
	if err != nil {
		return err
	}
	return p.connector.Close()
}

func (p *Pool) acquire(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.mu.Lock()
			if p.openConns < p.maxConnection || p.maxConnection == 0 {
				p.openConns++
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
		}
	}
}

func (p *Pool) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.openConns--
}

func (p *Pool) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	conn, err := p.Conn(ctx)
	if err != nil {
		return nil, err
	}
	res, err := conn.Exec(ctx, query, args...)

	return res, err
}

func (p *Pool) Conn(ctx context.Context) (*Connection, error) {
	if tx := ctx.Value(txKey); tx != nil {
		return tx.(*txContext).Connection, nil
	}
	// wait for available connection
	p.acquire(ctx)
	conn, err := p.db.Conn(ctx)
	if err != nil {
		p.release()
		return nil, err
	}
	return &Connection{
		conn:    conn,
		release: p.release,
	}, nil
}

func (p *Pool) Arrow(ctx context.Context) (*Arrow, error) {
	// wait for available connection
	p.acquire(ctx)
	conn, err := p.connector.Connect(ctx)
	if err != nil {
		p.release()
		return nil, err
	}
	arrow, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		p.release()
		return nil, err
	}
	return &Arrow{
		Arrow:   arrow,
		drv:     conn,
		release: p.release,
	}, nil
}

type Connection struct {
	conn    *sql.Conn
	tx      *sql.Tx
	release func()
}

func (c *Connection) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if c.tx != nil {
		return c.tx.QueryContext(ctx, query, args...)
	}
	return c.conn.QueryContext(ctx, query, args...)
}

func (c *Connection) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	if c.tx != nil {
		return c.tx.QueryRowContext(ctx, query, args...)
	}
	return c.conn.QueryRowContext(ctx, query, args...)
}

func (c *Connection) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if c.tx != nil {
		return c.tx.ExecContext(ctx, query, args...)
	}
	return c.conn.ExecContext(ctx, query, args...)
}

func (c *Connection) DBConn() *sql.Conn {
	return c.conn
}

func (c *Connection) Close() error {
	if c.tx != nil {
		return nil
	}
	defer c.release()
	err := c.conn.Close()
	return err
}

type Arrow struct {
	*duckdb.Arrow
	drv     driver.Conn
	release func()
}

func (a *Arrow) Close() error {
	defer a.release()
	return a.drv.Close()
}
