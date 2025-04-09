package db

import (
	"context"
	"database/sql"
	"sync/atomic"
)

type contextTxKey string

var txKey contextTxKey = "contextTx"

type txContext struct {
	*Connection
	tx    *sql.Tx
	level atomic.Int32
}

func (p *Pool) WithTx(parent context.Context) (context.Context, error) {
	tx := parent.Value(txKey)
	if tx != nil {
		txc := tx.(*txContext)
		txc.level.Add(1)
		return parent, nil
	}
	c, err := p.Conn(parent)
	if err != nil {
		return nil, err
	}
	dbTx, err := c.conn.BeginTx(parent, nil)
	if err != nil {
		c.Close()
		return nil, err
	}
	c.tx = dbTx
	txc := &txContext{
		Connection: c,
		tx:         dbTx,
	}
	txc.level.Store(1)
	ctx := context.WithValue(parent, txKey, txc)
	return ctx, nil
}

func (p *Pool) Commit(ctx context.Context) error {
	tx := ctx.Value(txKey)
	if tx == nil {
		return nil
	}
	txc := tx.(*txContext)
	if !txc.level.CompareAndSwap(1, 0) {
		txc.level.Add(-1)
		return nil
	}
	err := txc.tx.Commit()
	txc.Connection.tx = nil
	txc.Connection.Close()
	return err
}

func (p *Pool) Rollback(ctx context.Context) error {
	tx := ctx.Value(txKey)
	if tx == nil {
		return nil
	}
	txc := tx.(*txContext)
	if txc.level.CompareAndSwap(0, 0) {
		return nil
	}
	if !txc.level.CompareAndSwap(1, 0) {
		txc.level.Add(-1)
		return nil
	}
	if txc.tx == nil {
		return nil
	}
	err := txc.tx.Rollback()
	txc.Connection.tx = nil
	txc.Connection.Close()
	return err
}

func (p *Pool) IsTxContext(ctx context.Context) bool {
	tx := ctx.Value(txKey)
	if tx == nil {
		return false
	}
	txc := tx.(*txContext)
	return txc.Connection.tx != nil
}
