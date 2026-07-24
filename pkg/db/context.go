package db

import (
	"context"
	"database/sql"
	"sync/atomic"
)

type contextTxKey string

var txKey contextTxKey = "contextTx"

type timezoneKey struct{}

// ContextWithTimezone returns a new context with the given IANA timezone identifier.
func ContextWithTimezone(ctx context.Context, tz string) context.Context {
	return context.WithValue(ctx, timezoneKey{}, tz)
}

// TimezoneFromCtx extracts the timezone identifier from the context, or returns "" if not set.
func TimezoneFromCtx(ctx context.Context) string {
	if tz, ok := ctx.Value(timezoneKey{}).(string); ok {
		return tz
	}
	return ""
}

// txContext is the ONE physical transaction shared by a WithTx scope and every
// nested WithTx joined to it — DuckDB has no nested transactions, so nesting is
// FLATTENED onto a single *sql.Tx. level counts the live holders; a COMMIT is
// physical only when the last holder commits, while a ROLLBACK at ANY level
// aborts the whole flattened tx (there are no savepoints to undo just part of
// it). settled records that the physical tx has terminated, so once a rollback
// poisons it every later Commit/Rollback — here or in an enclosing frame —
// no-ops instead of acting on a finished tx.
type txContext struct {
	*Connection
	tx      *sql.Tx
	level   atomic.Int32
	settled atomic.Bool
}

// txFrame is one WithTx scope. done makes that scope's terminal call
// idempotent: the FIRST Commit or Rollback on the frame runs, any later one is
// a no-op. This makes the idiomatic `defer Rollback(); …; Commit()` safe even
// when nested — the deferred Rollback that fires after a successful Commit does
// NOTHING, instead of decrementing the shared counter a second time and (since
// the counter would then read 1) physically rolling back the PARENT's flattened
// transaction. Each WithTx call gets its own frame in a distinct child context;
// all frames of a scope share the one txContext.
type txFrame struct {
	txc  *txContext
	done atomic.Bool
}

func txFrameFrom(ctx context.Context) *txFrame {
	if v := ctx.Value(txKey); v != nil {
		return v.(*txFrame)
	}
	return nil
}

func ClearTxContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, txKey, nil)
}

func (p *Pool) WithTx(parent context.Context) (context.Context, error) {
	if f := txFrameFrom(parent); f != nil {
		f.txc.level.Add(1)
		return context.WithValue(parent, txKey, &txFrame{txc: f.txc}), nil
	}
	c, err := p.Conn(parent)
	if err != nil {
		return nil, err
	}
	dbTx, err := c.conn.BeginTx(parent, nil)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	c.tx = dbTx
	txc := &txContext{
		Connection: c,
		tx:         dbTx,
	}
	txc.level.Store(1)
	return context.WithValue(parent, txKey, &txFrame{txc: txc}), nil
}

func (p *Pool) Commit(ctx context.Context) error {
	f := txFrameFrom(ctx)
	if f == nil || f.done.Swap(true) {
		return nil
	}
	txc := f.txc
	// A nested commit is only a vote — the physical commit happens when the LAST
	// holder commits, and only if no rollback has poisoned the flattened tx.
	// Committing work a rollback has already thrown away must NOT look like
	// success — report ErrTxDone so the caller knows nothing was persisted.
	if txc.level.Add(-1) > 0 {
		if txc.settled.Load() {
			return sql.ErrTxDone
		}
		return nil
	}
	if txc.settled.Swap(true) {
		return sql.ErrTxDone
	}
	err := txc.tx.Commit()
	txc.Connection.tx = nil
	_ = txc.Close()
	return err
}

func (p *Pool) Rollback(ctx context.Context) error {
	f := txFrameFrom(ctx)
	if f == nil || f.done.Swap(true) {
		return nil
	}
	txc := f.txc
	txc.level.Add(-1)
	// A rollback at ANY level aborts the whole flattened tx — roll back once and
	// poison it; every later terminal (here or in an enclosing frame) no-ops.
	if txc.settled.Swap(true) || txc.tx == nil {
		return nil
	}
	err := txc.tx.Rollback()
	txc.Connection.tx = nil
	_ = txc.Close()
	return err
}

func (p *Pool) IsTxContext(ctx context.Context) bool {
	f := txFrameFrom(ctx)
	return f != nil && f.txc.Connection.tx != nil
}
