package db

import (
	"context"
	"testing"
	"time"
)

func TestPool_WithTx(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	parent := context.Background()

	ctx, err := pool.WithTx(parent)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ctx == nil {
		t.Fatalf("expected non-nil context")
	}
	if !pool.IsTxContext(ctx) {
		t.Fatalf("expected false, got true")
	}

	// A nested WithTx joins the same physical tx via its OWN frame.
	ctx2, err := pool.WithTx(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(ctx2) {
		t.Fatalf("expected nested context to be a tx context")
	}
	// The inner commit is only a vote — the physical tx stays open until the
	// owner terminates.
	if err = pool.Commit(ctx2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(ctx) {
		t.Fatalf("tx must stay open after an inner commit")
	}
	// The owner commit finalizes the physical tx.
	if err = pool.Commit(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool.IsTxContext(ctx) {
		t.Fatalf("tx must be closed after the owner commit")
	}
}

func TestPool_Commit(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	parent := context.Background()

	ctx, err := pool.WithTx(parent)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = pool.Commit(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool.IsTxContext(ctx) {
		t.Fatalf("expected false, got true")
	}
}

func TestPool_Rollback(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	parent := context.Background()

	ctx, err := pool.WithTx(parent)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = pool.Rollback(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool.IsTxContext(ctx) {
		t.Fatalf("expected false, got true")
	}
}

func TestPool_IsTxContext(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	parent := context.Background()

	if pool.IsTxContext(parent) {
		t.Fatalf("expected false, got true")
	}
	ctx, err := pool.WithTx(parent)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(ctx) {
		t.Fatalf("expected true, got false")
	}
	err = pool.Rollback(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool.IsTxContext(ctx) {
		t.Fatalf("expected false, got true")
	}
}

func TestPool_ChainContext(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	parent := context.Background()

	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	// Create a context with a transaction
	txCtx, err := pool.WithTx(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(txCtx) {
		t.Fatalf("expected true, got false")
	}

	// Create an intermediate context
	intermediateCtx := context.WithValue(txCtx, "key", "value")
	if !pool.IsTxContext(intermediateCtx) {
		t.Fatalf("expected true, got false")
	}

	// Create a nested transaction context
	nestedTxCtx, err := pool.WithTx(intermediateCtx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(nestedTxCtx) {
		t.Fatalf("expected true, got false")
	}

	// Commit the nested transaction — a vote; the physical tx stays open.
	err = pool.Commit(nestedTxCtx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(nestedTxCtx) {
		t.Fatalf("expected true, got false")
	}

	// The owner terminates the physical tx (through the WithValue chain).
	err = pool.Rollback(txCtx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool.IsTxContext(nestedTxCtx) {
		t.Fatalf("expected false, got true")
	}
}

// TestPool_NestedTxSemantics pins the two guarantees the frame model provides
// on the flattened (DuckDB has no nested tx) transaction: the idiomatic
// `defer Rollback(); …; Commit()` is safe when nested (the deferred Rollback
// no-ops after the commit), and a rollback at ANY level aborts the WHOLE tx.
func TestPool_NestedTxSemantics(t *testing.T) {
	ctx := context.Background()
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	c0, _ := pool.Conn(ctx)
	if _, err := c0.Exec(ctx, "CREATE TABLE t(x INT)"); err != nil {
		t.Fatal(err)
	}
	c0.Close()

	count := func() int {
		c, _ := pool.Conn(ctx)
		defer c.Close()
		var n int
		_ = c.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&n)
		return n
	}
	exec := func(c context.Context, q string) {
		conn, _ := pool.Conn(c)
		defer conn.Close()
		if _, err := conn.Exec(c, q); err != nil {
			t.Fatal(err)
		}
	}

	// (a) Nested `defer Rollback + Commit` on success commits everything.
	func() {
		owner, _ := pool.WithTx(ctx)
		defer pool.Rollback(owner)
		exec(owner, "INSERT INTO t VALUES (1)")
		func() {
			inner, _ := pool.WithTx(owner)
			defer pool.Rollback(inner)
			exec(inner, "INSERT INTO t VALUES (2)")
			_ = pool.Commit(inner) // deferred Rollback below must no-op
		}()
		_ = pool.Commit(owner)
	}()
	if got := count(); got != 2 {
		t.Fatalf("nested commit idiom: want 2 rows, got %d", got)
	}
	exec(ctx, "DELETE FROM t")

	// (b) A rollback at the inner level aborts the whole flattened tx.
	func() {
		owner, _ := pool.WithTx(ctx)
		defer pool.Rollback(owner)
		exec(owner, "INSERT INTO t VALUES (1)")
		abort := func() error {
			inner, _ := pool.WithTx(owner)
			defer pool.Rollback(inner) // no Commit → rolls back
			exec(inner, "INSERT INTO t VALUES (2)")
			return context.Canceled
		}()
		if abort != nil {
			return // owner aborts; its deferred Rollback no-ops (already poisoned)
		}
		_ = pool.Commit(owner)
	}()
	if got := count(); got != 0 {
		t.Fatalf("inner rollback must abort the whole tx: want 0 rows, got %d", got)
	}
}
