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

	// Test nested transaction
	ctx2, err := pool.WithTx(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ctx != ctx2 {
		t.Fatalf("expected contexts to be equal")
	}
	err = pool.Commit(ctx2)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(ctx) {
		t.Fatalf("expected false, got true")
	}
	err = pool.Commit(ctx2)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool.IsTxContext(ctx) {
		t.Fatalf("expected true, got false")
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

	// Commit the nested transaction
	err = pool.Commit(nestedTxCtx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !pool.IsTxContext(nestedTxCtx) {
		t.Fatalf("expected true, got false")
	}

	// Rollback the transaction
	err = pool.Rollback(nestedTxCtx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool.IsTxContext(nestedTxCtx) {
		t.Fatalf("expected false, got true")
	}
}
