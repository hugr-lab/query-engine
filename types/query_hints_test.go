package types

import (
	"context"
	"testing"
)

func TestQueryHints_Empty(t *testing.T) {
	ctx := context.Background()
	if IsNoMutationContext(ctx) {
		t.Error("empty context must not be a no-mutation context")
	}
	if IsValidateOnlyContext(ctx) {
		t.Error("empty context must not be a validate-only context")
	}
}

func TestNoMutationHint(t *testing.T) {
	ctx := ContextWithQueryHint(context.Background(), NoMutationHint())
	if !IsNoMutationContext(ctx) {
		t.Error("NoMutationHint must mark the context as no-mutation")
	}
	// NoMutation and ValidateOnly are independent.
	if IsValidateOnlyContext(ctx) {
		t.Error("NoMutationHint must not imply validate-only")
	}
}

func TestValidateOnlyHint(t *testing.T) {
	ctx := ContextWithQueryHint(context.Background(), ValidateOnlyHint())
	if !IsValidateOnlyContext(ctx) {
		t.Error("ValidateOnlyHint must mark the context as validate-only")
	}
	if IsNoMutationContext(ctx) {
		t.Error("ValidateOnlyHint must not imply no-mutation")
	}
}

func TestQueryHints_Composed(t *testing.T) {
	ctx := ContextWithQueryHint(context.Background(), ValidateOnlyHint(), NoMutationHint())
	if !IsValidateOnlyContext(ctx) {
		t.Error("composed hints must set validate-only")
	}
	if !IsNoMutationContext(ctx) {
		t.Error("composed hints must set no-mutation")
	}
}

func TestContextWithQueryHint_NoHints(t *testing.T) {
	parent := context.Background()
	ctx := ContextWithQueryHint(parent)
	if IsNoMutationContext(ctx) || IsValidateOnlyContext(ctx) {
		t.Error("ContextWithQueryHint with no hints must leave the context unchanged")
	}
}
