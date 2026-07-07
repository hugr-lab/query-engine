package cache

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
)

func TestFormatKey_PerUser(t *testing.T) {
	s := &Service{}
	ctxFor := func(role, userID string) context.Context {
		return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Role: role, UserId: userID})
	}

	// The key must carry both role and user id.
	if got, want := s.formatKey(ctxFor("agent", "u1"), "q"), "agent:u1:q"; got != want {
		t.Errorf("formatKey = %q, want %q", got, want)
	}

	// Two users of the SAME role must get different keys (the leak this fixes).
	a := s.formatKey(ctxFor("agent", "u1"), "q")
	b := s.formatKey(ctxFor("agent", "u2"), "q")
	if a == b {
		t.Errorf("same-role different-user keys collided: both %q", a)
	}

	// The SAME user must get a stable key.
	if a2 := s.formatKey(ctxFor("agent", "u1"), "q"); a2 != a {
		t.Errorf("same user key not stable: %q vs %q", a, a2)
	}

	// A different role for the same user id must differ (impersonation reuses ids).
	if s.formatKey(ctxFor("admin", "u1"), "q") == a {
		t.Errorf("role is not part of the key")
	}

	// No AuthInfo → bare key (no identity prefix).
	if got := s.formatKey(context.Background(), "q"); got != "q" {
		t.Errorf("formatKey without auth = %q, want %q", got, "q")
	}

	// Anonymous (empty user id) → shared key across anonymous callers.
	anon1 := s.formatKey(ctxFor("public", ""), "q")
	anon2 := s.formatKey(ctxFor("public", ""), "q")
	if anon1 != anon2 || anon1 != "public::q" {
		t.Errorf("anonymous keys = %q / %q, want stable %q", anon1, anon2, "public::q")
	}
}
