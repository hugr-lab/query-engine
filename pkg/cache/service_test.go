package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
)

func ctxUser(role, userID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Role: role, UserId: userID})
}

func TestFormatKey_Identity(t *testing.T) {
	s := &Service{}

	// Distinct identities must produce distinct keys; the same identity a
	// stable one.
	a := s.formatKey(ctxUser("agent", "u1"), "q")
	a2 := s.formatKey(ctxUser("agent", "u1"), "q")
	b := s.formatKey(ctxUser("agent", "u2"), "q")
	if a != a2 {
		t.Errorf("same identity not stable: %q vs %q", a, a2)
	}
	if a == b {
		t.Errorf("same-role different-user keys collided: %q", a)
	}

	// Role is part of the identity (same user id, different role differs).
	if s.formatKey(ctxUser("admin", "u1"), "q") == a {
		t.Errorf("role not part of the key")
	}

	// The ':' delimiter must not let identity components collide:
	// (role="a", user="b:c") must differ from (role="a:b", user="c").
	if s.formatKey(ctxUser("a", "b:c"), "q") == s.formatKey(ctxUser("a:b", "c"), "q") {
		t.Errorf("delimiter collision between (a, b:c) and (a:b, c)")
	}

	// The trailing key still varies the result.
	if s.formatKey(ctxUser("agent", "u1"), "q") == s.formatKey(ctxUser("agent", "u1"), "other") {
		t.Errorf("different query keys must differ")
	}

	// No AuthInfo → bare key.
	if got := s.formatKey(context.Background(), "q"); got != "q" {
		t.Errorf("formatKey without auth = %q, want %q", got, "q")
	}

	// Anonymous (role=public, user=anonymous per AnonymousProvider) is stable
	// and shared across anonymous callers, but distinct from other identities.
	an1 := s.formatKey(ctxUser("public", "anonymous"), "q")
	an2 := s.formatKey(ctxUser("public", "anonymous"), "q")
	if an1 != an2 || an1 == a {
		t.Errorf("anonymous key unstable or collided: %q / %q", an1, an2)
	}
}

func TestFormatKey_FullAccess(t *testing.T) {
	s := &Service{}
	fa := s.formatKey(auth.ContextWithFullAccess(ctxUser("agent", "u1")), "q")
	if fa != "fa:q" {
		t.Errorf("full-access key = %q, want %q", fa, "fa:q")
	}
	// Full-access is identity-independent: a different caller's full-access
	// request for the same key shares the namespace.
	if other := s.formatKey(auth.ContextWithFullAccess(ctxUser("admin", "u9")), "q"); other != fa {
		t.Errorf("full-access keys differ by identity: %q vs %q", other, fa)
	}
	// ... and differs from a non-full-access request of the same identity.
	if s.formatKey(ctxUser("agent", "u1"), "q") == fa {
		t.Errorf("full-access and normal request share a key")
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	s := New(Config{
		L1: L1Config{Enabled: true, MaxSize: 8, MaxItemSize: 1 << 20, EvictionTime: types.Interval(time.Minute)},
	})
	if err := s.Init(context.Background()); err != nil {
		t.Fatalf("cache Init: %v", err)
	}
	return s
}

// Regression for the singleflight leak: two concurrent identical @cache'd
// queries from DIFFERENT identities must each run their own fn and get their
// own result. Under a bare (un-namespaced) singleflight key they would coalesce
// and one caller would receive the other's result.
func TestLoad_SingleflightPerIdentity(t *testing.T) {
	s := newTestService(t)

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	fn := func(id string) func() (any, error) {
		return func() (any, error) {
			entered <- struct{}{}
			<-release // hold in-flight until BOTH callers have entered
			return id, nil
		}
	}

	var wg sync.WaitGroup
	var resA, resB any
	wg.Add(2)
	go func() { defer wg.Done(); resA, _ = s.Load(ctxUser("agent", "a"), "k", fn("A")) }()
	go func() { defer wg.Done(); resB, _ = s.Load(ctxUser("agent", "b"), "k", fn("B")) }()

	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("only one fn ran — concurrent identities coalesced (singleflight used a bare key)")
		}
	}
	close(release)
	wg.Wait()

	if resA != "A" || resB != "B" {
		t.Fatalf("resA=%v resB=%v, want A/B (identities must not share a singleflight result)", resA, resB)
	}
}

// Same identity + same key MUST coalesce (that is the point of singleflight):
// one fn runs, both callers get its result.
func TestLoad_SingleflightSameIdentityCoalesces(t *testing.T) {
	s := newTestService(t)

	var calls int
	var mu sync.Mutex
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	fn := func() (any, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		started <- struct{}{}
		<-release
		return "shared", nil
	}

	ctx := ctxUser("agent", "same")
	var wg sync.WaitGroup
	var r1, r2 any
	wg.Add(2)
	go func() { defer wg.Done(); r1, _ = s.Load(ctx, "k", fn) }()
	// ensure the first is in-flight before the second joins the group
	<-started
	go func() { defer wg.Done(); r2, _ = s.Load(ctx, "k", fn) }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if r1 != "shared" || r2 != "shared" {
		t.Fatalf("r1=%v r2=%v, want both shared", r1, r2)
	}
	if calls != 1 {
		t.Fatalf("fn ran %d times, want 1 (same identity must coalesce)", calls)
	}
}
