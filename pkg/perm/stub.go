package perm

import "context"

// StubStore is a permissive permission store used when RBAC is not configured.
// It allows all operations including impersonation — no RBAC means trusted environment.
type StubStore struct{}

func (s *StubStore) ContextWithPermissions(ctx context.Context) (context.Context, error) {
	role, err := s.RolePermissions(ctx)
	if err != nil {
		return nil, err
	}
	return CtxWithPerm(ctx, &role), nil
}

func (s *StubStore) RolePermissions(_ context.Context) (RolePermissions, error) {
	return RolePermissions{
		Name:           "admin",
		CanImpersonate: true,
	}, nil
}
