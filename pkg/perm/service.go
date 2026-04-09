package perm

import (
	"context"
	"errors"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/types"
)

// Store manager that provides methods to to retrieve Role permissions info from context.
type Store interface {
	// ContextWithPermissions returns a new context with the permissions of the user.
	ContextWithPermissions(ctx context.Context) (context.Context, error)
	RolePermissions(ctx context.Context) (RolePermissions, error)
}

type Service struct {
	qe types.Querier
}

func New(qe types.Querier) *Service {
	return &Service{qe: qe}
}

func (s *Service) ContextWithPermissions(ctx context.Context) (context.Context, error) {
	role, err := s.RolePermissions(ctx)
	if err != nil {
		return nil, err
	}
	return CtxWithPerm(ctx, &role), nil
}

func (s *Service) RolePermissions(ctx context.Context) (RolePermissions, error) {
	if auth.IsFullAccess(ctx) {
		return RolePermissions{
			Name: "admin",
		}, nil
	}
	info := auth.AuthInfoFromContext(ctx)
	if info == nil {
		return RolePermissions{}, auth.ErrForbidden
	}

	// If impersonation is active, verify the original role is authorized to impersonate.
	if info.ImpersonatedBy != nil {
		origRole, err := s.loadRole(ctx, info.ImpersonatedBy.Role)
		if err != nil {
			return RolePermissions{}, fmt.Errorf("impersonation failed: original role %q: %w", info.ImpersonatedBy.Role, err)
		}
		if !origRole.CanImpersonate {
			return RolePermissions{}, fmt.Errorf("role %q is not authorized to impersonate: %w", info.ImpersonatedBy.Role, auth.ErrForbidden)
		}
		targetRole, err := s.loadRole(ctx, info.Role)
		if err != nil {
			return RolePermissions{}, fmt.Errorf("impersonation failed: target role %q not found: %w", info.Role, err)
		}
		return targetRole, nil
	}

	return s.loadRole(ctx, info.Role)
}

func (s *Service) loadRole(ctx context.Context, roleName string) (RolePermissions, error) {
	fc := auth.ContextWithFullAccess(ctx)
	res, err := s.qe.Query(fc, `query ($role: String!, $cacheKey: String) {
		core {
			info: roles_by_pk (name: $role) @cache(key: $cacheKey, tags: ["$role_permissions"]) {
				name
				disabled
				can_impersonate
				permissions {
					type_name
					field_name
					hidden
					disabled
					filter
					data
				}
			}
		}
	}
	`, map[string]any{"role": roleName, "cacheKey": "RolePermissions:" + roleName})
	if errors.Is(err, types.ErrNoData) {
		return RolePermissions{}, auth.ErrForbidden
	}
	defer res.Close()
	if err != nil {
		return RolePermissions{}, err
	}
	var role RolePermissions
	err = res.ScanData("core.info", &role)
	if err != nil {
		return RolePermissions{}, err
	}
	if role.Disabled {
		return RolePermissions{}, auth.ErrForbidden
	}
	return role, nil
}
