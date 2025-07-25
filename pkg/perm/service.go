package perm

import (
	"context"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/types"
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

	fc := auth.ContextWithFullAccess(ctx)
	res, err := s.qe.Query(fc, `query ($role: String!, $cacheKey: String) {
		core {
			info: roles_by_pk (name: $role) @cache(key: $cacheKey, tags: ["$role_permissions"]) {
				name
				disabled
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
	`, map[string]any{"role": info.Role, "cacheKey": "RolePermissions:" + info.Role})
	defer res.Close()
	if errors.Is(err, types.ErrNoData) {
		return RolePermissions{}, auth.ErrForbidden
	}
	if err != nil {
		return RolePermissions{}, err
	}
	var role RolePermissions
	err = res.ScanData("core.info", &role)

	return role, err
}
