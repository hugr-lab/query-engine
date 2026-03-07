package hugr

// Re-exports from client/ package for backward compatibility.
// New code should import "github.com/hugr-lab/query-engine/client" directly.

import "github.com/hugr-lab/query-engine/client"

type Client = client.Client
type Option = client.Option
type ClientConfig = client.ClientConfig

var (
	NewClient                = client.NewClient
	WithApiKey               = client.WithApiKey
	WithApiKeyCustomHeader   = client.WithApiKeyCustomHeader
	WithUserRole             = client.WithUserRole
	WithUserRoleCustomHeader = client.WithUserRoleCustomHeader
	WithUserInfo             = client.WithUserInfo
	WithUserInfoCustomHeader = client.WithUserInfoCustomHeader
	WithTimeout              = client.WithTimeout
	WithTransport            = client.WithTransport
	WithToken                = client.WithToken
	WithHttpUrl              = client.WithHttpUrl
	WithJQQueryUrl           = client.WithJQQueryUrl
)
