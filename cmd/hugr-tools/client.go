package main

import (
	"flag"

	hugr "github.com/hugr-lab/query-engine"
)

// addGlobalFlags registers global flags on the given FlagSet and returns the parsed globalFlags.
func addGlobalFlags(fs *flag.FlagSet) *globalFlags {
	gf := &globalFlags{}
	fs.StringVar(&gf.URL, "url", envOrDefault("HUGR_URL", "http://localhost:15000/ipc"), "Engine GraphQL endpoint")
	fs.StringVar(&gf.Secret, "secret", envOrDefault("HUGR_SECRET", ""), "API key for authentication")
	fs.StringVar(&gf.SecretHeader, "secret-header", envOrDefault("HUGR_SECRET_HEADER", "x-api-key"), "API key header name")
	fs.DurationVar(&gf.Timeout, "timeout", 30_000_000_000, "Request timeout") // 30s
	return gf
}

// newClient creates a Hugr IPC client from the global flags.
func newClient(gf *globalFlags) *hugr.Client {
	var opts []hugr.Option
	opts = append(opts, hugr.WithTimeout(gf.Timeout))
	if gf.Secret != "" {
		opts = append(opts, hugr.WithApiKeyCustomHeader(gf.Secret, gf.SecretHeader))
	}
	return hugr.NewClient(gf.URL, opts...)
}
