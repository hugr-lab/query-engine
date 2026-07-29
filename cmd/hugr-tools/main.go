// Command hugr-tools is DEPRECATED and is moving to the hub; it is no longer
// developed here.
//
// It has not been ported to the catalog storage introduced in CoreDB 0.0.20:
// every command still reads the compiled-schema views (core.catalog.types,
// core.catalog.modules, core.catalog.module_intro) and calls the _schema_*
// mutation functions, none of which exist any more. The code compiles, but the
// commands fail at run time against a current engine. The engine-side
// replacements are the core.catalog.annotate_* functions, the core.entity_*
// views and the MCP catalog-* tools.
package main

import (
	"fmt"
	"os"
	"time"
)

// deprecationNotice is printed before every command: the binary still builds
// and ships, so a user has no other way to learn that it is retired.
const deprecationNotice = `hugr-tools is DEPRECATED and is moving to the hub.

It has not been ported to the catalog storage of CoreDB 0.0.20 — it reads the
compiled-schema views and _schema_* functions that version removed, so the
commands below fail against a current engine. Use core.catalog.annotate_*, the
core.entity_* views or the MCP catalog-* tools instead.
`

// Global flags shared by all subcommands.
type globalFlags struct {
	URL          string
	Secret       string
	SecretHeader string
	Timeout      time.Duration
}

func main() {
	fmt.Fprintln(os.Stderr, deprecationNotice)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "summarize":
		if err := runSummarize(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "reindex":
		if err := runReindex(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "schema-info":
		if err := runSchemaInfo(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `hugr-tools — Hugr schema management utilities (DEPRECATED)

Usage: hugr-tools <command> [flags]

Commands (all currently broken against CoreDB 0.0.20):
  summarize    AI-powered schema summarization using LLM
  reindex      Recompute vector embeddings for schema entities
  schema-info  Display human-readable schema overview

Global flags (available for all commands):
  --url              Engine GraphQL endpoint (env: HUGR_URL, default: http://localhost:15000/ipc)
  --secret           API key for authentication (env: HUGR_SECRET)
  --secret-header    API key header name (env: HUGR_SECRET_HEADER, default: x-api-key)
  --timeout          Request timeout (default: 30s)

Run 'hugr-tools <command> --help' for more information on a command.`)
}

// envOrDefault returns the environment variable value if set, otherwise the default.
func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
