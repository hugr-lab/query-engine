package main

import (
	"fmt"
	"os"
	"time"
)

// Global flags shared by all subcommands.
type globalFlags struct {
	URL          string
	Secret       string
	SecretHeader string
	Timeout      time.Duration
}

func main() {
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
	fmt.Fprintln(os.Stderr, `hugr-tools — Hugr schema management utilities

Usage: hugr-tools <command> [flags]

Commands:
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
