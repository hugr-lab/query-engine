package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

func runReindex(args []string) error {
	fs := flag.NewFlagSet("reindex", flag.ExitOnError)
	gf := addGlobalFlags(fs)

	name := fs.String("name", "", "Reindex single type (+ its fields); empty = all")
	batchSize := fs.Int("batch-size", 50, "Batch size (max 200)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Recompute vector embeddings for schema entities.

Usage: hugr-tools reindex [flags]

Flags:`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *batchSize < 1 || *batchSize > 200 {
		return fmt.Errorf("batch-size must be between 1 and 200, got %d", *batchSize)
	}

	client := newClient(gf)
	ctx := context.Background()

	fmt.Fprintf(os.Stderr, "Reindexing: %s (batch size: %d)\n",
		nameOrAll(*name), *batchSize)

	res, err := client.Query(ctx, `mutation($name: String, $batch_size: Int) {
		function {
			core {
				_schema_reindex(name: $name, batch_size: $batch_size) {
					success
					affected_rows
				}
			}
		}
	}`, map[string]any{
		"name":       *name,
		"batch_size": *batchSize,
	})
	if err != nil {
		return fmt.Errorf("reindex query failed: %w", err)
	}
	defer res.Close()
	if res.Err() != nil {
		return res.Err()
	}

	var result struct {
		Success bool `json:"success"`
		AffectedRows int    `json:"affected_rows"`
	}
	if err := res.ScanData("function.core._schema_reindex", &result); err != nil {
		return fmt.Errorf("scan result: %w", err)
	}

	fmt.Fprintf(os.Stdout, "Updated: %d embeddings\n", result.AffectedRows)
	return nil
}

func nameOrAll(name string) string {
	if name == "" {
		return "all entities"
	}
	return name
}
