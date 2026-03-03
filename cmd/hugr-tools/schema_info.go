package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	hugr "github.com/hugr-lab/query-engine"
)

func runSchemaInfo(args []string) error {
	fs := flag.NewFlagSet("schema-info", flag.ExitOnError)
	gf := addGlobalFlags(fs)

	module := fs.String("module", "", "Module to inspect (default: root)")
	format := fs.String("format", "table", "Output format: table, json")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Display human-readable schema overview.

Usage: hugr-tools schema-info [flags]

Flags:`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	client := newClient(gf)
	ctx := context.Background()

	info, err := fetchSchemaInfo(ctx, client, *module)
	if err != nil {
		return err
	}

	switch *format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(info)
	case "table":
		printSchemaTable(info, *module)
		return nil
	default:
		return fmt.Errorf("unknown format: %s (supported: table, json)", *format)
	}
}

type schemaInfo struct {
	Module     string      `json:"module"`
	Tables     []typeEntry `json:"tables,omitempty"`
	Views      []typeEntry `json:"views,omitempty"`
	Functions  []funcEntry `json:"functions,omitempty"`
	Submodules []subModule `json:"submodules,omitempty"`
}

type typeEntry struct {
	Name       string `json:"name"`
	FieldCount int    `json:"field_count"`
	Catalog    string `json:"catalog"`
}

type funcEntry struct {
	Name    string `json:"name"`
	Catalog string `json:"catalog"`
}

type subModule struct {
	Name       string `json:"name"`
	TableCount int    `json:"table_count"`
	FuncCount  int    `json:"function_count"`
}

func fetchSchemaInfo(ctx context.Context, client *hugr.Client, module string) (*schemaInfo, error) {
	// Fetch types (tables + views) with optional module filter.
	filter := map[string]any{}
	if module != "" {
		filter["module"] = map[string]any{"eq": module}
	}
	filter["hugr_type"] = map[string]any{"in": []string{"table", "view"}}

	typesRes, err := client.Query(ctx, `query($filter: core_catalog_types_filter) {
		core {
			catalog {
				types(filter: $filter) {
					name
					hugr_type
					catalog
				}
			}
		}
	}`, map[string]any{"filter": filter})
	if err != nil {
		return nil, fmt.Errorf("query types: %w", err)
	}
	defer typesRes.Close()
	if typesRes.Err() != nil {
		return nil, typesRes.Err()
	}

	var types []struct {
		Name     string `json:"name"`
		HugrType string `json:"hugr_type"`
		Catalog  string `json:"catalog"`
		Fields   *struct {
			Count int `json:"count"`
		} `json:"fields"`
	}
	_ = typesRes.ScanData("core.catalog.types", &types)

	// Fetch module intro for functions.
	introFilter := map[string]any{}
	if module != "" {
		introFilter["module"] = map[string]any{"eq": module}
	}
	introFilter["type_type"] = map[string]any{"in": []string{"function", "mutation_function"}}

	introRes, err := client.Query(ctx, `query($filter: core_catalog_module_intro_filter) {
		core {
			catalog {
				module_intro(filter: $filter) {
					field_name
					catalog
				}
			}
		}
	}`, map[string]any{"filter": introFilter})
	if err != nil {
		return nil, fmt.Errorf("query module_intro: %w", err)
	}
	defer introRes.Close()
	if introRes.Err() != nil {
		return nil, introRes.Err()
	}

	var intros []struct {
		FieldName string `json:"field_name"`
		Catalog   string `json:"catalog"`
	}
	_ = introRes.ScanData("core.catalog.module_intro", &intros)

	// Fetch submodules.
	modulesRes, err := client.Query(ctx, `query {
		core {
			catalog {
modules {
					name
				}
			}
		}
	}`, nil)
	if err != nil {
		return nil, fmt.Errorf("query modules: %w", err)
	}
	defer modulesRes.Close()
	if modulesRes.Err() != nil {
		return nil, modulesRes.Err()
	}

	var modules []struct {
		Name string `json:"name"`
	}
	_ = modulesRes.ScanData("core.catalog.modules", &modules)

	// Build result.
	info := &schemaInfo{Module: module}

	for _, t := range types {
		fc := 0
		if t.Fields != nil {
			fc = t.Fields.Count
		}
		entry := typeEntry{Name: t.Name, FieldCount: fc, Catalog: t.Catalog}
		switch t.HugrType {
		case "table":
			info.Tables = append(info.Tables, entry)
		case "view":
			info.Views = append(info.Views, entry)
		}
	}

	funcSeen := map[string]bool{}
	for _, intro := range intros {
		if funcSeen[intro.FieldName] {
			continue
		}
		funcSeen[intro.FieldName] = true
		info.Functions = append(info.Functions, funcEntry{
			Name:    intro.FieldName,
			Catalog: intro.Catalog,
		})
	}

	// Collect direct child modules.
	prefix := ""
	if module != "" {
		prefix = module + "."
	}
	for _, m := range modules {
		if m.Name == module {
			continue
		}
		if prefix != "" && !strings.HasPrefix(m.Name, prefix) {
			continue
		}
		remainder := m.Name
		if prefix != "" {
			remainder = strings.TrimPrefix(m.Name, prefix)
		}
		if strings.Contains(remainder, ".") {
			continue // not a direct child
		}
		info.Submodules = append(info.Submodules, subModule{
			Name: m.Name,
		})
	}

	return info, nil
}

func printSchemaTable(info *schemaInfo, module string) {
	if module == "" {
		module = "(root)"
	}
	fmt.Printf("Module: %s\n", module)

	if len(info.Tables) > 0 {
		fmt.Println("  Tables:")
		for _, t := range info.Tables {
			fmt.Printf("    - %s (%d fields, catalog: %s)\n", t.Name, t.FieldCount, t.Catalog)
		}
	}
	if len(info.Views) > 0 {
		fmt.Println("  Views:")
		for _, v := range info.Views {
			fmt.Printf("    - %s (%d fields, catalog: %s)\n", v.Name, v.FieldCount, v.Catalog)
		}
	}
	if len(info.Functions) > 0 {
		fmt.Println("  Functions:")
		for _, f := range info.Functions {
			fmt.Printf("    - %s (catalog: %s)\n", f.Name, f.Catalog)
		}
	}
	if len(info.Submodules) > 0 {
		fmt.Println("  Sub-modules:")
		for _, sm := range info.Submodules {
			fmt.Printf("    - %s\n", sm.Name)
		}
	}
}
