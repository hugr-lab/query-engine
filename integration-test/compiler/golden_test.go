package compiler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
	"github.com/vektah/gqlparser/v2/parser"
)

// testConfig represents the config.json for a golden test case.
type testConfig struct {
	Catalogs      []catalogConfig `json:"catalogs"`
	ExpectedError string          `json:"expected_error"`
	SkipTypes     []string        `json:"skip_types"`
}

// catalogConfig represents one catalog entry in config.json.
type catalogConfig struct {
	File        string `json:"file"`
	Name        string `json:"name"`
	Engine      string `json:"engine"`
	AsModule    bool   `json:"as_module"`
	ReadOnly    bool   `json:"read_only"`
	IsExtension bool   `json:"is_extension"`
	Prefix      string `json:"prefix"`

	// Capabilities preset: "duckdb", "postgres", "duckdb_cross_catalog", or "" (no capabilities).
	Capabilities string `json:"capabilities"`
}

func TestGolden(t *testing.T) {
	testdataDir := filepath.Join("testdata")
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("read testdata dir: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		testDir := filepath.Join(testdataDir, dirName)
		// Must have config.json to be a test case
		if _, err := os.Stat(filepath.Join(testDir, "config.json")); err != nil {
			continue
		}

		t.Run(dirName, func(t *testing.T) {
			cfg := loadConfig(t, testDir)
			provider, compiler := setupMultiCatalogProvider(t)
			ctx := context.Background()

			for i, cat := range cfg.Catalogs {
				sdlPath := filepath.Join(testDir, "schemes", cat.File)
				sdlBytes, err := os.ReadFile(sdlPath)
				if err != nil {
					t.Fatalf("read %s: %v", sdlPath, err)
				}
				sdl := string(sdlBytes)

				sd, err := parser.ParseSchema(&ast.Source{Name: cat.File, Input: sdl})
				if err != nil {
					t.Fatalf("parse %s: %v", cat.File, err)
				}
				source := extractSourceDefs(sd)
				opts := buildOptions(cat)

				var p base.Provider
				if i > 0 {
					p = provider
				}

				compiled, err := compiler.Compile(ctx, p, source, opts)
				if cfg.ExpectedError != "" && err != nil {
					if !strings.Contains(err.Error(), cfg.ExpectedError) {
						t.Fatalf("expected error containing %q, got: %v", cfg.ExpectedError, err)
					}
					return // pass — error matched
				}
				if err != nil {
					t.Fatalf("compile %s: %v", cat.Name, err)
				}

				if err := provider.Update(ctx, compiled); err != nil {
					if cfg.ExpectedError != "" && strings.Contains(err.Error(), cfg.ExpectedError) {
						return // pass — error matched during update
					}
					t.Fatalf("update %s: %v", cat.Name, err)
				}
			}

			if cfg.ExpectedError != "" {
				t.Fatal("expected error but compilation succeeded")
			}

			// Format schema → SDL, filter out system types
			actual := formatSchemaSDL(provider.Schema(), cfg.SkipTypes)
			expectedPath := filepath.Join(testDir, "expected", "schema.graphql")

			if os.Getenv("UPDATE_GOLDEN") == "1" {
				if err := os.MkdirAll(filepath.Join(testDir, "expected"), 0o755); err != nil {
					t.Fatalf("create expected dir: %v", err)
				}
				if err := os.WriteFile(expectedPath, []byte(actual), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				t.Logf("updated golden file: %s", expectedPath)
				return
			}

			expectedBytes, err := os.ReadFile(expectedPath)
			if err != nil {
				t.Fatalf("read expected %s: %v (run with UPDATE_GOLDEN=1 to generate)", expectedPath, err)
			}
			expected := string(expectedBytes)

			if actual != expected {
				actualPath := filepath.Join(testDir, "actual.graphql")
				if writeErr := os.WriteFile(actualPath, []byte(actual), 0o644); writeErr != nil {
					t.Logf("warning: failed to write actual: %v", writeErr)
				}
				t.Fatalf("golden mismatch:\n  diff %s %s", expectedPath, actualPath)
			}
		})
	}
}

func loadConfig(t *testing.T, dir string) *testConfig {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("read config.json: %v", err)
	}
	var cfg testConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse config.json: %v", err)
	}
	return &cfg
}

func buildOptions(cat catalogConfig) base.Options {
	opts := base.Options{
		Name:        cat.Name,
		EngineType:  cat.Engine,
		AsModule:    cat.AsModule,
		ReadOnly:    cat.ReadOnly,
		IsExtension: cat.IsExtension,
		Prefix:      cat.Prefix,
	}
	if cap := capabilitiesPreset(cat.Capabilities); cap != nil {
		opts.Capabilities = cap
	}
	return opts
}

func capabilitiesPreset(name string) *base.EngineCapabilities {
	switch name {
	case "duckdb":
		return duckdbCapabilities()
	case "postgres":
		return postgresCapabilities()
	case "duckdb_cross_catalog":
		return &base.EngineCapabilities{
			General: base.EngineGeneralCapabilities{
				SupportDefaultSequences:       true,
				SupportCrossCatalogReferences: true,
				UnsupportedTypes:              []string{"IntRange", "BigIntRange", "TimestampRange"},
			},
			Insert: base.EngineInsertCapabilities{
				Insert:           true,
				Returning:        true,
				InsertReferences: true,
			},
			Update: base.EngineUpdateCapabilities{
				Update:           true,
				UpdatePKColumns:  true,
				UpdateWithoutPKs: true,
			},
			Delete: base.EngineDeleteCapabilities{
				Delete:           true,
				DeleteWithoutPKs: true,
			},
		}
	case "":
		return nil
	default:
		return nil
	}
}

// formatSchemaSDL formats the schema as SDL, including all types except
// GraphQL introspection builtins and per-test skip_types from config.json.
func formatSchemaSDL(schema *ast.Schema, extraSkipTypes []string) string {
	skipSet := make(map[string]bool)
	for _, name := range extraSkipTypes {
		skipSet[name] = true
	}
	// Only skip GraphQL built-in scalars and introspection types
	for _, name := range []string{
		"String", "Int", "Float", "Boolean", "ID",
		"__Schema", "__Type", "__Field", "__InputValue",
		"__EnumValue", "__Directive", "__DirectiveLocation",
		"__TypeKind",
	} {
		skipSet[name] = true
	}

	// Create a filtered copy of the schema
	filtered := &ast.Schema{
		Types:            make(map[string]*ast.Definition),
		Directives:       make(map[string]*ast.DirectiveDefinition),
		PossibleTypes:    schema.PossibleTypes,
		Implements:       schema.Implements,
		Query:            schema.Query,
		Mutation:         schema.Mutation,
		Subscription:     schema.Subscription,
		SchemaDirectives: schema.SchemaDirectives,
	}

	for name, def := range schema.Types {
		if skipSet[name] {
			continue
		}
		if def.BuiltIn {
			continue
		}
		// Sort fields for deterministic output
		sortedDef := *def
		sortedDef.Fields = make(ast.FieldList, len(def.Fields))
		copy(sortedDef.Fields, def.Fields)
		slices.SortFunc(sortedDef.Fields, func(a, b *ast.FieldDefinition) int {
			return strings.Compare(a.Name, b.Name)
		})
		// Sort enum values for deterministic output
		if len(sortedDef.EnumValues) > 0 {
			sortedDef.EnumValues = make(ast.EnumValueList, len(def.EnumValues))
			copy(sortedDef.EnumValues, def.EnumValues)
			slices.SortFunc(sortedDef.EnumValues, func(a, b *ast.EnumValueDefinition) int {
				return strings.Compare(a.Name, b.Name)
			})
		}
		filtered.Types[name] = &sortedDef
	}

	// Skip directive definitions — they are system-level
	// (we only care about type/field structure)

	var buf bytes.Buffer
	f := formatter.NewFormatter(&buf, formatter.WithIndent("  "))
	f.FormatSchema(filtered)
	return strings.TrimSpace(buf.String()) + "\n"
}
