package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/types"
	"github.com/schollz/progressbar/v3"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/anthropic"
	"github.com/tmc/langchaingo/llms/openai"
)

// hugr_type constants for types and fields (mirror base.HugrType* values).
const (
	hugrTypeTable    = "table"
	hugrTypeView     = "view"
	hugrTypeFunction = "function"

	hugrFieldSelect     = "select"
	hugrFieldFunction   = "function"
	hugrFieldExtraField = "extra_field"

	hugrFunctionTypes = "function,mutation_function,table_function,table_function_join"
)

//go:embed templates
var templatesFS embed.FS

func runSummarize(args []string) error {
	fs := flag.NewFlagSet("summarize", flag.ExitOnError)
	gf := addGlobalFlags(fs)

	catalog := fs.String("catalog", "", "Only summarize this catalog (default: all)")
	typeName := fs.String("type", "", "Summarize a single type (table/view) by name")
	funcName := fs.String("function", "", "Summarize a single function by type_name.field_name")
	moduleName := fs.String("module", "", "Summarize a single module by name")
	sourceName := fs.String("source", "", "Summarize a single data source by name")
	provider := fs.String("provider", envOrDefault("SUMMARIZE_PROVIDER", "openai"), "LLM provider: openai, anthropic, custom")
	model := fs.String("model", envOrDefault("SUMMARIZE_MODEL", "gpt-4o-mini"), "LLM model name")
	baseURL := fs.String("base-url", envOrDefault("SUMMARIZE_BASE_URL", ""), "Custom LLM API URL")
	apiKey := fs.String("api-key", envOrDefault("SUMMARIZE_API_KEY", ""), "LLM API key")
	maxConns := fs.Int("max-connections", 5, "Concurrent LLM requests")
	llmTimeout := fs.Duration("llm-timeout", 60*time.Second, "Per-request LLM timeout")
	retries := fs.Int("retries", 3, "Max attempts per LLM call when response parsing fails")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `AI-powered schema summarization using LLM.

Usage: hugr-tools summarize [flags]

Modes:
  (no flags)         Summarize all unsummarized entities (4-phase pipeline)
  --type <name>      Summarize a single type (table/view)
  --function <t.f>   Summarize a single function (type_name.field_name)
  --module <name>    Summarize a single module
  --source <name>    Summarize a single data source

Flags:`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *apiKey == "" {
		return fmt.Errorf("--api-key (or SUMMARIZE_API_KEY env) is required")
	}

	llm, err := createLLM(*provider, *model, *baseURL, *apiKey)
	if err != nil {
		return fmt.Errorf("create LLM: %w", err)
	}

	client := newClient(gf)
	ctx := context.Background()

	s := &summarizer{
		client:     client,
		llm:        llm,
		catalog:    *catalog,
		maxConns:   *maxConns,
		llmTimeout: *llmTimeout,
		retries:    *retries,
	}

	// Single-entity modes.
	switch {
	case *typeName != "":
		return s.summarizeSingleType(ctx, *typeName)
	case *funcName != "":
		parts := strings.SplitN(*funcName, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("--function requires type_name.field_name format")
		}
		return s.summarizeSingleFunction(ctx, parts[0], parts[1])
	case *moduleName != "":
		return s.summarizeSingleModule(ctx, *moduleName)
	case *sourceName != "":
		return s.summarizeSingleSource(ctx, *sourceName)
	}

	// Full pipeline mode.
	catLabel := *catalog
	if catLabel == "" {
		catLabel = "(all)"
	}
	fmt.Fprintf(os.Stderr, "Summarizing catalog: %s\n", catLabel)

	var total int

	n, err := s.summarizeDataObjects(ctx)
	if err != nil && !errors.Is(err, types.ErrNoData) {
		return fmt.Errorf("phase 1 (data objects): %w", err)
	}
	total += n

	n, err = s.summarizeFunctions(ctx)
	if err != nil && !errors.Is(err, types.ErrNoData) {
		return fmt.Errorf("phase 2 (functions): %w", err)
	}
	total += n

	n, err = s.summarizeDataSources(ctx)
	if err != nil && !errors.Is(err, types.ErrNoData) {
		return fmt.Errorf("phase 3 (data sources): %w", err)
	}
	total += n

	n, err = s.summarizeModules(ctx)
	if err != nil && !errors.Is(err, types.ErrNoData) {
		return fmt.Errorf("phase 4 (modules): %w", err)
	}
	total += n

	fmt.Fprintf(os.Stdout, "Done. %d entities summarized.\n", total)
	return nil
}

func createLLM(provider, model, baseURL, apiKey string) (llms.Model, error) {
	switch provider {
	case "openai":
		opts := []openai.Option{
			openai.WithToken(apiKey),
			openai.WithModel(model),
		}
		if baseURL != "" {
			opts = append(opts, openai.WithBaseURL(baseURL))
		}
		return openai.New(opts...)
	case "anthropic":
		opts := []anthropic.Option{
			anthropic.WithToken(apiKey),
			anthropic.WithModel(model),
		}
		if baseURL != "" {
			opts = append(opts, anthropic.WithBaseURL(baseURL))
		}
		return anthropic.New(opts...)
	case "custom":
		// Custom = OpenAI-compatible endpoint with custom base URL.
		if baseURL == "" {
			return nil, fmt.Errorf("custom provider requires --base-url")
		}
		return openai.New(
			openai.WithToken(apiKey),
			openai.WithModel(model),
			openai.WithBaseURL(baseURL),
		)
	default:
		return nil, fmt.Errorf("unknown provider: %s (supported: openai, anthropic, custom)", provider)
	}
}

type summarizer struct {
	client     *hugr.Client
	llm        llms.Model
	catalog    string
	maxConns   int
	llmTimeout time.Duration
	retries    int
}

// --- LLM Output Models ---

type dataObjectSummary struct {
	Short                      string                     `json:"short"`
	Long                       string                     `json:"long"`
	AggregationTypeShort       string                     `json:"aggregation_type_short,omitempty"`
	AggregationTypeLong        string                     `json:"aggregation_type_long,omitempty"`
	SubAggregationTypeShort    string                     `json:"sub_aggregation_type_short,omitempty"`
	SubAggregationTypeLong     string                     `json:"sub_aggregation_type_long,omitempty"`
	BucketAggregationTypeShort string                     `json:"bucket_aggregation_type_short,omitempty"`
	BucketAggregationTypeLong  string                     `json:"bucket_aggregation_type_long,omitempty"`
	Filter                     *filterSummary             `json:"filter,omitempty"`
	Fields                     map[string]string          `json:"fields,omitempty"`
	ExtraFields                map[string]string          `json:"extra_fields,omitempty"`
	Relations                  map[string]relationSummary `json:"relations,omitempty"`
	FunctionCalls              map[string]string          `json:"function_calls,omitempty"`
	Arguments                  *argumentsSummary          `json:"arguments,omitempty"`
	Queries                    map[string]string          `json:"queries,omitempty"`
	Mutations                  map[string]string          `json:"mutations,omitempty"`
}

type relationSummary struct {
	Short     string `json:"short"`
	Select    string `json:"select"`
	Agg       string `json:"select_agg,omitempty"`
	BucketAgg string `json:"select_bucket_agg,omitempty"`
}

type filterSummary struct {
	Row        string            `json:"row"`
	Fields     map[string]string `json:"fields"`
	References map[string]string `json:"references,omitempty"`
}

type argumentsSummary struct {
	Short  string            `json:"short"`
	Fields map[string]string `json:"fields"`
}

type functionSummary struct {
	Short      string                 `json:"short"`
	Long       string                 `json:"long,omitempty"`
	Parameters map[string]string      `json:"parameters,omitempty"`
	Returns    functionReturnsSummary `json:"returns,omitempty"`
}

type functionReturnsSummary struct {
	Short  string            `json:"short"`
	Fields map[string]string `json:"fields,omitempty"`
}

type dataSourceSummary struct {
	Short string `json:"short"`
	Long  string `json:"long"`
}

type moduleSummary struct {
	Short           string `json:"short"`
	Long            string `json:"long,omitempty"`
	QueryType       string `json:"query_type,omitempty"`
	MutationType    string `json:"mutation_type,omitempty"`
	FunctionType    string `json:"function_type,omitempty"`
	MutFunctionType string `json:"mutation_function_type,omitempty"`
}

// --- Template Data Structures ---

type dataObjectTemplateData struct {
	ObjectJSON            string
	ColumnsJSON           string
	RelationsJSON         string
	FunctionCallsJSON     string
	QueriesJSON           string
	MutationsJSON         string
	ArgumentsJSON         string
	DataSourceContextJSON string
	ModuleContextJSON     string
	RelatedGraphMaxDepth  int
	RelatedGraphNodesJSON string
	RelatedGraphEdgesJSON string
}

type functionTemplateData struct {
	Name                  string
	Description           string
	ParametersJSON        string
	ReturnType            string
	ReturnsArray          bool
	ReturnedFieldsJSON    string
	DataSourceContextJSON string
	ModuleContextJSON     string
}

type dataSourceTemplateData struct {
	Name           string
	Description    string
	ReadOnly       bool
	AsModule       bool
	TablesJSON     string
	ViewsJSON      string
	FunctionsJSON  string
	SubmodulesJSON string
}

type moduleTemplateData struct {
	Name                   string
	Description            string
	TablesJSON             string
	ViewsJSON              string
	FunctionsJSON          string
	MutationFunctionsJSON  string
	SubmodulesJSON         string
	DataSourceContextsJSON string
}

// --- Helper Types for Data Collection ---

type objectInfo struct {
	Name                     string `json:"name"`
	Type                     string `json:"type"`
	Description              string `json:"description,omitempty"`
	HasPrimaryKey            bool   `json:"has_primary_key"`
	HasGeometry              bool   `json:"has_geometry"`
	HasAggregationType       bool   `json:"has_aggregation_type,omitempty"`
	HasSubAggregationType    bool   `json:"has_sub_aggregation_type,omitempty"`
	HasBucketAggregationType bool   `json:"has_bucket_aggregation_type,omitempty"`
}

type columnInfo struct {
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	Type         string   `json:"type"`
	IsPrimaryKey bool     `json:"is_primary_key,omitempty"`
	IsArray      bool     `json:"is_array,omitempty"`
	ExtraFields  []string `json:"extra_fields,omitempty"`
}

type relationInfo struct {
	Name                string `json:"name"`
	Type                string `json:"type,omitempty"`
	DataObject          string `json:"data_object,omitempty"`
	Description         string `json:"description,omitempty"`
	FieldDataQuery      string `json:"field_data_query"`
	FieldAggQuery       string `json:"field_agg_query,omitempty"`
	FieldBucketAggQuery string `json:"field_bucket_agg_query,omitempty"`
}

type functionCallInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	FieldName   string `json:"field_name"`
}

type queryInfo struct {
	Name      string `json:"name"`
	QueryType string `json:"query_type"`
	QueryRoot string `json:"query_root,omitempty"`
}

type mutationFieldInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

type argumentInfo struct {
	TypeName string         `json:"type_name,omitempty"`
	Fields   []argumentItem `json:"fields,omitempty"`
}

type argumentItem struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

type dataSourceContext struct {
	Name        string `json:"name"`
	SummaryText string `json:"summary_text,omitempty"`
}

type moduleContext struct {
	Name     string `json:"name"`
	Overview string `json:"overview,omitempty"`
}

type relatedNode struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Module     string `json:"module,omitempty"`
	DataSource string `json:"data_source,omitempty"`
	Brief      string `json:"brief,omitempty"`
}

type relatedEdge struct {
	Name string `json:"name"`
	From string `json:"from"`
	To   string `json:"to"`
	Kind string `json:"kind"`
}

type returnedFieldInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type"`
	IsArray     bool   `json:"is_array,omitempty"`
}

type namedItem struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// --- callLLM ---

// callLLM renders a template with data and sends it to the LLM.
// Returns raw JSON content string for caller to unmarshal.
func (s *summarizer) callLLM(ctx context.Context, tmplName string, data any, maxTokens int) (string, error) {
	systemPrompt, err := templatesFS.ReadFile("templates/system.txt")
	if err != nil {
		return "", fmt.Errorf("read system prompt: %w", err)
	}

	tmplBytes, err := templatesFS.ReadFile("templates/" + tmplName)
	if err != nil {
		return "", fmt.Errorf("read template %s: %w", tmplName, err)
	}

	tmpl, err := template.New(tmplName).Parse(string(tmplBytes))
	if err != nil {
		return "", fmt.Errorf("parse template %s: %w", tmplName, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template %s: %w", tmplName, err)
	}

	ctx, cancel := context.WithTimeout(ctx, s.llmTimeout)
	defer cancel()

	msgs := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, string(systemPrompt)),
		llms.TextParts(llms.ChatMessageTypeHuman, buf.String()),
	}

	resp, err := s.llm.GenerateContent(ctx, msgs,
		llms.WithMaxTokens(maxTokens),
		llms.WithTemperature(0.3),
	)
	if err != nil {
		return "", fmt.Errorf("LLM call: %w", err)
	}
	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("LLM returned no choices")
	}
	content := resp.Choices[0].Content

	// Strip markdown code fence if present.
	content = strings.TrimSpace(content)
	content = strings.TrimPrefix(content, "```json")
	content = strings.TrimPrefix(content, "```")
	content = strings.TrimSuffix(content, "```")
	content = strings.TrimSpace(content)

	return content, nil
}

// callLLMAndParse calls callLLM and unmarshals the response into dest.
// On parse failure it retries up to s.retries attempts.
func (s *summarizer) callLLMAndParse(ctx context.Context, tmplName string, data any, maxTokens int, dest any) error {
	attempts := s.retries
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := range attempts {
		content, err := s.callLLM(ctx, tmplName, data, maxTokens)
		if err != nil {
			return err // LLM call error — not retryable
		}
		if err := json.Unmarshal([]byte(content), dest); err != nil {
			lastErr = fmt.Errorf("attempt %d/%d: parse error: %w", i+1, attempts, err)
			continue
		}
		return nil
	}
	return lastErr
}

// runParallel runs fn for each item with bounded concurrency.
func runParallel[T any](items []T, maxConns int, fn func(T) error) error {
	sem := make(chan struct{}, maxConns)
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for _, item := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func(it T) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(it); err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
		}(item)
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// newPhaseBar creates a styled progress bar for a summarization phase.
func newPhaseBar(total int, description string) *progressbar.ProgressBar {
	return progressbar.NewOptions(total,
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetWidth(25),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionSetElapsedTime(true),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "\u2588",
			SaucerPadding: "\u2591",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
}

// --- Write-back Helpers ---

func (s *summarizer) writeTypeDesc(ctx context.Context, name, short, long string) error {
	res, err := s.client.Query(ctx, `mutation($name: String!, $description: String!, $long_description: String!) {
		function {
			core {
				_schema_update_type_desc(name: $name, description: $description, long_description: $long_description) {
					success
				}
			}
		}
	}`, map[string]any{
		"name":             name,
		"description":      short,
		"long_description": long,
	})
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Err()
}

func (s *summarizer) writeFieldDesc(ctx context.Context, typeName, name, desc, longDesc string) error {
	res, err := s.client.Query(ctx, `mutation($type_name: String!, $name: String!, $description: String!, $long_description: String!) {
		function {
			core {
				_schema_update_field_desc(type_name: $type_name, name: $name, description: $description, long_description: $long_description) {
					success
				}
			}
		}
	}`, map[string]any{
		"type_name":        typeName,
		"name":             name,
		"description":      desc,
		"long_description": longDesc,
	})
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Err()
}

func (s *summarizer) writeCatalogDesc(ctx context.Context, name, short, long string) error {
	res, err := s.client.Query(ctx, `mutation($name: String!, $description: String!, $long_description: String!) {
		function {
			core {
				_schema_update_catalog_desc(name: $name, description: $description, long_description: $long_description) {
					success
				}
			}
		}
	}`, map[string]any{
		"name":             name,
		"description":      short,
		"long_description": long,
	})
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Err()
}

func (s *summarizer) writeModuleDesc(ctx context.Context, name, short, long string) error {
	res, err := s.client.Query(ctx, `mutation($name: String!, $description: String!, $long_description: String!) {
		function {
			core {
				_schema_update_module_desc(name: $name, description: $description, long_description: $long_description) {
					success
				}
			}
		}
	}`, map[string]any{
		"name":             name,
		"description":      short,
		"long_description": long,
	})
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Err()
}

// --- Phase 1: Data Objects ---

// prepareDataObjectContext queries all the context needed for a data object.
func (s *summarizer) prepareDataObjectContext(ctx context.Context, typeName string) (*dataObjectTemplateData, *dataObjectMeta, error) {
	// 1. Fetch type with fields, data_object info, module, and catalog.
	res, err := s.client.Query(ctx, `query($name: String!) {
		core {
			catalog {
				types_by_pk(name: $name) {
					name
					hugr_type
					description
					catalog
					module
					fields {
						name
						description
						field_type
						field_type_name
						hugr_type
						is_pk
					}
					data_object {
						filter_type_name
						args_type_name
						queries {
							name
							query_root
							query_type
						}
					}
					module_info {
						name
						description
						query_root
						mutation_root
						function_root
						mut_function_root
					}
					catalog_info {
						name
						description
					}
				}
			}
		}
	}`, map[string]any{"name": typeName})
	if err != nil {
		return nil, nil, fmt.Errorf("query type %s: %w", typeName, err)
	}
	defer res.Close()
	if res.Err() != nil {
		return nil, nil, res.Err()
	}

	var typeInfo struct {
		Name        string `json:"name"`
		HugrType    string `json:"hugr_type"`
		Description string `json:"description"`
		Catalog     string `json:"catalog"`
		Module      string `json:"module"`
		Fields      []struct {
			Name          string `json:"name"`
			Description   string `json:"description"`
			FieldType     string `json:"field_type"`
			FieldTypeName string `json:"field_type_name"`
			HugrType      string `json:"hugr_type"`
			IsPK          bool   `json:"is_pk"`
		} `json:"fields"`
		DataObject []struct {
			FilterTypeName string `json:"filter_type_name"`
			ArgsTypeName   string `json:"args_type_name"`
			Queries        []struct {
				Name      string `json:"name"`
				QueryRoot string `json:"query_root"`
				QueryType string `json:"query_type"`
			} `json:"queries"`
		} `json:"data_object"`
		ModuleInfo *struct {
			Name            string `json:"name"`
			Description     string `json:"description"`
			QueryRoot       string `json:"query_root"`
			MutationRoot    string `json:"mutation_root"`
			FunctionRoot    string `json:"function_root"`
			MutFunctionRoot string `json:"mut_function_root"`
		} `json:"module_info"`
		CatalogInfo *struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"catalog_info"`
	}
	if err := res.ScanData("core.catalog.types_by_pk", &typeInfo); err != nil {
		return nil, nil, fmt.Errorf("type %s not found: %w", typeName, err)
	}

	// Categorize fields.
	obj := objectInfo{
		Name:        typeInfo.Name,
		Type:        typeInfo.HugrType,
		Description: typeInfo.Description,
	}
	var columns []columnInfo
	var relations []relationInfo
	var functionCalls []functionCallInfo
	for _, f := range typeInfo.Fields {
		switch f.HugrType {
		case "": // regular column
			col := columnInfo{
				Name:        f.Name,
				Description: f.Description,
				Type:        f.FieldType,
				IsArray:     strings.HasPrefix(f.FieldType, "["),
			}
			// Check for PK and geometry.
			if f.IsPK {
				col.IsPrimaryKey = true
				obj.HasPrimaryKey = true
			}
			if strings.Contains(f.FieldType, "Geometry") {
				obj.HasGeometry = true
			}
			// Extra fields are collected separately via hugr_type.
			columns = append(columns, col)

		case hugrFieldSelect: // relation (reference or join)
			rel := relationInfo{
				Name:           f.Name,
				DataObject:     f.FieldTypeName,
				Description:    f.Description,
				FieldDataQuery: f.Name,
			}
			// Check for corresponding aggregation and bucket aggregation fields.
			aggName := f.Name + "_aggregation"
			bucketAggName := f.Name + "_bucket_aggregation"
			for _, af := range typeInfo.Fields {
				if af.Name == aggName {
					rel.FieldAggQuery = aggName
				}
				if af.Name == bucketAggName {
					rel.FieldBucketAggQuery = bucketAggName
				}
			}
			relations = append(relations, rel)

		case hugrFieldFunction: // function call
			functionCalls = append(functionCalls, functionCallInfo{
				Name:        f.FieldTypeName,
				Description: f.Description,
				FieldName:   f.Name,
			})
		}
	}

	// Build extra fields list for template.
	var extraColumns []columnInfo
	for _, f := range typeInfo.Fields {
		if f.HugrType == hugrFieldExtraField {
			extraColumns = append(extraColumns, columnInfo{
				Name:        f.Name,
				Description: f.Description,
				Type:        f.FieldType,
			})
		}
	}

	// 2. Check aggregation types.
	aggTypeName := typeName + "_aggregation"
	subAggTypeName := typeName + "_sub_aggregation"
	bucketAggTypeName := typeName + "_bucket_aggregation"

	aggRes, err := s.client.Query(ctx, `query($names: [String!]) {
		core { catalog { types(filter: {name: {in: $names}}) { name } } }
	}`, map[string]any{"names": []string{aggTypeName, subAggTypeName, bucketAggTypeName}})
	if err == nil {
		var aggTypes []struct {
			Name string `json:"name"`
		}
		_ = aggRes.ScanData("core.catalog.types", &aggTypes)
		aggRes.Close()
		for _, t := range aggTypes {
			switch t.Name {
			case aggTypeName:
				obj.HasAggregationType = true
			case subAggTypeName:
				obj.HasSubAggregationType = true
			case bucketAggTypeName:
				obj.HasBucketAggregationType = true
			}
		}
	}

	// 3. Collect queries.
	var queries []queryInfo
	if len(typeInfo.DataObject) > 0 {
		for _, q := range typeInfo.DataObject[0].Queries {
			queries = append(queries, queryInfo{
				Name:      q.Name,
				QueryType: q.QueryType,
				QueryRoot: q.QueryRoot,
			})
		}
	}

	// 4. Collect mutations from module_intro.
	var mutations []mutationFieldInfo
	if typeInfo.ModuleInfo != nil && typeInfo.ModuleInfo.MutationRoot != "" {
		mutRes, err := s.client.Query(ctx, `query($module: String!) {
			core {
				catalog {
					module_intro(filter: {module: {eq: $module}, type_type: {eq: "mutation"}}) {
						field_name
						field_description
					}
				}
			}
		}`, map[string]any{"module": typeInfo.Module})
		if err == nil {
			var muts []struct {
				FieldName   string `json:"field_name"`
				Description string `json:"field_description"`
			}
			_ = mutRes.ScanData("core.catalog.module_intro", &muts)
			mutRes.Close()
			// Filter mutations related to this data object.
			for _, m := range muts {
				if strings.HasSuffix(m.FieldName, "_"+typeName) ||
					strings.HasSuffix(m.FieldName, typeName) {
					mutations = append(mutations, mutationFieldInfo{
						Name:        m.FieldName,
						Description: m.Description,
					})
				}
			}
		}
	}

	// 5. Collect arguments.
	var arguments *argumentInfo
	if len(typeInfo.DataObject) > 0 && typeInfo.DataObject[0].ArgsTypeName != "" {
		argsRes, err := s.client.Query(ctx, `query($typeName: String!) {
			core { catalog { fields(filter: {type_name: {eq: $typeName}}) { name field_type description } } }
		}`, map[string]any{"typeName": typeInfo.DataObject[0].ArgsTypeName})
		if err == nil {
			var argFields []struct {
				Name        string `json:"name"`
				Type        string `json:"field_type"`
				Description string `json:"description"`
			}
			_ = argsRes.ScanData("core.catalog.fields", &argFields)
			argsRes.Close()
			if len(argFields) > 0 {
				arguments = &argumentInfo{TypeName: typeInfo.DataObject[0].ArgsTypeName}
				for _, af := range argFields {
					arguments.Fields = append(arguments.Fields, argumentItem{
						Name:        af.Name,
						Type:        af.Type,
						Description: af.Description,
					})
				}
			}
		}
	}

	// 6. Build related graph (depth 2).
	nodes := []relatedNode{{
		Type:       typeInfo.HugrType,
		Name:       typeInfo.Name,
		Module:     typeInfo.Module,
		DataSource: typeInfo.Catalog,
		Brief:      buildBrief(typeInfo.Description, columns),
	}}
	var edges []relatedEdge

	for _, rel := range relations {
		if rel.DataObject == "" || rel.DataObject == typeName {
			continue
		}
		// Fetch related type's fields.
		relRes, err := s.client.Query(ctx, `query($name: String!) {
			core { catalog { types_by_pk(name: $name) {
				name hugr_type module catalog description
				fields { name description field_type hugr_type }
			}}}
		}`, map[string]any{"name": rel.DataObject})
		if err != nil {
			continue
		}
		var relType struct {
			Name        string `json:"name"`
			HugrType    string `json:"hugr_type"`
			Module      string `json:"module"`
			Catalog     string `json:"catalog"`
			Description string `json:"description"`
			Fields      []struct {
				Name        string `json:"name"`
				Description string `json:"description"`
				FieldType   string `json:"field_type"`
				HugrType    string `json:"hugr_type"`
			} `json:"fields"`
		}
		if err := relRes.ScanData("core.catalog.types_by_pk", &relType); err != nil {
			relRes.Close()
			continue
		}
		relRes.Close()

		var relCols []columnInfo
		for _, f := range relType.Fields {
			if f.HugrType == "" {
				relCols = append(relCols, columnInfo{Name: f.Name, Description: f.Description, Type: f.FieldType})
			}
		}

		nodeExists := false
		for _, n := range nodes {
			if n.Name == relType.Name {
				nodeExists = true
				break
			}
		}
		if !nodeExists {
			nodes = append(nodes, relatedNode{
				Type:       relType.HugrType,
				Name:       relType.Name,
				Module:     relType.Module,
				DataSource: relType.Catalog,
				Brief:      buildBrief(relType.Description, relCols),
			})
		}
		edges = append(edges, relatedEdge{
			Name: typeName + ":" + rel.FieldDataQuery,
			From: typeName,
			To:   rel.DataObject,
			Kind: "relation",
		})
	}

	// 7. Build data source and module context.
	// Fetch full catalog info separately (type/prefix/as_module not available through reference subquery).
	dsCtx := dataSourceContext{}
	if typeInfo.Catalog != "" {
		catRes, err := s.client.Query(ctx, `query($name: String!) {
			core { catalog { schema_catalogs_by_pk(name: $name) { name type description } } }
		}`, map[string]any{"name": typeInfo.Catalog})
		if err == nil {
			var cat struct {
				Name        string `json:"name"`
				Type        string `json:"type"`
				Description string `json:"description"`
			}
			if err := catRes.ScanData("core.catalog.schema_catalogs_by_pk", &cat); err == nil {
				dsCtx.Name = cat.Name
				dsCtx.SummaryText = cat.Description
				if dsCtx.SummaryText != "" {
					dsCtx.SummaryText += " (" + cat.Type + ")"
				}
			}
			catRes.Close()
		}
	} else if typeInfo.CatalogInfo != nil {
		dsCtx.Name = typeInfo.CatalogInfo.Name
		dsCtx.SummaryText = typeInfo.CatalogInfo.Description
	}
	modCtx := moduleContext{}
	if typeInfo.ModuleInfo != nil {
		modCtx.Name = typeInfo.ModuleInfo.Name
		modCtx.Overview = typeInfo.ModuleInfo.Description
	}

	// Marshal template data.
	objectJSON, _ := json.Marshal(obj)
	columnsJSON, _ := json.Marshal(columns)
	relationsJSON, _ := json.Marshal(relations)
	fcJSON, _ := json.Marshal(functionCalls)
	queriesJSON, _ := json.Marshal(queries)
	mutationsJSON, _ := json.Marshal(mutations)
	argsJSON, _ := json.Marshal(arguments)
	dsCtxJSON, _ := json.Marshal(dsCtx)
	modCtxJSON, _ := json.Marshal(modCtx)
	nodesJSON, _ := json.Marshal(nodes)
	edgesJSON, _ := json.Marshal(edges)

	td := &dataObjectTemplateData{
		ObjectJSON:            string(objectJSON),
		ColumnsJSON:           string(columnsJSON),
		RelationsJSON:         string(relationsJSON),
		FunctionCallsJSON:     string(fcJSON),
		QueriesJSON:           string(queriesJSON),
		MutationsJSON:         string(mutationsJSON),
		ArgumentsJSON:         string(argsJSON),
		DataSourceContextJSON: string(dsCtxJSON),
		ModuleContextJSON:     string(modCtxJSON),
		RelatedGraphMaxDepth:  2,
		RelatedGraphNodesJSON: string(nodesJSON),
		RelatedGraphEdgesJSON: string(edgesJSON),
	}

	// Build meta for write-back.
	meta := &dataObjectMeta{
		TypeName:          typeName,
		Module:            typeInfo.Module,
		AggTypeName:       aggTypeName,
		SubAggTypeName:    subAggTypeName,
		BucketAggTypeName: bucketAggTypeName,
		HasAgg:            obj.HasAggregationType,
		HasSubAgg:         obj.HasSubAggregationType,
		HasBucketAgg:      obj.HasBucketAggregationType,
		Relations:         relations,
		FunctionCalls:     functionCalls,
		Queries:           queries,
		Mutations:         mutations,
		ExtraFields:       extraColumns,
	}
	if len(typeInfo.DataObject) > 0 {
		meta.FilterTypeName = typeInfo.DataObject[0].FilterTypeName
		meta.ArgsTypeName = typeInfo.DataObject[0].ArgsTypeName
	}
	if typeInfo.ModuleInfo != nil {
		meta.QueryRoot = typeInfo.ModuleInfo.QueryRoot
		meta.MutationRoot = typeInfo.ModuleInfo.MutationRoot
	}
	return td, meta, nil
}

// dataObjectMeta holds metadata needed for write-back after LLM summarization.
type dataObjectMeta struct {
	TypeName          string
	Module            string
	FilterTypeName    string
	ArgsTypeName      string
	AggTypeName       string
	SubAggTypeName    string
	BucketAggTypeName string
	HasAgg            bool
	HasSubAgg         bool
	HasBucketAgg      bool
	QueryRoot         string
	MutationRoot      string
	Relations         []relationInfo
	FunctionCalls     []functionCallInfo
	Queries           []queryInfo
	Mutations         []mutationFieldInfo
	ExtraFields       []columnInfo
}

func buildBrief(desc string, columns []columnInfo) string {
	brief := desc
	if brief != "" {
		brief += " | "
	}
	brief += "Fields: "
	for i, c := range columns {
		if i > 0 {
			brief += ", "
		}
		brief += c.Name
		if c.Description != "" {
			brief += " (" + c.Description + ")"
		}
		if i >= 10 {
			brief += ", ..."
			break
		}
	}
	return brief
}

// writeBackDataObject writes all descriptions from dataObjectSummary to the schema.
func (s *summarizer) writeBackDataObject(ctx context.Context, ds *dataObjectSummary, meta *dataObjectMeta) error {
	// 1. Main type description.
	if err := s.writeTypeDesc(ctx, meta.TypeName, ds.Short, ds.Long); err != nil {
		return fmt.Errorf("write type desc: %w", err)
	}

	// 2. Field descriptions.
	for name, desc := range ds.Fields {
		_ = s.writeFieldDesc(ctx, meta.TypeName, name, desc, "")
		if meta.HasAgg {
			_ = s.writeFieldDesc(ctx, meta.AggTypeName, name, desc, "")
		}
		if meta.HasSubAgg {
			_ = s.writeFieldDesc(ctx, meta.SubAggTypeName, name, desc, "")
		}
	}

	// 3. Extra field descriptions.
	for name, desc := range ds.ExtraFields {
		_ = s.writeFieldDesc(ctx, meta.TypeName, name, desc, "")
		if meta.HasAgg {
			_ = s.writeFieldDesc(ctx, meta.AggTypeName, name, desc, "")
		}
		if meta.HasSubAgg {
			_ = s.writeFieldDesc(ctx, meta.SubAggTypeName, name, desc, "")
		}
	}

	// 4. Filter type description.
	if meta.FilterTypeName != "" && ds.Filter != nil {
		_ = s.writeTypeDesc(ctx, meta.FilterTypeName, ds.Filter.Row, "")
		for name, desc := range ds.Filter.Fields {
			_ = s.writeFieldDesc(ctx, meta.FilterTypeName, name, desc, "")
		}
		for name, desc := range ds.Filter.References {
			_ = s.writeFieldDesc(ctx, meta.FilterTypeName, name, desc, "")
		}
	}

	// 5. Relation descriptions.
	for _, rel := range meta.Relations {
		rs, ok := ds.Relations[rel.Name]
		if !ok {
			continue
		}
		if rs.Select != "" {
			_ = s.writeFieldDesc(ctx, meta.TypeName, rel.FieldDataQuery, rs.Select, "")
		}
		if rs.Agg != "" && rel.FieldAggQuery != "" {
			_ = s.writeFieldDesc(ctx, meta.TypeName, rel.FieldAggQuery, rs.Agg, "")
		}
		if rs.BucketAgg != "" && rel.FieldBucketAggQuery != "" {
			_ = s.writeFieldDesc(ctx, meta.TypeName, rel.FieldBucketAggQuery, rs.BucketAgg, "")
		}
		// Propagate to aggregation type.
		if meta.HasAgg && rs.Select != "" {
			_ = s.writeFieldDesc(ctx, meta.AggTypeName, rel.FieldDataQuery, rs.Select, "")
			if rel.FieldAggQuery != "" && rs.Agg != "" {
				_ = s.writeFieldDesc(ctx, meta.AggTypeName, rel.FieldAggQuery, rs.Agg, "")
			}
		}
		if meta.HasSubAgg && rel.FieldAggQuery != "" && rs.Agg != "" {
			_ = s.writeFieldDesc(ctx, meta.SubAggTypeName, rel.FieldDataQuery, rs.Agg, "")
		}
	}

	// 6. Function call descriptions.
	for _, fc := range meta.FunctionCalls {
		desc, ok := ds.FunctionCalls[fc.Name]
		if !ok {
			continue
		}
		_ = s.writeFieldDesc(ctx, meta.TypeName, fc.FieldName, desc, "")
		if meta.HasAgg {
			_ = s.writeFieldDesc(ctx, meta.AggTypeName, fc.FieldName, desc, "")
		}
	}

	// 7. Query descriptions on module query root + shared types.
	if meta.QueryRoot != "" {
		for _, q := range meta.Queries {
			desc, ok := ds.Queries[q.Name]
			if !ok {
				continue
			}
			_ = s.writeFieldDesc(ctx, meta.QueryRoot, q.Name, desc, "")
		}

		// Write to shared _join, _spatial, _h3_data_query types using TypeName.
		// The compiled type name is the field name in shared types (already prefixed).
		sharedTypes := []string{"_join", "_spatial", "_h3_data_query"}
		for _, st := range sharedTypes {
			_ = s.writeFieldDesc(ctx, st, meta.TypeName, ds.Short, "")
			if meta.HasAgg {
				_ = s.writeFieldDesc(ctx, st, meta.TypeName+"_aggregation", ds.AggregationTypeShort, "")
				_ = s.writeFieldDesc(ctx, st, meta.TypeName+"_bucket_aggregation", ds.BucketAggregationTypeShort, "")
			}
		}
	}

	// 8. Mutation descriptions on module mutation root.
	if meta.MutationRoot != "" {
		for _, m := range meta.Mutations {
			desc, ok := ds.Mutations[m.Name]
			if !ok {
				continue
			}
			_ = s.writeFieldDesc(ctx, meta.MutationRoot, m.Name, desc, "")
		}
	}

	// 9. Arguments type description.
	if meta.ArgsTypeName != "" && ds.Arguments != nil && ds.Arguments.Short != "" {
		_ = s.writeTypeDesc(ctx, meta.ArgsTypeName, ds.Arguments.Short, "")
		for name, desc := range ds.Arguments.Fields {
			_ = s.writeFieldDesc(ctx, meta.ArgsTypeName, name, desc, "")
		}
	}

	// 10. Aggregation type descriptions.
	if meta.HasAgg && ds.AggregationTypeShort != "" {
		_ = s.writeTypeDesc(ctx, meta.AggTypeName, ds.AggregationTypeShort, ds.AggregationTypeLong)
	}
	if meta.HasSubAgg && ds.SubAggregationTypeShort != "" {
		_ = s.writeTypeDesc(ctx, meta.SubAggTypeName, ds.SubAggregationTypeShort, ds.SubAggregationTypeLong)
	}
	if meta.HasBucketAgg && ds.BucketAggregationTypeShort != "" {
		_ = s.writeTypeDesc(ctx, meta.BucketAggTypeName, ds.BucketAggregationTypeShort, ds.BucketAggregationTypeLong)
	}

	// 11. Derived input type descriptions (copy from parent fields, no LLM needed).
	s.writeBackDerivedInputTypes(ctx, ds, meta)

	return nil
}

// writeBackDerivedInputTypes copies field descriptions from the parent data object
// to its derived input types (_filter, _mut_input_data, _mut_data, _list_filter).
// These types mirror the parent's fields but are generated without descriptions.
func (s *summarizer) writeBackDerivedInputTypes(ctx context.Context, ds *dataObjectSummary, meta *dataObjectMeta) {
	type derivedType struct {
		suffix   string
		typeDesc string
	}
	derived := []derivedType{
		{"_filter", "Filter input for " + meta.TypeName},
		{"_list_filter", "List filter input for " + meta.TypeName},
		{"_mut_input_data", "Insert input for " + meta.TypeName},
		{"_mut_data", "Update input for " + meta.TypeName},
	}

	for _, dt := range derived {
		// Skip _filter if already handled by LLM (step 4).
		if dt.suffix == "_filter" && ds.Filter != nil {
			continue
		}

		typeName := meta.TypeName + dt.suffix
		fields, err := s.getTypeFields(ctx, typeName)
		if err != nil || len(fields) == 0 {
			continue // type doesn't exist or has no fields
		}

		_ = s.writeTypeDesc(ctx, typeName, dt.typeDesc, "")

		for _, fieldName := range fields {
			if desc, ok := ds.Fields[fieldName]; ok && desc != "" {
				_ = s.writeFieldDesc(ctx, typeName, fieldName, desc, "")
			} else if desc, ok := ds.ExtraFields[fieldName]; ok && desc != "" {
				_ = s.writeFieldDesc(ctx, typeName, fieldName, desc, "")
			}
		}
	}
}

// getTypeFields queries the catalog for all field names of a given type.
func (s *summarizer) getTypeFields(ctx context.Context, typeName string) ([]string, error) {
	res, err := s.client.Query(ctx, `query($typeName: String!) {
		core { catalog { fields(filter: {type_name: {eq: $typeName}}) { name } } }
	}`, map[string]any{"typeName": typeName})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Err() != nil {
		return nil, res.Err()
	}

	var fields []struct {
		Name string `json:"name"`
	}
	if err := res.ScanData("core.catalog.fields", &fields); err != nil {
		return nil, err
	}

	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names, nil
}

func (s *summarizer) summarizeDataObjects(ctx context.Context) (int, error) {
	filter := map[string]any{
		"is_summarized": map[string]any{"eq": false},
		"hugr_type":     map[string]any{"in": []string{hugrTypeTable, hugrTypeView}},
	}
	if s.catalog != "" {
		filter["catalog"] = map[string]any{"eq": s.catalog}
	}

	res, err := s.client.Query(ctx, `query($filter: core_types_filter) {
		core {
			catalog {
				types(filter: $filter) {
					name
				}
			}
		}
	}`, map[string]any{"filter": filter})
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if res.Err() != nil {
		return 0, res.Err()
	}

	var objects []struct {
		Name string `json:"name"`
	}
	if err := res.ScanData("core.catalog.types", &objects); err != nil {
		return 0, err
	}

	bar := newPhaseBar(len(objects), "  [1/4] Data objects")
	var processed atomic.Int32
	err = runParallel(objects, s.maxConns, func(obj struct {
		Name string `json:"name"`
	}) error {
		defer bar.Add(1)

		td, meta, err := s.prepareDataObjectContext(ctx, obj.Name)
		if err != nil {
			bar.Describe(fmt.Sprintf("  [1/4] WARN: %s: %v", obj.Name, err))
			return nil
		}

		var ds dataObjectSummary
		if err := s.callLLMAndParse(ctx, "data_object.tmpl", td, 16384, &ds); err != nil {
			bar.Describe(fmt.Sprintf("  [1/4] WARN: %s: %v", obj.Name, err))
			return nil
		}

		if err := s.writeBackDataObject(ctx, &ds, meta); err != nil {
			bar.Describe(fmt.Sprintf("  [1/4] WARN: %s: write error", obj.Name))
			return nil
		}
		processed.Add(1)
		bar.Describe("  [1/4] Data objects")
		return nil
	})
	if err != nil {
		return int(processed.Load()), err
	}

	n := int(processed.Load())
	bar.Finish()
	fmt.Fprintf(os.Stderr, "  [1/4] Data objects: %d/%d done\n", n, len(objects))
	return n, nil
}

// --- Phase 2: Functions ---

// prepareFunctionContext queries all context needed for a function.
func (s *summarizer) prepareFunctionContext(ctx context.Context, typeName, fieldName string) (*functionTemplateData, *functionMeta, error) {
	// Fetch function field info.
	res, err := s.client.Query(ctx, `query($filter: core_fields_filter) {
		core {
			catalog {
				fields(filter: $filter, limit: 1) {
					type_name
					name
					description
					field_type
					field_type_name
					hugr_type
					catalog
					arguments {
						name
						arg_type
						description
					}
				}
			}
		}
	}`, map[string]any{
		"filter": map[string]any{
			"type_name": map[string]any{"eq": typeName},
			"name":      map[string]any{"eq": fieldName},
		},
	})
	if err != nil {
		return nil, nil, err
	}
	defer res.Close()
	if res.Err() != nil {
		return nil, nil, res.Err()
	}

	var functions []struct {
		TypeName      string `json:"type_name"`
		Name          string `json:"name"`
		Description   string `json:"description"`
		FieldType     string `json:"field_type"`
		FieldTypeName string `json:"field_type_name"`
		HugrType      string `json:"hugr_type"`
		Catalog       string `json:"catalog"`
		Args          []struct {
			Name        string `json:"name"`
			Type        string `json:"arg_type"`
			Description string `json:"description"`
		} `json:"arguments"`
	}
	if err := res.ScanData("core.catalog.fields", &functions); err != nil || len(functions) == 0 {
		return nil, nil, fmt.Errorf("function %s.%s not found", typeName, fieldName)
	}
	fn := functions[0]

	// Determine module from type_name.
	module := ""
	if idx := strings.LastIndex(fn.TypeName, "_"); idx > 0 {
		module = fn.TypeName[:idx]
	}

	// Fetch return type fields.
	var returnedFields []returnedFieldInfo
	returnsArray := strings.HasPrefix(fn.FieldType, "[")
	returnTypeName := fn.FieldTypeName

	if returnTypeName != "" {
		rtRes, err := s.client.Query(ctx, `query($typeName: String!) {
			core { catalog { fields(filter: {type_name: {eq: $typeName}}) {
				name description field_type hugr_type
			}}}
		}`, map[string]any{"typeName": returnTypeName})
		if err == nil {
			var rtFields []struct {
				Name        string `json:"name"`
				Description string `json:"description"`
				FieldType   string `json:"field_type"`
				HugrType    string `json:"hugr_type"`
			}
			_ = rtRes.ScanData("core.catalog.fields", &rtFields)
			rtRes.Close()
			for _, f := range rtFields {
				if f.HugrType == "" {
					returnedFields = append(returnedFields, returnedFieldInfo{
						Name:        f.Name,
						Description: f.Description,
						Type:        f.FieldType,
						IsArray:     strings.HasPrefix(f.FieldType, "["),
					})
				}
			}
		}
	}

	// Fetch module context.
	modCtx := moduleContext{Name: module}
	if module != "" {
		modRes, err := s.client.Query(ctx, `query($name: String!) {
			core { catalog { modules_by_pk(name: $name) { name description } } }
		}`, map[string]any{"name": module})
		if err == nil {
			var m struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			}
			if err := modRes.ScanData("core.catalog.modules_by_pk", &m); err == nil {
				modCtx.Overview = m.Description
			}
			modRes.Close()
		}
	}

	// Fetch catalog context.
	dsCtx := dataSourceContext{Name: fn.Catalog}
	if fn.Catalog != "" {
		catRes, err := s.client.Query(ctx, `query($name: String!) {
			core { catalog { schema_catalogs_by_pk(name: $name) { name type description } } }
		}`, map[string]any{"name": fn.Catalog})
		if err == nil {
			var c struct {
				Name        string `json:"name"`
				Type        string `json:"type"`
				Description string `json:"description"`
			}
			if err := catRes.ScanData("core.catalog.schema_catalogs_by_pk", &c); err == nil {
				dsCtx.SummaryText = c.Description
				if dsCtx.SummaryText != "" {
					dsCtx.SummaryText += " (" + c.Type + ")"
				}
			}
			catRes.Close()
		}
	}

	// Marshal parameters.
	type paramInfo struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		Description string `json:"description,omitempty"`
	}
	var params []paramInfo
	for _, a := range fn.Args {
		params = append(params, paramInfo{Name: a.Name, Type: a.Type, Description: a.Description})
	}

	paramsJSON, _ := json.Marshal(params)
	returnedFieldsJSON, _ := json.Marshal(returnedFields)
	dsCtxJSON, _ := json.Marshal(dsCtx)
	modCtxJSON, _ := json.Marshal(modCtx)

	td := &functionTemplateData{
		Name:                  fmt.Sprintf("%q", fn.Name),
		Description:           fmt.Sprintf("%q", fn.Description),
		ParametersJSON:        string(paramsJSON),
		ReturnType:            fmt.Sprintf("%q", fn.FieldType),
		ReturnsArray:          returnsArray,
		ReturnedFieldsJSON:    string(returnedFieldsJSON),
		DataSourceContextJSON: string(dsCtxJSON),
		ModuleContextJSON:     string(modCtxJSON),
	}

	meta := &functionMeta{
		TypeName:       fn.TypeName,
		FieldName:      fn.Name,
		ReturnTypeName: returnTypeName,
	}

	return td, meta, nil
}

type functionMeta struct {
	TypeName       string
	FieldName      string
	ReturnTypeName string
}

func (s *summarizer) writeBackFunction(ctx context.Context, fs *functionSummary, meta *functionMeta) error {
	// 1. Function field description.
	if fs.Long != "" {
		_ = s.writeFieldDesc(ctx, meta.TypeName, meta.FieldName, fs.Long, "")
	}

	// 2. Return type description.
	if meta.ReturnTypeName != "" && fs.Returns.Short != "" {
		_ = s.writeTypeDesc(ctx, meta.ReturnTypeName, fs.Returns.Short, fs.Long)
		for name, desc := range fs.Returns.Fields {
			_ = s.writeFieldDesc(ctx, meta.ReturnTypeName, name, desc, "")
		}
	}

	return nil
}

func (s *summarizer) summarizeFunctions(ctx context.Context) (int, error) {
	filter := map[string]any{
		"is_summarized": map[string]any{"eq": false},
		"hugr_type":     map[string]any{"in": strings.Split(hugrFunctionTypes, ",")},
	}
	if s.catalog != "" {
		filter["catalog"] = map[string]any{"eq": s.catalog}
	}

	res, err := s.client.Query(ctx, `query($filter: core_fields_filter) {
		core {
			catalog {
				fields(filter: $filter) {
					type_name
					name
				}
			}
		}
	}`, map[string]any{"filter": filter})
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if res.Err() != nil {
		return 0, res.Err()
	}

	var functions []struct {
		TypeName string `json:"type_name"`
		Name     string `json:"name"`
	}
	if err := res.ScanData("core.catalog.fields", &functions); err != nil {
		return 0, err
	}

	bar := newPhaseBar(len(functions), "  [2/4] Functions")
	var processed atomic.Int32
	err = runParallel(functions, s.maxConns, func(fn struct {
		TypeName string `json:"type_name"`
		Name     string `json:"name"`
	}) error {
		defer bar.Add(1)

		td, meta, err := s.prepareFunctionContext(ctx, fn.TypeName, fn.Name)
		if err != nil {
			bar.Describe(fmt.Sprintf("  [2/4] WARN: %s.%s: %v", fn.TypeName, fn.Name, err))
			return nil
		}

		var fs functionSummary
		if err := s.callLLMAndParse(ctx, "function.tmpl", td, 4096, &fs); err != nil {
			bar.Describe(fmt.Sprintf("  [2/4] WARN: %s: %v", fn.Name, err))
			return nil
		}

		if err := s.writeBackFunction(ctx, &fs, meta); err != nil {
			bar.Describe(fmt.Sprintf("  [2/4] WARN: %s: write error", fn.Name))
			return nil
		}
		processed.Add(1)
		bar.Describe("  [2/4] Functions")
		return nil
	})
	if err != nil {
		return int(processed.Load()), err
	}

	n := int(processed.Load())
	bar.Finish()
	fmt.Fprintf(os.Stderr, "  [2/4] Functions: %d/%d done\n", n, len(functions))
	return n, nil
}

// --- Phase 3: Data Sources ---

func (s *summarizer) prepareDataSourceContext(ctx context.Context, name string) (*dataSourceTemplateData, error) {
	// Fetch catalog info.
	catRes, err := s.client.Query(ctx, `query($name: String!) {
		core { catalog { schema_catalogs_by_pk(name: $name) {
			name type description read_only as_module
			types_in_catalog(filter: {hugr_type: {in: ["table","view"]}}) {
				name hugr_type description
			}
		}}}
	}`, map[string]any{"name": name})
	if err != nil {
		return nil, err
	}
	defer catRes.Close()
	if catRes.Err() != nil {
		return nil, catRes.Err()
	}

	var cat struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		Description string `json:"description"`
		ReadOnly    bool   `json:"read_only"`
		AsModule    bool   `json:"as_module"`
		Types       []struct {
			Name        string `json:"name"`
			HugrType    string `json:"hugr_type"`
			Description string `json:"description"`
		} `json:"types_in_catalog"`
	}
	if err := catRes.ScanData("core.catalog.schema_catalogs_by_pk", &cat); err != nil {
		return nil, fmt.Errorf("catalog %s not found: %w", name, err)
	}

	var tables, views []namedItem
	for _, t := range cat.Types {
		item := namedItem{Name: t.Name, Description: t.Description}
		switch t.HugrType {
		case hugrTypeTable:
			tables = append(tables, item)
		case hugrTypeView:
			views = append(views, item)
		}
	}

	// Fetch functions from module_intro.
	fnRes, err := s.client.Query(ctx, `query($name: String!, $types: [String!]) {
		core { catalog { module_intro(filter: {catalog: {eq: $name}, hugr_type: {in: $types}}) {
			field_name field_description
		}}}
	}`, map[string]any{"name": name, "types": strings.Split(hugrFunctionTypes, ",")})
	var functions []namedItem
	if err == nil {
		var fns []struct {
			FieldName   string `json:"field_name"`
			Description string `json:"field_description"`
		}
		_ = fnRes.ScanData("core.catalog.module_intro", &fns)
		fnRes.Close()
		seen := map[string]bool{}
		for _, f := range fns {
			if !seen[f.FieldName] {
				functions = append(functions, namedItem{Name: f.FieldName, Description: f.Description})
				seen[f.FieldName] = true
			}
		}
	}

	// Fetch submodules associated with this catalog.
	modRes, err := s.client.Query(ctx, `query($name: String!) {
		core { catalog { module_catalogs(filter: {catalog: {eq: $name}}) {
			module_info { name description }
		}}}
	}`, map[string]any{"name": name})
	var submodules []namedItem
	if err == nil {
		var mcs []struct {
			ModuleInfo struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"module_info"`
		}
		_ = modRes.ScanData("core.catalog.module_catalogs", &mcs)
		modRes.Close()
		seen := map[string]bool{}
		for _, mc := range mcs {
			if !seen[mc.ModuleInfo.Name] {
				submodules = append(submodules, namedItem{
					Name:        mc.ModuleInfo.Name,
					Description: mc.ModuleInfo.Description,
				})
				seen[mc.ModuleInfo.Name] = true
			}
		}
	}

	tablesJSON, _ := json.Marshal(tables)
	viewsJSON, _ := json.Marshal(views)
	functionsJSON, _ := json.Marshal(functions)
	submodulesJSON, _ := json.Marshal(submodules)

	return &dataSourceTemplateData{
		Name:           cat.Name,
		Description:    cat.Description,
		ReadOnly:       cat.ReadOnly,
		AsModule:       cat.AsModule,
		TablesJSON:     string(tablesJSON),
		ViewsJSON:      string(viewsJSON),
		FunctionsJSON:  string(functionsJSON),
		SubmodulesJSON: string(submodulesJSON),
	}, nil
}

func (s *summarizer) summarizeDataSources(ctx context.Context) (int, error) {
	filter := map[string]any{
		"is_summarized": map[string]any{"eq": false},
	}
	if s.catalog != "" {
		filter["name"] = map[string]any{"eq": s.catalog}
	}

	res, err := s.client.Query(ctx, `query($filter: core_schema_catalogs_filter) {
		core {
			catalog {
				schema_catalogs(filter: $filter) {
					name
				}
			}
		}
	}`, map[string]any{"filter": filter})
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if res.Err() != nil {
		return 0, res.Err()
	}

	var catalogs []struct {
		Name string `json:"name"`
	}
	if err := res.ScanData("core.catalog.schema_catalogs", &catalogs); err != nil {
		return 0, err
	}

	bar := newPhaseBar(len(catalogs), "  [3/4] Data sources")
	var processed atomic.Int32
	err = runParallel(catalogs, s.maxConns, func(cat struct {
		Name string `json:"name"`
	}) error {
		defer bar.Add(1)

		td, err := s.prepareDataSourceContext(ctx, cat.Name)
		if err != nil {
			bar.Describe(fmt.Sprintf("  [3/4] WARN: %s: %v", cat.Name, err))
			return nil
		}

		var dsSummary dataSourceSummary
		if err := s.callLLMAndParse(ctx, "data_source.tmpl", td, 4096, &dsSummary); err != nil {
			bar.Describe(fmt.Sprintf("  [3/4] WARN: %s: %v", cat.Name, err))
			return nil
		}

		if err := s.writeCatalogDesc(ctx, cat.Name, dsSummary.Short, dsSummary.Long); err != nil {
			bar.Describe(fmt.Sprintf("  [3/4] WARN: %s: write error", cat.Name))
			return nil
		}
		processed.Add(1)
		bar.Describe("  [3/4] Data sources")
		return nil
	})
	if err != nil {
		return int(processed.Load()), err
	}

	n := int(processed.Load())
	bar.Finish()
	fmt.Fprintf(os.Stderr, "  [3/4] Data sources: %d/%d done\n", n, len(catalogs))
	return n, nil
}

// --- Phase 4: Modules ---

// moduleForSummaryInfo holds metadata about a module for summarization.
type moduleForSummaryInfo struct {
	Name            string
	Description     string
	QueryRoot       string
	MutationRoot    string
	FunctionRoot    string
	MutFunctionRoot string
}

func (s *summarizer) prepareModuleContext(ctx context.Context, name string) (*moduleTemplateData, *moduleForSummaryInfo, error) {
	// Fetch module info.
	modRes, err := s.client.Query(ctx, `query($name: String!) {
		core { catalog { modules_by_pk(name: $name) {
			name description query_root mutation_root function_root mut_function_root
		}}}
	}`, map[string]any{"name": name})
	if err != nil {
		return nil, nil, err
	}
	defer modRes.Close()

	var mod struct {
		Name            string `json:"name"`
		Description     string `json:"description"`
		QueryRoot       string `json:"query_root"`
		MutationRoot    string `json:"mutation_root"`
		FunctionRoot    string `json:"function_root"`
		MutFunctionRoot string `json:"mut_function_root"`
	}
	if err := modRes.ScanData("core.catalog.modules_by_pk", &mod); err != nil {
		return nil, nil, fmt.Errorf("module %s not found: %w", name, err)
	}

	// Fetch tables/views with descriptions.
	typesRes, err := s.client.Query(ctx, `query($filter: core_types_filter) {
		core { catalog { types(filter: $filter) { name hugr_type description catalog } } }
	}`, map[string]any{
		"filter": map[string]any{
			"module":    map[string]any{"eq": name},
			"hugr_type": map[string]any{"in": []string{hugrTypeTable, hugrTypeView}},
		},
	})
	if err != nil {
		return nil, nil, err
	}
	defer typesRes.Close()

	var types []struct {
		Name        string `json:"name"`
		HugrType    string `json:"hugr_type"`
		Description string `json:"description"`
		Catalog     string `json:"catalog"`
	}
	_ = typesRes.ScanData("core.catalog.types", &types)

	tables := map[string]string{}
	views := map[string]string{}
	dsNames := map[string]bool{}
	for _, t := range types {
		switch t.HugrType {
		case hugrTypeTable:
			tables[t.Name] = t.Description
		case hugrTypeView:
			views[t.Name] = t.Description
		}
		if t.Catalog != "" {
			dsNames[t.Catalog] = true
		}
	}

	// Fetch functions and mutation functions from module_intro.
	introRes, err := s.client.Query(ctx, `query($module: String!) {
		core { catalog { module_intro(filter: {module: {eq: $module}}) {
			field_name field_description type_type hugr_type catalog
		}}}
	}`, map[string]any{"module": name})
	if err != nil {
		return nil, nil, err
	}
	defer introRes.Close()

	var intros []struct {
		FieldName   string `json:"field_name"`
		Description string `json:"field_description"`
		TypeType    string `json:"type_type"`
		HugrType    string `json:"hugr_type"`
		Catalog     string `json:"catalog"`
	}
	_ = introRes.ScanData("core.catalog.module_intro", &intros)

	functions := map[string]string{}
	mutFunctions := map[string]string{}
	seenFunc := map[string]bool{}
	for _, i := range intros {
		if i.Catalog != "" {
			dsNames[i.Catalog] = true
		}
		switch i.TypeType {
		case "function":
			if !seenFunc["f:"+i.FieldName] {
				functions[i.FieldName] = i.Description
				seenFunc["f:"+i.FieldName] = true
			}
		case "mut_function":
			if !seenFunc["mf:"+i.FieldName] {
				mutFunctions[i.FieldName] = i.Description
				seenFunc["mf:"+i.FieldName] = true
			}
		}
	}

	// Fetch submodules (direct children).
	allModsRes, err := s.client.Query(ctx, `query {
		core { catalog { modules { name description } } }
	}`, nil)
	submodules := map[string]string{}
	if err == nil {
		var allMods []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		}
		_ = allModsRes.ScanData("core.catalog.modules", &allMods)
		allModsRes.Close()

		prefix := name + "."
		for _, m := range allMods {
			if strings.HasPrefix(m.Name, prefix) {
				remainder := strings.TrimPrefix(m.Name, prefix)
				if !strings.Contains(remainder, ".") {
					submodules[m.Name] = m.Description
				}
			}
		}
	}

	// Fetch data source contexts.
	var dsContexts []dataSourceContext
	if len(dsNames) > 0 {
		var names []string
		for n := range dsNames {
			names = append(names, n)
		}
		catRes, err := s.client.Query(ctx, `query($names: [String!]) {
			core { catalog { schema_catalogs(filter: {name: {in: $names}}) { name type description } } }
		}`, map[string]any{"names": names})
		if err == nil {
			var cats []struct {
				Name        string `json:"name"`
				Type        string `json:"type"`
				Description string `json:"description"`
			}
			_ = catRes.ScanData("core.catalog.schema_catalogs", &cats)
			catRes.Close()
			for _, c := range cats {
				summary := c.Description
				if summary != "" {
					summary += " (" + c.Type + ")"
				}
				dsContexts = append(dsContexts, dataSourceContext{
					Name:        c.Name,
					SummaryText: summary,
				})
			}
		}
	}

	tablesJSON, _ := json.Marshal(tables)
	viewsJSON, _ := json.Marshal(views)
	functionsJSON, _ := json.Marshal(functions)
	mutFunctionsJSON, _ := json.Marshal(mutFunctions)
	submodulesJSON, _ := json.Marshal(submodules)
	dsCtxJSON, _ := json.Marshal(dsContexts)

	td := &moduleTemplateData{
		Name:                   mod.Name,
		Description:            mod.Description,
		TablesJSON:             string(tablesJSON),
		ViewsJSON:              string(viewsJSON),
		FunctionsJSON:          string(functionsJSON),
		MutationFunctionsJSON:  string(mutFunctionsJSON),
		SubmodulesJSON:         string(submodulesJSON),
		DataSourceContextsJSON: string(dsCtxJSON),
	}

	info := &moduleForSummaryInfo{
		Name:            mod.Name,
		Description:     mod.Description,
		QueryRoot:       mod.QueryRoot,
		MutationRoot:    mod.MutationRoot,
		FunctionRoot:    mod.FunctionRoot,
		MutFunctionRoot: mod.MutFunctionRoot,
	}

	return td, info, nil
}

func (s *summarizer) writeBackModule(ctx context.Context, ms *moduleSummary, info *moduleForSummaryInfo) error {
	// 1. Module description.
	if ms.Short != "" || ms.Long != "" {
		_ = s.writeModuleDesc(ctx, info.Name, ms.Short, ms.Long)
	}

	// 2. Root type descriptions.
	if info.QueryRoot != "" && ms.QueryType != "" {
		_ = s.writeTypeDesc(ctx, info.QueryRoot, ms.QueryType, "")
	}
	if info.MutationRoot != "" && ms.MutationType != "" {
		_ = s.writeTypeDesc(ctx, info.MutationRoot, ms.MutationType, "")
	}
	if info.FunctionRoot != "" && ms.FunctionType != "" {
		_ = s.writeTypeDesc(ctx, info.FunctionRoot, ms.FunctionType, "")
	}
	if info.MutFunctionRoot != "" && ms.MutFunctionType != "" {
		_ = s.writeTypeDesc(ctx, info.MutFunctionRoot, ms.MutFunctionType, "")
	}

	// 3. Update parent module fields (for submodules).
	parts := strings.Split(info.Name, ".")
	if len(parts) > 1 {
		parentName := strings.Join(parts[:len(parts)-1], ".")
		fieldName := parts[len(parts)-1]

		parentRes, err := s.client.Query(ctx, `query($name: String!) {
			core { catalog { modules_by_pk(name: $name) {
				query_root mutation_root function_root mut_function_root
			}}}
		}`, map[string]any{"name": parentName})
		if err == nil {
			var parent struct {
				QueryRoot       string `json:"query_root"`
				MutationRoot    string `json:"mutation_root"`
				FunctionRoot    string `json:"function_root"`
				MutFunctionRoot string `json:"mut_function_root"`
			}
			if err := parentRes.ScanData("core.catalog.modules_by_pk", &parent); err == nil {
				if parent.QueryRoot != "" && ms.QueryType != "" {
					_ = s.writeFieldDesc(ctx, parent.QueryRoot, fieldName, ms.QueryType, "")
				}
				if parent.MutationRoot != "" && ms.MutationType != "" {
					_ = s.writeFieldDesc(ctx, parent.MutationRoot, fieldName, ms.MutationType, "")
				}
				if parent.FunctionRoot != "" && ms.FunctionType != "" {
					_ = s.writeFieldDesc(ctx, parent.FunctionRoot, fieldName, ms.FunctionType, "")
				}
				if parent.MutFunctionRoot != "" && ms.MutFunctionType != "" {
					_ = s.writeFieldDesc(ctx, parent.MutFunctionRoot, fieldName, ms.MutFunctionType, "")
				}
			}
			parentRes.Close()
		}
	}

	return nil
}

func (s *summarizer) summarizeModules(ctx context.Context) (int, error) {
	filter := map[string]any{
		"is_summarized": map[string]any{"eq": false},
	}

	res, err := s.client.Query(ctx, `query($filter: core_modules_filter) {
		core {
			catalog {
				modules(filter: $filter, order_by: [{field: "name", direction: DESC}]) {
					name
				}
			}
		}
	}`, map[string]any{"filter": filter})
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if res.Err() != nil {
		return 0, res.Err()
	}

	var mods []struct {
		Name string `json:"name"`
	}
	if err := res.ScanData("core.catalog.modules", &mods); err != nil {
		return 0, err
	}

	// Sort descending by name so sub-modules are processed before parents.
	sort.Slice(mods, func(i, j int) bool {
		return mods[i].Name > mods[j].Name
	})

	bar := newPhaseBar(len(mods), "  [4/4] Modules")
	var processed int
	for _, mod := range mods {
		// Sequential for dependency ordering (sub-modules first).
		name := mod.Name

		td, info, err := s.prepareModuleContext(ctx, name)
		if err != nil {
			bar.Describe(fmt.Sprintf("  [4/4] WARN: %s: %v", name, err))
			bar.Add(1)
			continue
		}

		var ms moduleSummary
		if err := s.callLLMAndParse(ctx, "module.tmpl", td, 4096, &ms); err != nil {
			bar.Describe(fmt.Sprintf("  [4/4] WARN: %s: %v", name, err))
			bar.Add(1)
			continue
		}

		if err := s.writeBackModule(ctx, &ms, info); err != nil {
			bar.Describe(fmt.Sprintf("  [4/4] WARN: %s: write error", name))
			bar.Add(1)
			continue
		}
		processed++
		bar.Describe("  [4/4] Modules")
		bar.Add(1)
	}

	bar.Finish()
	fmt.Fprintf(os.Stderr, "  [4/4] Modules: %d/%d done\n", processed, len(mods))
	return processed, nil
}

// --- Single-entity summarization ---

func (s *summarizer) summarizeSingleType(ctx context.Context, name string) error {
	fmt.Fprintf(os.Stderr, "Summarizing type: %s\n", name)

	td, meta, err := s.prepareDataObjectContext(ctx, name)
	if err != nil {
		return fmt.Errorf("prepare context: %w", err)
	}

	var ds dataObjectSummary
	if err := s.callLLMAndParse(ctx, "data_object.tmpl", td, 16384, &ds); err != nil {
		return fmt.Errorf("LLM: %w", err)
	}

	fmt.Fprintf(os.Stderr, "  description: %s\n", ds.Short)
	return s.writeBackDataObject(ctx, &ds, meta)
}

func (s *summarizer) summarizeSingleFunction(ctx context.Context, typeName, fieldName string) error {
	fmt.Fprintf(os.Stderr, "Summarizing function: %s.%s\n", typeName, fieldName)

	td, meta, err := s.prepareFunctionContext(ctx, typeName, fieldName)
	if err != nil {
		return fmt.Errorf("prepare context: %w", err)
	}

	var fs functionSummary
	if err := s.callLLMAndParse(ctx, "function.tmpl", td, 4096, &fs); err != nil {
		return fmt.Errorf("LLM: %w", err)
	}

	fmt.Fprintf(os.Stderr, "  description: %s\n", fs.Short)
	return s.writeBackFunction(ctx, &fs, meta)
}

func (s *summarizer) summarizeSingleModule(ctx context.Context, name string) error {
	fmt.Fprintf(os.Stderr, "Summarizing module: %s\n", name)

	td, info, err := s.prepareModuleContext(ctx, name)
	if err != nil {
		return fmt.Errorf("prepare context: %w", err)
	}

	var ms moduleSummary
	if err := s.callLLMAndParse(ctx, "module.tmpl", td, 4096, &ms); err != nil {
		return fmt.Errorf("LLM: %w", err)
	}

	fmt.Fprintf(os.Stderr, "  description: %s\n", ms.Short)
	return s.writeBackModule(ctx, &ms, info)
}

func (s *summarizer) summarizeSingleSource(ctx context.Context, name string) error {
	fmt.Fprintf(os.Stderr, "Summarizing source: %s\n", name)

	td, err := s.prepareDataSourceContext(ctx, name)
	if err != nil {
		return fmt.Errorf("prepare context: %w", err)
	}

	var dsSummary dataSourceSummary
	if err := s.callLLMAndParse(ctx, "data_source.tmpl", td, 4096, &dsSummary); err != nil {
		return fmt.Errorf("LLM: %w", err)
	}

	fmt.Fprintf(os.Stderr, "  description: %s\n", dsSummary.Short)
	return s.writeCatalogDesc(ctx, name, dsSummary.Short, dsSummary.Long)
}
