package mcp

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

// The catalog-* family answers questions about the LOGICAL model — modules,
// data sources, data objects and functions — over one `kind` vocabulary and
// three verbs: list (what exists), search (what is relevant), describe (how do
// I call these exact ones).
//
// Every catalog-* tool that does not RANK runs its GraphQL in the CALLER's
// context against the _catalog meta-query family, so permission filtering is
// the engine's (pkg/metadata) and nothing is re-filtered here. Only the search
// path reads privileged and filters in Go, because the ranking index lives in
// views the caller may have no rights on.

const (
	kindModule     = "module"
	kindDataSource = "data_source"
	kindDataObject = "data_object"
	kindFunction   = "function"
)

var catalogKinds = []string{kindModule, kindDataSource, kindDataObject, kindFunction}

const (
	defaultPageLimit = 50
	maxPageLimit     = 200

	// moduleWalkDepth is how many module levels below the walk root carry a
	// payload selection. The engine's metadata depth budget (Config.MaxDepth,
	// default 7) is consumed one unit per nested module and one more by the
	// payload, so this stays well inside it. A tree deeper than this is
	// reported as truncated, and re-rooting the walk with `module` reaches the
	// rest — which is also the cheaper call.
	moduleWalkDepth = 4
)

// Page is the uniform pagination envelope of every list-returning tool.
// Total is counted AFTER permission filtering, so "50 of 312" is honest.
type Page[T any] struct {
	Items   []T  `json:"items"`
	Total   int  `json:"total"    jsonschema_description:"Total matching items after permission filtering"`
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
	HasMore bool `json:"has_more" jsonschema_description:"More items exist past this page"`
	// Truncated marks a module tree deeper than the walk reaches: re-run with
	// `module` set to a deeper node to enumerate the rest.
	Truncated bool `json:"truncated,omitempty"`
}

// CatalogItem is one entry of catalog-list. The shape is kind-dependent; the
// fields that do not apply to a kind are omitted rather than zeroed.
type CatalogItem struct {
	Kind string `json:"kind" jsonschema_description:"module | data_source | data_object | function"`
	Name string `json:"name" jsonschema_description:"Module: dotted path (\"\" is the root). Data object: GraphQL type name. Function: field name in its module"`
	// Type is TABLE|VIEW for a data object, FUNCTION|MUTATION|SUBSCRIPTION for
	// a callable, and the engine type string for a data source.
	Type        string `json:"type,omitempty"`
	Module      string `json:"module,omitempty"      jsonschema_description:"Owning module — REQUIRED to nest the GraphQL query"`
	DataSource  string `json:"data_source,omitempty"`
	Description string `json:"description,omitempty"`

	// Module only.
	DataObjects *int `json:"data_objects,omitempty" jsonschema_description:"Number of data objects directly in this module"`
	Functions   *int `json:"functions,omitempty"    jsonschema_description:"Number of callables directly in this module"`
	Submodules  *int `json:"submodules,omitempty"`

	// Data source only.
	ReadOnly    *bool    `json:"read_only,omitempty"    jsonschema_description:"null means the catalog storage does not record it — NOT that mutations are available"`
	AsModule    *bool    `json:"as_module,omitempty"`
	IsExtension *bool    `json:"is_extension,omitempty"`
	Modules     []string `json:"modules,omitempty"      jsonschema_description:"Modules this source places members in"`
}

// --- meta-query scan targets ---

type metaModuleNode struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	DataObjects []metaObjectNode   `json:"dataObjects"`
	Functions   []metaFunctionNode `json:"functions"`
	Modules     []metaModuleNode   `json:"modules"`
}

type metaObjectNode struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Module      string `json:"moduleName"`
	DataSource  string `json:"dataSourceName"`
	Description string `json:"description"`
}

type metaFunctionNode struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Module      string `json:"moduleName"`
	DataSource  string `json:"dataSourceName"`
	Description string `json:"description"`
}

type metaDataSourceNode struct {
	Name        string   `json:"name"`
	Engine      string   `json:"engine"`
	Description string   `json:"description"`
	ReadOnly    *bool    `json:"readOnly"`
	AsModule    *bool    `json:"asModule"`
	IsExtension *bool    `json:"isExtension"`
	Modules     []string `json:"modules"`
}

// moduleWalkQuery nests `modules { … }` moduleWalkDepth levels below the walk
// root, selecting payload at every level. The innermost level selects module
// NAMES only: a name showing up there is how the walk learns it was cut short
// without paying for another payload level.
func moduleWalkQuery(payload string) string {
	level := "name description " + payload
	nested := "modules { name }"
	for range moduleWalkDepth {
		nested = "modules { " + level + " " + nested + " }"
	}
	return "query($module: String!) { _module(name: $module) { " + level + " " + nested + " } }"
}

const (
	modulePayload     = "dataObjects { name } functions { name }"
	dataObjectPayload = "dataObjects { name type moduleName dataSourceName description }"
	functionPayload   = "functions { name type moduleName dataSourceName description }"
)

// walkModules flattens the module tree into pre-order, and reports whether the
// walk was cut short by moduleWalkDepth. A node beyond the payload levels
// arrives with a name and nothing else, which is exactly the truncation
// signal — such a node is NOT emitted, because its payload was never read.
func walkModules(root *metaModuleNode, depth int, visit func(*metaModuleNode)) bool {
	if depth > moduleWalkDepth {
		return true
	}
	visit(root)
	truncated := false
	for i := range root.Modules {
		if walkModules(&root.Modules[i], depth+1, visit) {
			truncated = true
		}
	}
	return truncated
}

func (s *Server) catalogList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	kind := req.GetString("kind", "")
	if !slices.Contains(catalogKinds, kind) {
		return toolResultError(fmt.Sprintf("kind must be one of %s", strings.Join(catalogKinds, ", "))), nil
	}
	module := req.GetString("module", "")
	prefix := strings.ToLower(req.GetString("prefix", ""))
	limit := req.GetInt("limit", defaultPageLimit)
	if limit <= 0 || limit > maxPageLimit {
		limit = min(maxPageLimit, max(1, limit))
	}
	offset := max(0, req.GetInt("offset", 0))

	items, truncated, err := s.catalogItems(ctx, kind, module)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	if prefix != "" {
		items = slices.DeleteFunc(items, func(it CatalogItem) bool {
			return !strings.HasPrefix(strings.ToLower(it.Name), prefix)
		})
	}
	return toolResultJSON(paginate(items, limit, offset, truncated)), nil
}

// catalogItems reads one kind's full permission-filtered enumeration. The read
// runs in the CALLER's context: the meta-query family hides what the role may
// not see, so there is nothing left to filter here.
func (s *Server) catalogItems(ctx context.Context, kind, module string) ([]CatalogItem, bool, error) {
	if kind == kindDataSource {
		var sources []metaDataSourceNode
		err := s.queryScan(ctx, `{ _dataSources {
			name engine description readOnly asModule isExtension modules
		} }`, nil, "_dataSources", &sources)
		if err != nil {
			return nil, false, err
		}
		items := make([]CatalogItem, 0, len(sources))
		for _, ds := range sources {
			items = append(items, CatalogItem{
				Kind: kindDataSource, Name: ds.Name, Type: ds.Engine,
				Description: ds.Description, ReadOnly: ds.ReadOnly, AsModule: ds.AsModule,
				IsExtension: ds.IsExtension, Modules: ds.Modules,
			})
		}
		slices.SortFunc(items, func(a, b CatalogItem) int { return strings.Compare(a.Name, b.Name) })
		return items, false, nil
	}

	payload := modulePayload
	switch kind {
	case kindDataObject:
		payload = dataObjectPayload
	case kindFunction:
		payload = functionPayload
	}
	var root metaModuleNode
	err := s.queryScan(ctx, moduleWalkQuery(payload), map[string]any{"module": module}, "_module", &root)
	if err != nil {
		return nil, false, err
	}

	var items []CatalogItem
	truncated := walkModules(&root, 0, func(m *metaModuleNode) {
		switch kind {
		case kindModule:
			objects, functions, submodules := len(m.DataObjects), len(m.Functions), len(m.Modules)
			items = append(items, CatalogItem{
				Kind: kindModule, Name: m.Name, Description: m.Description,
				DataObjects: &objects, Functions: &functions, Submodules: &submodules,
			})
		case kindDataObject:
			for _, o := range m.DataObjects {
				items = append(items, CatalogItem{
					Kind: kindDataObject, Name: o.Name, Type: o.Type, Module: o.Module,
					DataSource: o.DataSource, Description: o.Description,
				})
			}
		case kindFunction:
			for _, f := range m.Functions {
				items = append(items, CatalogItem{
					Kind: kindFunction, Name: f.Name, Type: f.Type, Module: f.Module,
					DataSource: f.DataSource, Description: f.Description,
				})
			}
		}
	})

	slices.SortFunc(items, func(a, b CatalogItem) int {
		if c := strings.Compare(a.Module, b.Module); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	return items, truncated, nil
}

// paginate applies the uniform envelope. Items is never nil, so a client sees
// an empty list rather than a null.
func paginate[T any](items []T, limit, offset int, truncated bool) Page[T] {
	total := len(items)
	if offset > total {
		offset = total
	}
	end := min(offset+limit, total)
	page := items[offset:end]
	if page == nil {
		page = []T{}
	}
	return Page[T]{
		Items: page, Total: total, Limit: limit, Offset: offset,
		HasMore: end < total, Truncated: truncated,
	}
}
