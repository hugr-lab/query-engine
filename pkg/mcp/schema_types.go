package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/mark3labs/mcp-go/mcp"
)

// schema-describe_types is the seam between the two families. An agent
// constantly ends up holding a bare type name — from an error message, from a
// field's type, from an argument like shop_items_filter — and has nowhere to
// take it: catalog-* is keyed on logical entities, and the name may be a
// filter input, an aggregation, a mutation input or a module root instead.
//
// So it answers "what IS this", cheaply: kind, description, member COUNTS —
// never the members. And when the name turns out to belong to the logical
// model, it says which catalog-* tool owns it, which is the whole point of
// being a seam rather than a lookup.
//
// It runs in the caller's context through __type, so a type the caller may not
// see is simply not found — the same answer a misspelling gets.

// TypeDescription is what one name turned out to be.
type TypeDescription struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"        jsonschema_description:"OBJECT | INPUT_OBJECT | ENUM | SCALAR | INTERFACE | UNION | LIST | NON_NULL"`
	Description string `json:"description,omitempty"`

	// Member counts — call schema-type_fields / schema-enum_values for the
	// members themselves.
	FieldsCount      int `json:"fields_count,omitempty"`
	InputFieldsCount int `json:"input_fields_count,omitempty"`
	EnumValuesCount  int `json:"enum_values_count,omitempty"`

	// Role in the logical model, when it has one.
	LogicalKind string `json:"logical_kind,omitempty" jsonschema_description:"Set when this type IS a logical entity: data_object, or module_root"`
	Module      string `json:"module,omitempty"`
	DerivedFrom string `json:"derived_from,omitempty" jsonschema_description:"The data object this type was generated FROM — a filter, aggregation or mutation input names its base object here"`
	Role        string `json:"role,omitempty"         jsonschema_description:"What the generated type is FOR: filter, list_filter, unique_filter, insert_input, update_input, aggregation, bucket_aggregation, sub_aggregation"`

	NextCall string `json:"next_call,omitempty" jsonschema_description:"The tool that owns this name"`
}

type TypeDescriptions struct {
	Items    []TypeDescription `json:"items"`
	NotFound []string          `json:"not_found,omitempty" jsonschema_description:"Names that do not exist OR that the caller may not see"`
}

type metaTypeNode struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Description string `json:"description"`
	Fields      []struct {
		Name string `json:"name"`
	} `json:"fields"`
	InputFields []struct {
		Name string `json:"name"`
	} `json:"inputFields"`
	EnumValues []struct {
		Name string `json:"name"`
	} `json:"enumValues"`
}

type metaObjectRef struct {
	Name       string `json:"name"`
	ModuleName string `json:"moduleName"`
}

// derivedRoles maps a generated type's shape back to what it is FOR. The
// forward mapping is base.DerivedTypeNames; this is its inverse, and it is
// exact rather than suffix-guessing: a candidate base is accepted only when
// regenerating its derived names reproduces the name in hand.
var derivedRoles = map[string]string{
	"_filter":         "filter",
	"_list_filter":    "list_filter",
	"_unique_filter":  "unique_filter",
	"_mut_input_data": "insert_input",
	"_mut_data":       "update_input",
}

// derivedBase returns the data object a generated type came from, and the role
// it plays, or ("", ""). The candidate is verified by round-tripping through
// base.DerivedTypeNames, so a source type genuinely named "x_filter" is not
// mistaken for a filter over "x".
func derivedBase(name string) (string, string) {
	for suffix, role := range derivedRoles {
		if candidate, ok := strings.CutSuffix(name, suffix); ok && candidate != "" {
			if slices.Contains(base.DerivedTypeNames(candidate), name) {
				return candidate, role
			}
		}
	}
	// The aggregation family is prefixed as well as suffixed.
	if trimmed, ok := strings.CutPrefix(name, "_"); ok {
		for _, suffix := range []string{
			"_aggregation_sub_aggregation_sub_aggregation",
			"_aggregation_sub_aggregation",
			"_aggregation_bucket",
			"_aggregation",
		} {
			if candidate, ok := strings.CutSuffix(trimmed, suffix); ok && candidate != "" {
				if slices.Contains(base.DerivedTypeNames(candidate), name) {
					return candidate, strings.TrimPrefix(suffix, "_")
				}
			}
		}
	}
	return "", ""
}

// moduleRootKinds are the five generated root types of a module. Their names
// are not reversible — a dot in a module name becomes an underscore — so the
// answer names the KIND of thing it is and sends the agent to the module list
// rather than guessing which module it belongs to.
var moduleRootKinds = []string{"_query", "_mutation", "_function", "_mut_function", "_subscription"}

func isModuleRoot(name string) bool {
	if !strings.HasPrefix(name, "_module_") {
		return false
	}
	return slices.ContainsFunc(moduleRootKinds, func(suffix string) bool {
		return strings.HasSuffix(name, suffix)
	})
}

func (s *Server) schemaDescribeTypes(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	names := req.GetStringSlice("names", nil)
	if len(names) == 0 {
		return toolResultError("names is required — pass the type names to identify"), nil
	}
	if len(names) > maxPageLimit {
		names = names[:maxPageLimit]
	}

	var sig, body strings.Builder
	vars := map[string]any{}
	arg := func(v string) string {
		name := fmt.Sprintf("$n%d", len(vars))
		vars[name[1:]] = v
		if sig.Len() > 0 {
			sig.WriteString(", ")
		}
		fmt.Fprintf(&sig, "%s: String!", name)
		return name
	}

	bases := make([]string, len(names))
	roles := make([]string, len(names))
	for i, name := range names {
		fmt.Fprintf(&body, "t%d: __type(name: %s) { name kind description fields { name } inputFields { name } enumValues { name } }\n",
			i, arg(name))
		// Is the name itself a data object?
		fmt.Fprintf(&body, "o%d: _dataObject(name: %s) { name moduleName }\n", i, arg(name))
		// Or was it generated FROM one?
		if b, role := derivedBase(name); b != "" {
			bases[i], roles[i] = b, role
			fmt.Fprintf(&body, "b%d: _dataObject(name: %s) { name moduleName }\n", i, arg(b))
		}
	}

	batch := map[string]json.RawMessage{}
	gql := "query(" + sig.String() + ") {\n" + body.String() + "}"
	if err := s.queryScan(ctx, gql, vars, "", &batch); err != nil {
		return toolResultError(err.Error()), nil
	}

	out := &TypeDescriptions{Items: []TypeDescription{}}
	for i, name := range names {
		var node metaTypeNode
		if !decodeAlias(batch, fmt.Sprintf("t%d", i), &node) {
			out.NotFound = append(out.NotFound, name)
			continue
		}
		d := TypeDescription{
			Name: node.Name, Kind: node.Kind, Description: node.Description,
			FieldsCount:      len(node.Fields),
			InputFieldsCount: len(node.InputFields),
			EnumValuesCount:  len(node.EnumValues),
		}

		var self metaObjectRef
		switch {
		case decodeAlias(batch, fmt.Sprintf("o%d", i), &self):
			d.LogicalKind = kindDataObject
			d.Module = self.ModuleName
			d.NextCall = fmt.Sprintf("catalog-describe(kind: data_object, names: [%q])", name)
		case isModuleRoot(name):
			d.LogicalKind = "module_root"
			d.NextCall = "catalog-list(kind: module)"
		default:
			var from metaObjectRef
			if bases[i] != "" && decodeAlias(batch, fmt.Sprintf("b%d", i), &from) {
				d.DerivedFrom = from.Name
				d.Role = roles[i]
				d.Module = from.ModuleName
				d.NextCall = fmt.Sprintf("catalog-describe(kind: data_object, names: [%q])", from.Name)
			} else if d.Kind == "ENUM" {
				d.NextCall = fmt.Sprintf("schema-enum_values(type_name: %q)", name)
			} else if d.FieldsCount+d.InputFieldsCount > 0 {
				d.NextCall = fmt.Sprintf("schema-type_fields(type_name: %q)", name)
			}
		}
		out.Items = append(out.Items, d)
	}
	return toolResultJSON(out), nil
}

// decodeAlias reads one alias of a batched answer; a missing or null alias
// means the engine did not serve it to this caller.
func decodeAlias(batch map[string]json.RawMessage, alias string, target any) bool {
	raw, ok := batch[alias]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return false
	}
	return json.Unmarshal(raw, target) == nil
}
